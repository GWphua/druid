/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.curator.announcement;

import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorMultiTransaction;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheBuilder;
import org.apache.curator.framework.recipes.cache.CuratorCacheStorage;
import org.apache.curator.utils.ZKPaths;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;

/**
 * Announces things on Zookeeper.
 */
public class Announcer
{
  private static final Logger log = new Logger(Announcer.class);

  private final CuratorFramework curator;
  private final ExecutorService pathChildrenCacheExecutor;

  private final List<Announceable> toAnnounce = new ArrayList<>();
  private final List<Announceable> toUpdate = new ArrayList<>();
  private final ConcurrentMap<String, CuratorCache> listeners = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, ConcurrentMap<String, byte[]>> announcements = new ConcurrentHashMap<>();
  private final List<String> parentsIBuilt = new CopyOnWriteArrayList<>();

  // Used for testing
  private Set<String> addedChildren;

  private boolean started = false;

  public Announcer(
      CuratorFramework curator,
      ExecutorService exec
  )
  {
    this.curator = curator;
    this.pathChildrenCacheExecutor = exec;
  }

  @VisibleForTesting
  void initializeAddedChildren()
  {
    addedChildren = new HashSet<>();
  }

  @VisibleForTesting
  Set<String> getAddedChildren()
  {
    return addedChildren;
  }

  @LifecycleStart
  public void start()
  {
    log.debug("Starting Announcer.");
    synchronized (toAnnounce) {
      if (started) {
        return;
      }

      started = true;

      for (Announceable announceable : toAnnounce) {
        announce(announceable.path, announceable.bytes, announceable.removeParentsIfCreated);
      }
      toAnnounce.clear();

      for (Announceable announceable : toUpdate) {
        update(announceable.path, announceable.bytes);
      }
      toUpdate.clear();
    }
  }

  @LifecycleStop
  public void stop()
  {
    log.debug("Stopping Announcer.");
    synchronized (toAnnounce) {
      if (!started) {
        return;
      }

      started = false;

      try {
        CloseableUtils.closeAll(listeners.values());
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      finally {
        pathChildrenCacheExecutor.shutdown();
      }

      for (Map.Entry<String, ConcurrentMap<String, byte[]>> entry : announcements.entrySet()) {
        String basePath = entry.getKey();

        for (String announcementPath : entry.getValue().keySet()) {
          unannounce(ZKPaths.makePath(basePath, announcementPath));
        }
      }

      if (!parentsIBuilt.isEmpty()) {
        CuratorMultiTransaction transaction = curator.transaction();

        ArrayList<CuratorOp> operations = new ArrayList<>();
        for (String parent : parentsIBuilt) {
          try {
            CuratorOp deleteParentOperation = curator.transactionOp().delete().forPath(parent);
            operations.add(deleteParentOperation);
          }
          catch (Exception e) {
            log.info(e, "Unable to delete parent[%s].", parent);
          }
        }
        try {
          transaction.forOperations(operations);
        }
        catch (Exception e) {
          log.info(e, "Unable to commit transaction.");
        }
      }
    }
  }

  /**
   * Like announce(path, bytes, true).
   */
  public void announce(String path, byte[] bytes)
  {
    announce(path, bytes, true);
  }

  /**
   * Announces the provided bytes at the given path.  Announcement means that it will create an ephemeral node
   * and monitor it to make sure that it always exists until it is unannounced or this object is closed.
   *
   * @param path                  The path to announce at
   * @param bytes                 The payload to announce
   * @param removeParentIfCreated remove parent of "path" if we had created that parent
   */
  public void announce(String path, byte[] bytes, boolean removeParentIfCreated)
  {
    synchronized (toAnnounce) {
      if (!started) {
        toAnnounce.add(new Announceable(path, bytes, removeParentIfCreated));
        return;
      }
    }

    final ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(path);

    final String parentPath = pathAndNode.getPath();
    boolean buildParentPath = false;

    ConcurrentMap<String, byte[]> subPaths = announcements.get(parentPath);

    if (subPaths == null) {
      try {
        if (curator.checkExists().forPath(parentPath) == null) {
          buildParentPath = true;
        }
      }
      catch (Exception e) {
        log.debug(e, "Problem checking if the parent existed, ignoring.");
      }

      // I don't have a watcher on this path yet, create a Map and start watching.
      announcements.putIfAbsent(parentPath, new ConcurrentHashMap<>());

      // Guaranteed to be non-null, but might be a map put in there by another thread.
      final ConcurrentMap<String, byte[]> finalSubPaths = announcements.get(parentPath);

      // Synchronize to make sure that I only create a listener once.
      synchronized (finalSubPaths) {
        if (!listeners.containsKey(parentPath)) {
          final CuratorCacheBuilder curatorCacheBuilder = CuratorCache.builder(curator, parentPath);
          final CuratorCache cache = curatorCacheBuilder
              .withOptions(
                  CuratorCache.Options.DO_NOT_CLEAR_ON_CLOSE,
                  CuratorCache.Options.COMPRESSED_DATA
              )
              .withStorage(
                  CuratorCacheStorage.dataNotCached()
              )
              .build();

          cache.listenable().addListener(
              (type, oldData, newData) -> {
                // NOTE: ZooKeeper does not guarantee that we will get every event, and thus PathChildrenCache doesn't
                //  as well. If one of the below events are missed, Announcer might not work properly.
                log.debug("Path[%s] got event type[%s]", parentPath, type);
                switch (type) {
                  case NODE_DELETED:
                    final ZKPaths.PathAndNode childPath = ZKPaths.getPathAndNode(newData.getPath());
                    final byte[] value = finalSubPaths.get(childPath.getNode());
                    if (value != null) {
                      log.info("Node[%s] dropped, reinstating.", newData.getPath());
                      try {
                        createAnnouncement(newData.getPath(), value);
                      }
                      catch (Exception e) {
                        throw new RuntimeException(e);
                      }
                    }
                    break;
                  case NODE_CREATED:
                    if (addedChildren != null) {
                      addedChildren.add(newData.getPath());
                    }
                    // fall through
                  case NODE_CHANGED:
                    // do nothing
                }
              }
          );

          synchronized (toAnnounce) {
            if (started) {
              if (buildParentPath) {
                createPath(parentPath, removeParentIfCreated);
              }
              startCache(cache);
              listeners.put(parentPath, cache);
            }
          }
        }
      }

      subPaths = finalSubPaths;
    }

    boolean created = false;
    synchronized (toAnnounce) {
      if (started) {
        byte[] oldBytes = subPaths.putIfAbsent(pathAndNode.getNode(), bytes);

        if (oldBytes == null) {
          created = true;
        } else if (!Arrays.equals(oldBytes, bytes)) {
          throw new IAE("Cannot reannounce different values under the same path");
        }
      }
    }

    if (created) {
      try {
        createAnnouncement(path, bytes);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void update(final String path, final byte[] bytes)
  {
    synchronized (toAnnounce) {
      if (!started) {
        // removeParentsIfCreated is not relevant for updates; use dummy value "false".
        toUpdate.add(new Announceable(path, bytes, false));
        return;
      }
    }

    final ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(path);

    final String parentPath = pathAndNode.getPath();
    final String nodePath = pathAndNode.getNode();

    ConcurrentMap<String, byte[]> subPaths = announcements.get(parentPath);

    if (subPaths == null || subPaths.get(nodePath) == null) {
      throw new ISE("Cannot update a path[%s] that hasn't been announced!", path);
    }

    synchronized (toAnnounce) {
      try {
        byte[] oldBytes = subPaths.get(nodePath);

        if (!Arrays.equals(oldBytes, bytes)) {
          subPaths.put(nodePath, bytes);
          updateAnnouncement(path, bytes);
        }
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private String createAnnouncement(final String path, byte[] value) throws Exception
  {
    return curator.create().compressed().withMode(CreateMode.EPHEMERAL).inBackground().forPath(path, value);
  }

  private Stat updateAnnouncement(final String path, final byte[] value) throws Exception
  {
    return curator.setData().compressed().inBackground().forPath(path, value);
  }

  /**
   * Unannounces an announcement created at path.  Note that if all announcements get removed, the Announcer
   * will continue to have ZK watches on paths because clearing them out is a source of ugly race conditions.
   * <p/>
   * If you need to completely clear all the state of what is being watched and announced, stop() the Announcer.
   *
   * @param path the path to unannounce
   */
  public void unannounce(String path)
  {
    final ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(path);
    final String parentPath = pathAndNode.getPath();

    final ConcurrentMap<String, byte[]> subPaths = announcements.get(parentPath);

    if (subPaths == null || subPaths.remove(pathAndNode.getNode()) == null) {
      log.debug("Path[%s] not announced, cannot unannounce.", path);
      return;
    }
    log.info("Unannouncing [%s]", path);

    try {
      CuratorOp deletePathOperation = curator.transactionOp().delete().forPath(path);
      curator.transaction().forOperations(deletePathOperation);
    }
    catch (KeeperException.NoNodeException e) {
      log.info("ZooKeeper tries to delete Node[%s] but it dosen't exist...", path);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void startCache(CuratorCache cache)
  {
    try {
      cache.start();
    }
    catch (Throwable e) {
      throw CloseableUtils.closeAndWrapInCatch(e, cache);
    }
  }

  private void createPath(String parentPath, boolean removeParentsIfCreated)
  {
    try {
      curator.create().creatingParentsIfNeeded().forPath(parentPath);
      if (removeParentsIfCreated) {
        parentsIBuilt.add(parentPath);
      }
      log.debug("Created parentPath[%s], %s remove on stop.", parentPath, removeParentsIfCreated ? "will" : "will not");
    }
    catch (Exception e) {
      log.info(e, "Problem creating parentPath[%s], someone else created it first?", parentPath);
    }
  }

  private static class Announceable
  {
    final String path;
    final byte[] bytes;
    final boolean removeParentsIfCreated;

    public Announceable(String path, byte[] bytes, boolean removeParentsIfCreated)
    {
      this.path = path;
      this.bytes = bytes;
      this.removeParentsIfCreated = removeParentsIfCreated;
    }
  }
}

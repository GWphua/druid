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
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorMultiTransaction;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

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
import java.util.concurrent.atomic.AtomicReference;

/**
 * The {@link Announcer} class manages the announcement of a node, and watches all child
 * and sibling nodes under the specified path in a ZooKeeper ensemble. It monitors these nodes
 * to ensure their existence and manage their lifecycle collectively.
 *
 * <p>
 * This class uses Apache Curator's PathChildrenCache recipe under the hood to track all znodes
 * under the specified node's parent. See {@link NodeAnnouncer} for an announcer that
 * uses the NodeCache recipe instead.
 * </p>
 */
public class Announcer
{
  private static final Logger log = new Logger(Announcer.class);

  private final CuratorFramework curator;
  private final ExecutorService pathChildrenCacheExecutor;

  @GuardedBy("toAnnounce")
  private final List<Announceable> toAnnounce = new ArrayList<>();
  @GuardedBy("toAnnounce")
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
        log.debug("Cannot start Announcer that has already started.");
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
        log.debug("Cannot stop Announcer that has not started.");
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
            operations.add(curator.transactionOp().delete().forPath(parent));
          }
          catch (Exception e) {
            log.info(e, "Unable to delete parent[%s] when closing Announcer.", parent);
          }
        }

        try {
          transaction.forOperations(operations);
        }
        catch (Exception e) {
          log.info(e, "Unable to commit transaction when closing Announcer.");
        }
      }
    }
  }

  /**
   * Overload of {@link #announce(String, byte[], boolean)}, but removes parent node of path after announcement.
   */
  public void announce(String path, byte[] bytes)
  {
    announce(path, bytes, true);
  }

  /**
   * Announces the provided bytes at the given path.
   *
   * <p>
   * Announcement using {@link Announcer} will create an ephemeral znode at the specified path, and uses its parent
   * path to watch all the siblings and children znodes of your specified path. The watched nodes will always exist
   * until it is unannounced, or until {@link #stop()} is called.
   * </p>
   *
   * @param path                  The path to announce at
   * @param bytes                 The payload to announce
   * @param removeParentIfCreated When {@link #stop()} is called, remove parent of "path" if we had created that
   *                              parent during announcement
   */
  public void announce(String path, byte[] bytes, boolean removeParentIfCreated)
  {
    synchronized (toAnnounce) {
      if (!started) {
        log.debug("Announcer has not started yet, queuing announcement for later processing...");
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
        log.warn(e, "Failed to check existence of parent path. Proceeding without creating parent path.");
      }

      // I don't have a watcher on this path yet, create a Map and start watching.
      announcements.putIfAbsent(parentPath, new ConcurrentHashMap<>());

      // Guaranteed to be non-null, but might be a map put in here by another thread.
      final ConcurrentMap<String, byte[]> finalSubPaths = announcements.get(parentPath);

      // Synchronize to make sure that I only create a listener once.
      synchronized (finalSubPaths) {
        if (!listeners.containsKey(parentPath)) {
          final CuratorCache cache = CuratorCache.builder(curator, parentPath).withOptions(CuratorCache.Options.COMPRESSED_DATA).build();

          cache.listenable().addListener(new CuratorCacheListener()
          {
            @Override
            public void event(Type type, ChildData oldData, ChildData data)
            {
              switch(type) {
                case NODE_DELETED:
                  final ZKPaths.PathAndNode childPath = ZKPaths.getPathAndNode(data.getPath());
                  final byte[] value = finalSubPaths.get(childPath.getNode());
                  if (value != null) {
                    log.info("Node[%s] dropped, reinstating.", data.getPath());
                    createAnnouncement(data.getPath(), value);
                  }
                  break;
                case NODE_CREATED:
                  if (addedChildren != null) {
                    addedChildren.add(data.getPath());
                  }
                  break;
              }
            }
          }, pathChildrenCacheExecutor);

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
        toUpdate.add(new Announceable(path, bytes));
        return;
      }
    }

    final ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(path);

    final String parentPath = pathAndNode.getPath();
    final String nodePath = pathAndNode.getNode();

    ConcurrentMap<String, byte[]> subPaths = announcements.get(parentPath);

    if (subPaths == null || subPaths.get(nodePath) == null) {
      throw new ISE("Cannot update path[%s] that hasn't been announced!", path);
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

  private void createAnnouncement(final String path, byte[] value)
  {
    try {
      curator.create().compressed().withMode(CreateMode.EPHEMERAL).inBackground().forPath(path, value);
    } catch (KeeperException.NodeExistsException e) {
      log.info(e, "Problem creating parentPath[%s], someone else created it first?", path);
    }
    catch (Exception e) {
      log.error(e, "Unhandled exception when creating parentPath[%s].", path);
    }
  }

  private void updateAnnouncement(final String path, final byte[] value)
  {
    try {
      curator.setData().compressed().inBackground().forPath(path, value);
    } catch (KeeperException.NodeExistsException e) {
      log.info(e, "Problem creating parentPath[%s], someone else created it first?", path);
    }
    catch (Exception e) {
      log.error(e, "Unhandled exception when creating parentPath[%s].", path);
    }
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
      CuratorOp deleteOp = curator.transactionOp().delete().forPath(path);
      curator.transaction().forOperations(deleteOp);
    }
    catch (KeeperException.NoNodeException e) {
      log.info("Unannounced node[%s] that does not exist.", path);
    }
    catch (KeeperException.NotEmptyException e) {
      log.warn("Unannouncing non-empty path[%s]", path);
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
    catch (KeeperException.NodeExistsException e) {
      log.info(e, "Problem creating parentPath[%s], someone else created it first?", parentPath);
    }
    catch (Exception e) {
      log.error(e, "Unhandled exception when creating parentPath[%s].", parentPath);
    }
  }
}

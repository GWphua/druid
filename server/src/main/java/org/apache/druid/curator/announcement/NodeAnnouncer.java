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
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.utils.ZKPaths;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.ZKPathsUtils;
import org.apache.druid.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

/**
 * The {@link NodeAnnouncer} class is responsible for announcing a single node
 * in a ZooKeeper ensemble. It creates an ephemeral node at a specified path
 * and monitors its existence to ensure that it remains active until it is
 * explicitly unannounced or the object is closed.
 *
 * <p>Use this class when you need to manage the lifecycle of a standalone
 * node. Should your use case involve watching all child nodes of your specified
 * path in a ZooKeeper ensemble, see {@link Announcer}.</p>
 */
public class NodeAnnouncer
{
  private static final Logger log = new Logger(NodeAnnouncer.class);

  private final CuratorFramework curator;
  private final ExecutorService pathNodeCacheExecutor;

  private final ConcurrentMap<String, NodeCache> listeners = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, byte[]> announcedPaths = new ConcurrentHashMap<>();

  @GuardedBy("toAnnounce")
  private boolean started = false;

  /**
   * This list holds paths that need to be announced. If a path is added to this list
   * in the {@link #announce(String, byte[], boolean)} method before the connection to ZooKeeper is established,
   * it will be stored here and announced later during the {@link #start} method.
   */
  @GuardedBy("toAnnounce")
  private final List<Announceable> toAnnounce = new ArrayList<>();

  /**
   * This list holds paths that need to be updated. If a path is added to this list
   * in the {@link #update} method before the connection to ZooKeeper is established,
   * it will be stored here and updated later during the {@link #start} method.
   */
  @GuardedBy("toAnnounce")
  private final List<Announceable> toUpdate = new ArrayList<>();

  /**
   * This list keeps track of all the paths created by this node announcer.
   * When the {@link #stop} method is called,
   * the node announcer is responsible for deleting all paths stored in this list.
   */
  @GuardedBy("toAnnounce")
  private final List<String> pathsCreatedInThisAnnouncer = new ArrayList<>();

  public NodeAnnouncer(CuratorFramework curator, ExecutorService exec)
  {
    this.curator = curator;
    this.pathNodeCacheExecutor = exec;
  }

  @VisibleForTesting
  Set<String> getAddedPaths()
  {
    return announcedPaths.keySet();
  }

  @LifecycleStart
  public void start()
  {
    log.info("Starting NodeAnnouncer");
    synchronized (toAnnounce) {
      if (started) {
        log.debug("Called start to an already-started NodeAnnouncer.");
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
    log.info("Stopping NodeAnnouncer");
    synchronized (toAnnounce) {
      if (!started) {
        log.debug("Called stop to NodeAnnouncer which is not started.");
        return;
      }

      started = false;
      closeResources();
      unannounceAllPaths();
      dropPathsCreatedInThisAnnouncer();
    }
  }

  @GuardedBy("toAnnounce")
  private void closeResources()
  {
    Closer closer = Closer.create();
    for (NodeCache cache : listeners.values()) {
      closer.register(cache);
    }
    for (String announcementPath : announcedPaths.keySet()) {
      closer.register(() -> unannounce(announcementPath));
    }

    try {
      CloseableUtils.closeAll(closer);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      pathNodeCacheExecutor.shutdown();
    }
  }

  @GuardedBy("toAnnounce")
  private void unannounceAllPaths() {
    for (String announcementPath: announcedPaths.keySet()) {
      unannounce(announcementPath);
    }
  }

  @GuardedBy("toAnnounce")
  private void dropPathsCreatedInThisAnnouncer()
  {
    if (!pathsCreatedInThisAnnouncer.isEmpty()) {
      final List<CuratorOp> deleteOps = new ArrayList<>(pathsCreatedInThisAnnouncer.size());
      for (String parent : pathsCreatedInThisAnnouncer) {
        try {
          deleteOps.add(curator.transactionOp().delete().forPath(parent));
        }
        catch (Exception e) {
          log.error(e, "Unable to delete parent[%s].", parent);
        }
      }

      try {
        curator.transaction().forOperations(deleteOps);
      }
      catch (Exception e) {
        log.error(e, "Unable to commit transaction.");
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
      // In the case that this method is called by other components or thread that assumes the NodeAnnouncer
      // is ready when NodeAnnouncer has not started, we will queue the announcement request.
      if (!started) {
        log.debug("NodeAnnouncer has not started yet, queuing announcement for later processing...");
        toAnnounce.add(new Announceable(path, bytes, removeParentIfCreated));
        return;
      }
    }

    final String parentPath = ZKPathsUtils.getParentPath(path);
    byte[] announcedPayload = announcedPaths.get(path);

    if (announcedPayload == null) {
      boolean buildParentPath = false;
      // Payload does not exist. We have yet to announce this path. Check if we need to build a parent path.
      try {
        buildParentPath = curator.checkExists().forPath(parentPath) == null;
      }
      catch (Exception e) {
        log.debug(e, "Failed to check existence of parent path. Proceeding without creating parent path.");
      }

      // Synchronize to make sure that I only create a listener once.
      synchronized (toAnnounce) {
        if (!listeners.containsKey(path)) {
          final NodeCache cache = setupNodeCache(path);

          if (started) {
            if (buildParentPath) {
              createPath(parentPath, removeParentIfCreated);
            }
            startCache(cache);
            listeners.put(path, cache);
          }
        }
      }
    }

    final boolean readyToCreateAnnouncement = updateAnnouncedPaths(path, bytes);

    if (readyToCreateAnnouncement) {
      try {
        createAnnouncement(path, bytes);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @GuardedBy("toAnnounce")
  private NodeCache setupNodeCache(String path)
  {
    final NodeCache cache = new NodeCache(curator, path, true);
    cache.getListenable().addListener(
        () -> {
          ChildData currentData = cache.getCurrentData();

          if (currentData == null) {
            // If currentData is null, and we record having announced the data,
            // this means that the ephemeral node was unexpectedly removed.
            // We will recreate the node again using the previous data.
            final byte[] previouslyAnnouncedData = announcedPaths.get(path);
            if (previouslyAnnouncedData != null) {
              log.info(
                  "Ephemeral node at path [%s] was unexpectedly removed. Recreating node with previous data.",
                  path
              );
              createAnnouncement(path, previouslyAnnouncedData);
            }
          }
        }
    );
    return cache;
  }

  private boolean updateAnnouncedPaths(String path, byte[] bytes)
  {
    synchronized (toAnnounce) {
      if (!started) {
        return false; // Do nothing if not started
      }
    }

    final byte[] updatedAnnouncementData = announcedPaths.compute(path, (key, oldBytes) -> {
      if (oldBytes == null) {
        return bytes; // Insert the new value
      } else if (!Arrays.equals(oldBytes, bytes)) {
        throw new IAE("Cannot reannounce different values under the same path.");
      }
      return oldBytes; // No change if values are equal
    });

    // Return true if we have updated the paths.
    return Arrays.equals(updatedAnnouncementData, bytes);
  }

  @GuardedBy("toAnnounce")
  private void createPath(String parentPath, boolean removeParentsIfCreated)
  {
    try {
      curator.create().creatingParentsIfNeeded().forPath(parentPath);
      if (removeParentsIfCreated) {
        pathsCreatedInThisAnnouncer.add(parentPath);
      }
      log.debug(
          "Created parentPath[%s], %s remove when stop() is called.",
          parentPath,
          removeParentsIfCreated ? "will" : "will not"
      );
    }
    catch (KeeperException.NodeExistsException e) {
      log.error(e, "The parentPath[%s] already exists.", parentPath);
    }
    catch (Exception e) {
      log.error(e, "Failed to create parentPath[%s].", parentPath);
    }
  }

  private void startCache(NodeCache cache)
  {
    try {
      cache.start();
    }
    catch (Exception e) {
      throw CloseableUtils.closeInCatch(new RuntimeException(e), cache);
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

    byte[] oldBytes = announcedPaths.get(path);

    if (oldBytes == null) {
      throw new ISE("Cannot update path[%s] that hasn't been announced!", path);
    }

    boolean canUpdate = false;
    synchronized (toAnnounce) {
      if (!Arrays.equals(oldBytes, bytes)) {
        announcedPaths.put(path, bytes);
        canUpdate = true;
      }
    }

    try {
      if (canUpdate) {
        updateAnnouncement(path, bytes);
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void createAnnouncement(final String path, byte[] value) throws Exception
  {
    curator.create().compressed().withMode(CreateMode.EPHEMERAL).inBackground().forPath(path, value);
  }

  private void updateAnnouncement(final String path, final byte[] value) throws Exception
  {
    curator.setData().compressed().inBackground().forPath(path, value);
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
    synchronized (toAnnounce) {
      log.info("unannouncing [%s]", path);
      final byte[] value = announcedPaths.remove(path);

      if (value == null) {
        log.error("Path[%s] not announced, cannot unannounce.", path);
        return;
      }
    }

    try {
      curator.transaction().forOperations(curator.transactionOp().delete().forPath(path));
    }
    catch (KeeperException.NoNodeException e) {
      log.info("Unannounced node[%s] that does not exist.", path);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

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

/**
 * The {@link Announceable} is a representation of an announcement to be made in ZooKeeper.
 */
class Announceable
{
  /**
   * Represents the path in ZooKeeper where the announcement will be made.
   */
  final String path;

  /**
   * Holds the actual data to be announced.
   */
  final byte[] bytes;

  /**
   * Indicates whether parent nodes created by this announcement should be removed during unannouncement.
   * This can be useful for cleaning up unused paths in ZooKeeper.
   */
  final boolean removeParentsIfCreated;

  /**
   * Used mainly for updating Announcing data, where the functionality of removing parent nodes are not applicable.
   *
   * @param path  Represents the path in ZooKeeper where the announcement will be made.
   * @param bytes Holds the actual data to be announced.
   */
  public Announceable(String path, byte[] bytes)
  {
    this.path = path;
    this.bytes = bytes;
    this.removeParentsIfCreated = false;
  }

  /**
   * Use when there is no need to remove created parent nodes when announcing this announceable instance.
   *
   * @param path                   Represents the path in ZooKeeper where the announcement will be made.
   * @param bytes                  Holds the actual data to be announced.
   * @param removeParentsIfCreated Indicates whether parent nodes created by this announcement should be removed
   *                               during unannouncement.
   */
  public Announceable(String path, byte[] bytes, boolean removeParentsIfCreated)
  {
    this.path = path;
    this.bytes = bytes;
    this.removeParentsIfCreated = removeParentsIfCreated;
  }
}

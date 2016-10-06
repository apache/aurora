/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.discovery;

import com.google.common.collect.ImmutableList;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;

/**
 * Utilities for dealing with ZooKeeper.
 */
final class ZooKeeperUtils {

  /**
   * An appropriate default session timeout for ZooKeeper clusters.
   */
  static final Amount<Integer, Time> DEFAULT_ZK_SESSION_TIMEOUT = Amount.of(4, Time.SECONDS);

  /**
   * An ACL that gives all permissions any user authenticated or not.
   */
  static final ImmutableList<ACL> OPEN_ACL_UNSAFE =
      ImmutableList.copyOf(Ids.OPEN_ACL_UNSAFE);

  /**
   * An ACL that gives all permissions to node creators and read permissions only to everyone else.
   */
  static final ImmutableList<ACL> EVERYONE_READ_CREATOR_ALL =
      ImmutableList.<ACL>builder()
          .addAll(Ids.CREATOR_ALL_ACL)
          .addAll(Ids.READ_ACL_UNSAFE)
          .build();

  private ZooKeeperUtils() {
    // utility
  }
}

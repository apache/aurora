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
package org.apache.aurora.common.net.pool;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.base.Command;

/**
 * Utility methods for dealing with dynamic sets of hosts.
 */
public final class DynamicHostSetUtil {

  /**
   * Gets a snapshot of a set of dynamic hosts (e.g. a ServerSet) and returns a readable copy of
   * the underlying actual endpoints.
   *
   * @param hostSet The hostSet to snapshot.
   * @throws DynamicHostSet.MonitorException if there was a problem obtaining the snapshot.
   */
  public static <T> ImmutableSet<T> getSnapshot(DynamicHostSet<T> hostSet) throws DynamicHostSet.MonitorException {
    final ImmutableSet.Builder<T> snapshot = ImmutableSet.builder();
    Command unwatch = hostSet.watch(new DynamicHostSet.HostChangeMonitor<T>() {
      @Override public void onChange(ImmutableSet<T> hostSet) {
        snapshot.addAll(hostSet);
      }
    });
    unwatch.execute();
    return snapshot.build();
  }

  private DynamicHostSetUtil() {
    // utility
  }
}

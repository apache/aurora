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
package org.apache.aurora.scheduler.storage.mem;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.apache.aurora.scheduler.storage.HostMaintenanceStore;
import org.apache.aurora.scheduler.storage.entities.IHostMaintenanceRequest;

public class MemHostMaintenanceStore implements HostMaintenanceStore.Mutable {

  private final Map<String, IHostMaintenanceRequest> hostMaintenanceRequests =
      Maps.newConcurrentMap();

  @Override
  public Optional<IHostMaintenanceRequest> getHostMaintenanceRequest(String host) {
    return Optional.ofNullable(hostMaintenanceRequests.get(host));
  }

  @Override
  public Set<IHostMaintenanceRequest> getHostMaintenanceRequests() {
    return ImmutableSet.copyOf(hostMaintenanceRequests.values());
  }

  @Override
  public void deleteHostMaintenanceRequests() {
    hostMaintenanceRequests.clear();
  }

  @Override
  public void saveHostMaintenanceRequest(IHostMaintenanceRequest hostMaintenanceRequest) {
    hostMaintenanceRequests.put(hostMaintenanceRequest.getHost(), hostMaintenanceRequest);
  }

  @Override
  public void removeHostMaintenanceRequest(String host) {
    hostMaintenanceRequests.remove(host);
  }
}

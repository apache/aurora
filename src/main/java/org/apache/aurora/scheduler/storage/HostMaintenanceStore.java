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
package org.apache.aurora.scheduler.storage;

import java.util.Optional;
import java.util.Set;

import org.apache.aurora.scheduler.storage.entities.IHostMaintenanceRequest;

/**
 * Stores maintenance requests for hosts.
 */
public interface HostMaintenanceStore {

  /**
   * Returns the maintenance request for the given host if exists.
   * {@link Optional#empty()} otherwise.
   * @param host Name of the host
   * @return {@link Optional} of {@link IHostMaintenanceRequest}
   */
  Optional<IHostMaintenanceRequest> getHostMaintenanceRequest(String host);

  /**
   * Returns all {@link IHostMaintenanceRequest}s currently in storage.
   * @return {@link Set} of {@link IHostMaintenanceRequest}s
   */
  Set<IHostMaintenanceRequest> getHostMaintenanceRequests();

  /**
   * Provides write operations to the {@link HostMaintenanceStore}.
   */
  interface Mutable extends HostMaintenanceStore {
    /**
     * Deletes all attributes in the store.
     */
    void deleteHostMaintenanceRequests();

    /**
     * Saves the maintenance request for the given host.
     * @param hostMaintenanceRequest {@link IHostMaintenanceRequest}
     */
    void saveHostMaintenanceRequest(IHostMaintenanceRequest hostMaintenanceRequest);

    /**
     * Removes the maintenance request for the given host if one exists.
     * @param host Name of the host
     */
    void removeHostMaintenanceRequest(String host);
  }
}

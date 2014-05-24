/**
 *
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

import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;

/**
 * Storage interface for host attributes.
 */
public interface AttributeStore {
  /**
   * Fetches all host attributes given by the host.
   *
   * @param host host name.
   * @return attributes associated with {@code host}, if the host is known.
   */
  Optional<HostAttributes> getHostAttributes(String host);

  /**
   * Fetches all attributes in the store.
   *
   * @return All host attributes.
   */
  Set<HostAttributes> getHostAttributes();

  /**
   * Attributes are considered mostly ephemeral and extremely low risk when inconsistency
   * is present.
   */
  public interface Mutable extends AttributeStore {

    /**
     * Deletes all attributes in the store.
     */
    void deleteHostAttributes();

    /**
     * Save a host attribute in the attribute store.
     *
     * @param hostAttributes The attribute we are going to save.
     */
    void saveHostAttributes(HostAttributes hostAttributes);

    /**
     * Adjusts the maintenance mode for a host.
     * No adjustment will be made if the host is unknown.
     *
     * @param host Host to adjust.
     * @param mode Mode to place the host in.
     * @return {@code true} if the host is known and the state was adjusted,
     *         {@code false} if the host is unrecognized.
     */
    boolean setMaintenanceMode(String host, MaintenanceMode mode);
  }

  public static final class Util {
    private Util() {
    }

    /**
     * Fetches attributes about a {@code host}.
     *
     * @param store Store to fetch host attributes from.
     * @param host Host to fetch attributes about.
     * @return Attributes associated with {@code host}, or an empty iterable if the host is
     *         unknown.
     */
    public static Iterable<Attribute> attributesOrNone(StoreProvider store, String host) {
      Optional<HostAttributes> attributes = store.getAttributeStore().getHostAttributes(host);
      return attributes.isPresent()
          ? attributes.get().getAttributes() : ImmutableList.<Attribute>of();
    }
  }

}

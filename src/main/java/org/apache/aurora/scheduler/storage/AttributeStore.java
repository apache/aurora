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

import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.scheduler.base.Conversions;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.entities.IAttribute;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.mesos.v1.Protos;

/**
 * Storage interface for host attributes.
 */
public interface AttributeStore {
  /**
   * Fetches all host attributes given by the host.
   * <p>
   * TODO(wfarner): We need to transition this API to key off slave ID instead of host name, as
   * the host is not guaranteed to be unique (it's possible, and sometimes desirable to have
   * multiple slaves on a machine).  The current API ripples down into storage, since the store
   * is not in the position to dictate behavior when host names collide.  Most callers seem to have
   * sufficient context to use slave ID instead, with the exception of those related to host
   * maintenance.
   *
   * @param host host name.
   * @return attributes associated with {@code host}, if the host is known.
   */
  Optional<IHostAttributes> getHostAttributes(String host);

  /**
   * Fetches all attributes in the store.
   *
   * @return All host attributes.
   */
  Set<IHostAttributes> getHostAttributes();

  /**
   * Attributes are considered mostly ephemeral and extremely low risk when inconsistency
   * is present.
   */
  interface Mutable extends AttributeStore {

    /**
     * Deletes all attributes in the store.
     */
    void deleteHostAttributes();

    /**
     * Save a host attribute in the attribute store.
     *
     * @param hostAttributes The attribute we are going to save.
     * @return {@code true} if the operation changed the attributes stored for the given
     *         {@link IHostAttributes#getHost() host}, or {@code false} if the save was a no-op.
     */
    boolean saveHostAttributes(IHostAttributes hostAttributes);
  }

  final class Util {
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
    public static Iterable<IAttribute> attributesOrNone(StoreProvider store, String host) {
      Optional<IHostAttributes> attributes = store.getAttributeStore().getHostAttributes(host);
      return attributes.isPresent()
          ? attributes.get().getAttributes() : ImmutableList.of();
    }

    /**
     * Merges a desired maintenance mode with the existing attributes of a host, if present.
     *
     * @param store The store to fetch existing attributes from.
     * @param host The host to merge existing attributes from.
     * @param mode Maintenance mode to save if the host is known.
     * @return The attributes that should be saved if there were already attributes stored for
     *         the {@code host}, or {@link Optional#absent() none} if the host is unknown and
     *         attributes should not be saved.
     */
    public static Optional<IHostAttributes> mergeMode(
        AttributeStore store,
        String host,
        MaintenanceMode mode) {

      Optional<IHostAttributes> stored = store.getHostAttributes(host);
      if (stored.isPresent()) {
        return Optional.of(IHostAttributes.build(stored.get().newBuilder().setMode(mode)));
      } else {
        return Optional.absent();
      }
    }

    /**
     * Merges the attributes from an offer, applying the default maintenance mode.
     *
     * @param store Store to fetch the existing maintenance mode for this host.
     * @param offer Offer to merge.
     * @return attributes from {@code offer} and the existing (or default) maintenance mode.
     */
    public static IHostAttributes mergeOffer(AttributeStore store, Protos.Offer offer) {
      IHostAttributes fromOffer = Conversions.getAttributes(offer);
      MaintenanceMode mode = store.getHostAttributes(fromOffer.getHost())
          .transform(IHostAttributes::getMode)
          .or(MaintenanceMode.NONE);
      return IHostAttributes.build(fromOffer.newBuilder().setMode(mode));
    }
  }
}

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
package org.apache.aurora.scheduler.offers;

import java.time.Instant;
import java.util.List;

import com.google.common.collect.Ordering;

import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.resources.ResourceType;

import static org.apache.aurora.gen.MaintenanceMode.DRAINED;
import static org.apache.aurora.gen.MaintenanceMode.DRAINING;
import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.apache.aurora.gen.MaintenanceMode.SCHEDULED;
import static org.apache.aurora.scheduler.resources.ResourceManager.bagFromMesosResources;
import static org.apache.aurora.scheduler.resources.ResourceManager.getNonRevocableOfferResources;
import static org.apache.aurora.scheduler.resources.ResourceManager.getRevocableOfferResources;

/**
 * Utility class for creating compounded offer orders based on some combination of offer ordering.
 */
final class OfferOrderBuilder {
  private OfferOrderBuilder() {

  }

  private static final Ordering<HostOffer> AURORA_MAINTENANCE_COMPARATOR =
      Ordering.explicit(NONE, SCHEDULED, DRAINING, DRAINED)
          .onResultOf(offer -> offer.getAttributes().getMode());

  // We should not prefer offers from agents that are scheduled to become unavailable.
  // We should also sort the unavailability start to prefer agents that are starting
  // maintenance later.
  private static final Ordering<HostOffer> MESOS_MAINTENANCE_COMPARATOR =
      Ordering
          .natural()
          .reverse()
          .onResultOf(o -> o.getUnavailabilityStart().or(Instant.MAX));

  private static final Ordering<HostOffer> BASE_COMPARATOR =
      AURORA_MAINTENANCE_COMPARATOR.compound(MESOS_MAINTENANCE_COMPARATOR);

  private static final Ordering<Object> RANDOM_COMPARATOR = Ordering.arbitrary();
  private static final Ordering<HostOffer> CPU_COMPARATOR =
      nonRevocableResourceOrdering(ResourceType.CPUS);
  private static final Ordering<HostOffer> RAM_COMPARATOR =
      nonRevocableResourceOrdering(ResourceType.RAM_MB);
  private static final Ordering<HostOffer> DISK_COMPARATOR =
      nonRevocableResourceOrdering(ResourceType.DISK_MB);
  private static final Ordering<HostOffer> REVOCABLE_CPU_COMPARATOR =
      revocableResourceOrdering(ResourceType.CPUS);

  private static Ordering<HostOffer> nonRevocableResourceOrdering(ResourceType resourceType) {
    return Ordering
        .natural()
        .onResultOf(o -> bagFromMesosResources(
            getNonRevocableOfferResources(o.getOffer())).valueOf(resourceType));
  }

  private static Ordering<HostOffer> revocableResourceOrdering(ResourceType resourceType) {
    return Ordering
        .natural()
        .onResultOf(o -> {
          double resource = bagFromMesosResources(
              getRevocableOfferResources(o.getOffer())).valueOf(resourceType);
          // resource will be 0.0 if there is no revocable cpus available. Since the purpose of
          // this ordering is to bin-pack revocable then we push those offers to the back.
          return resource == 0.0 ? Double.MAX_VALUE : resource;
        });
  }

  private static Ordering<HostOffer> getOrdering(Ordering<HostOffer> base, OfferOrder order) {
    // Random is Ordering<Object> so accepting base as a parameter and compounding in here is the
    // cleanest way I could come up with to avoid a whole bunch of type finagling.
    switch(order) {
      case CPU: return base.compound(CPU_COMPARATOR);
      case DISK: return base.compound(DISK_COMPARATOR);
      case MEMORY: return base.compound(RAM_COMPARATOR);
      case REVOCABLE_CPU: return base.compound(REVOCABLE_CPU_COMPARATOR);
      default: return base.compound(RANDOM_COMPARATOR);
    }
  }

  private static Ordering<HostOffer> create(Ordering<HostOffer> base, List<OfferOrder> order) {
    if (order.isEmpty()) {
      return base;
    }
    Ordering<HostOffer> compounded = getOrdering(base, order.get(0));
    if (order.size() > 1) {
      return create(compounded, order.subList(1, order.size()));
    } else {
      return compounded;
    }
  }

  /**
   * Create a total offer ordering, based on a general machine maintenance ordering. Additional
   * ordering is compounded based on the list order.
   *
   * @param order The list of offer orders. They will be compounded in the list order.
   * @return A HostOffer ordering.
   */
  static Ordering<HostOffer> create(List<OfferOrder> order) {
    return create(BASE_COMPARATOR, order);
  }
}

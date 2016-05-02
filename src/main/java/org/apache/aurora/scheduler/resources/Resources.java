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
package org.apache.aurora.scheduler.resources;

import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.scheduler.TierInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Value.Range;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;

/**
 * A container for multiple Mesos resource vectors.
 */
public final class Resources {

  /**
   * CPU resource filter.
   */
  private static final Predicate<Resource> CPU = e -> e.getName().equals(CPUS.getMesosName());

  /**
   * Revocable resource filter.
   */
  public static final Predicate<Resource> REVOCABLE =
      Predicates.or(Predicates.not(CPU), Predicates.and(CPU, Resource::hasRevocable));

  /**
   * Non-revocable resource filter.
   */
  public static final Predicate<Resource> NON_REVOCABLE = Predicates.not(Resource::hasRevocable);

  /**
   * Convert range to set of integers.
   */
  public static final Function<Range, Set<Integer>> RANGE_TO_MEMBERS =
      range -> ContiguousSet.create(
          com.google.common.collect.Range.closed((int) range.getBegin(), (int) range.getEnd()),
          DiscreteDomain.integers());

  private final Iterable<Resource> mesosResources;

  private Resources(Iterable<Resource> mesosResources) {
    this.mesosResources = ImmutableList.copyOf(mesosResources);
  }

  /**
   * Extracts the resources available in a slave offer.
   *
   * @param offer Offer to get resources from.
   * @return The resources available in the offer.
   */
  public static Resources from(Offer offer) {
    return new Resources(requireNonNull(offer.getResourcesList()));
  }

  /**
   * Filters resources by the provided {@code predicate}.
   *
   * @param predicate Predicate filter.
   * @return A new {@code Resources} object containing only filtered Mesos resources.
   */
  public Resources filter(Predicate<Resource> predicate) {
    return new Resources(Iterables.filter(mesosResources, predicate));
  }

  /**
   * Filters resources using the provided {@code tierInfo} instance.
   *
   * @param tierInfo Tier info.
   * @return A new {@code Resources} object containing only filtered Mesos resources.
   */
  public Resources filter(TierInfo tierInfo) {
    return filter(tierInfo.isRevocable() ? REVOCABLE : NON_REVOCABLE);
  }

  /**
   * Gets generalized aggregated resource view.
   *
   * @return {@code ResourceSlot} instance.
   */
  public ResourceSlot slot() {
    return new ResourceSlot(getScalarValue(CPUS.getMesosName()),
        Amount.of((long) getScalarValue(RAM_MB.getMesosName()), Data.MB),
        Amount.of((long) getScalarValue(DISK_MB.getMesosName()), Data.MB),
        getNumAvailablePorts());
  }

  private int getNumAvailablePorts() {
    int offeredPorts = 0;
    for (Range range : getPortRanges()) {
      offeredPorts += 1 + range.getEnd() - range.getBegin();
    }
    return offeredPorts;
  }

  private double getScalarValue(String key) {
    Iterable<Resource> resources = getResources(key);
    double value = 0;
    for (Resource r : resources) {
      value += r.getScalar().getValue();
    }
    return value;
  }

  private Iterable<Resource> getResources(String key) {
    return Iterables.filter(mesosResources, e -> e.getName().equals(key));
  }

  private Iterable<Range> getPortRanges() {
    ImmutableList.Builder<Range> ranges = ImmutableList.builder();
    for (Resource r : getResources(PORTS.getMesosName())) {
      ranges.addAll(r.getRanges().getRangeList().iterator());
    }

    return ranges.build();
  }

  /**
   * Thrown when there are insufficient resources to satisfy a request.
   */
  public static class InsufficientResourcesException extends RuntimeException {
    InsufficientResourcesException(String message) {
      super(message);
    }
  }
}

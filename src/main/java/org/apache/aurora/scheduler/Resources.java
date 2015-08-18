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
package org.apache.aurora.scheduler;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;

import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Value.Range;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.ResourceType.CPUS;
import static org.apache.aurora.scheduler.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.ResourceType.PORTS;
import static org.apache.aurora.scheduler.ResourceType.RAM_MB;

/**
 * A container for multiple Mesos resource vectors.
 */
public final class Resources {

  /**
   * CPU resource filter.
   */
  private static final Predicate<Resource> CPU = e -> e.getName().equals(CPUS.getName());

  /**
   * Revocable resource filter.
   */
  @VisibleForTesting
  static final Predicate<Resource> REVOCABLE =
      Predicates.or(Predicates.not(CPU), Predicates.and(CPU, Resource::hasRevocable));

  /**
   * Non-revocable resource filter.
   */
  private static final Predicate<Resource> NON_REVOCABLE = Predicates.not(Resource::hasRevocable);

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
    return new ResourceSlot(getScalarValue(CPUS.getName()),
        Amount.of((long) getScalarValue(RAM_MB.getName()), Data.MB),
        Amount.of((long) getScalarValue(DISK_MB.getName()), Data.MB),
        getNumAvailablePorts());
  }

  /**
   * Attempts to grab {@code numPorts} from this resource instance.
   *
   * @param numPorts The number of ports to grab.
   * @return The set of ports grabbed.
   * @throws InsufficientResourcesException if not enough ports were available.
   */
  public Set<Integer> getPorts(int numPorts)
      throws InsufficientResourcesException {

    if (numPorts == 0) {
      return ImmutableSet.of();
    }

    List<Integer> availablePorts = Lists.newArrayList(Sets.newHashSet(Iterables.concat(
        Iterables.transform(getPortRanges(), RANGE_TO_MEMBERS))));

    if (availablePorts.size() < numPorts) {
      throw new InsufficientResourcesException(
          String.format("Could not get %d ports from %s", numPorts, availablePorts));
    }

    Collections.shuffle(availablePorts);
    return ImmutableSet.copyOf(availablePorts.subList(0, numPorts));
  }

  private int getNumAvailablePorts() {
    int offeredPorts = 0;
    for (Range range : getPortRanges()) {
      offeredPorts += 1 + range.getEnd() - range.getBegin();
    }
    return offeredPorts;
  }

  private double getScalarValue(String key) {
    Resource resource = getResource(key);
    if (resource == null) {
      return 0;
    }

    return resource.getScalar().getValue();
  }

  private Resource getResource(String key) {
    return Iterables.find(mesosResources, e -> e.getName().equals(key), null);
  }

  private Iterable<Range> getPortRanges() {
    Resource resource = getResource(PORTS.getName());
    if (resource == null) {
      return ImmutableList.of();
    }

    return resource.getRanges().getRangeList();
  }

  /**
   * Thrown when there are insufficient resources to satisfy a request.
   */
  static class InsufficientResourcesException extends RuntimeException {
    public InsufficientResourcesException(String message) {
      super(message);
    }
  }

  private static final Function<Range, Set<Integer>> RANGE_TO_MEMBERS =
      new Function<Range, Set<Integer>>() {
        @Override
        public Set<Integer> apply(Range range) {
          return ContiguousSet.create(
              com.google.common.collect.Range.closed((int) range.getBegin(), (int) range.getEnd()),
              DiscreteDomain.integers());
        }
      };
}

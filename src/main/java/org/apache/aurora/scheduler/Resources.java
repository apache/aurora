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
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Function;
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
 * A container for multiple resource vectors.
 * TODO(wfarner): Collapse this in with ResourceAggregates AURORA-105.
 */
public final class Resources {
  private static final Function<Range, Set<Integer>> RANGE_TO_MEMBERS =
      new Function<Range, Set<Integer>>() {
        @Override
        public Set<Integer> apply(Range range) {
          return ContiguousSet.create(
              com.google.common.collect.Range.closed((int) range.getBegin(), (int) range.getEnd()),
              DiscreteDomain.integers());
        }
      };

  private final double numCpus;
  private final Amount<Long, Data> disk;
  private final Amount<Long, Data> ram;
  private final int numPorts;

  private Resources(double numCpus, Amount<Long, Data> ram, Amount<Long, Data> disk, int numPorts) {
    this.numCpus = numCpus;
    this.ram = requireNonNull(ram);
    this.disk = requireNonNull(disk);
    this.numPorts = numPorts;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Resources)) {
      return false;
    }

    Resources other = (Resources) o;
    return Objects.equals(numCpus, other.numCpus)
        && Objects.equals(ram, other.ram)
        && Objects.equals(disk, other.disk)
        && Objects.equals(numPorts, other.numPorts);
  }

  @Override
  public int hashCode() {
    return Objects.hash(numCpus, ram, disk, numPorts);
  }

  @Override
  public String toString() {
    return com.google.common.base.Objects.toStringHelper(this)
        .add("numCpus", numCpus)
        .add("ram", ram)
        .add("disk", disk)
        .add("numPorts", numPorts)
        .toString();
  }

  /**
   * Extracts the resources available in a slave offer.
   *
   * @param offer Offer to get resources from.
   * @return The resources available in the offer.
   */
  public static Resources from(Offer offer) {
    requireNonNull(offer);
    return new Resources(
        getScalarValue(offer, CPUS.getName()),
        Amount.of((long) getScalarValue(offer, RAM_MB.getName()), Data.MB),
        Amount.of((long) getScalarValue(offer, DISK_MB.getName()), Data.MB),
        getNumAvailablePorts(offer.getResourcesList()));
  }

  private static int getNumAvailablePorts(List<Resource> resource) {
    int offeredPorts = 0;
    for (Range range : getPortRanges(resource)) {
      offeredPorts += 1 + range.getEnd() - range.getBegin();
    }
    return offeredPorts;
  }

  private static double getScalarValue(Offer offer, String key) {
    return getScalarValue(offer.getResourcesList(), key);
  }

  private static double getScalarValue(List<Resource> resources, String key) {
    Resource resource = getResource(resources, key);
    if (resource == null) {
      return 0;
    }

    return resource.getScalar().getValue();
  }

  private static Resource getResource(List<Resource> resource, String key) {
    return Iterables.find(resource, e -> e.getName().equals(key), null);
  }

  private static Iterable<Range> getPortRanges(List<Resource> resources) {
    Resource resource = getResource(resources, PORTS.getName());
    if (resource == null) {
      return ImmutableList.of();
    }

    return resource.getRanges().getRangeList();
  }

  /**
   * Gets generalized aggregated resource view.
   *
   * @return {@code ResourceSlot} instance.
   */
  public ResourceSlot slot() {
    return new ResourceSlot(numCpus, ram, disk, numPorts);
  }

  /**
   * Thrown when there are insufficient resources to satisfy a request.
   */
  static class InsufficientResourcesException extends RuntimeException {
    public InsufficientResourcesException(String message) {
      super(message);
    }
  }

  /**
   * Attempts to grab {@code numPorts} from the given resource {@code offer}.
   *
   * @param offer The offer to grab ports from.
   * @param numPorts The number of ports to grab.
   * @return The set of ports grabbed.
   * @throws InsufficientResourcesException if not enough ports were available.
   */
  public static Set<Integer> getPorts(Offer offer, int numPorts)
      throws InsufficientResourcesException {

    requireNonNull(offer);

    if (numPorts == 0) {
      return ImmutableSet.of();
    }

    List<Integer> availablePorts = Lists.newArrayList(Sets.newHashSet(
        Iterables.concat(
            Iterables.transform(getPortRanges(offer.getResourcesList()), RANGE_TO_MEMBERS))));

    if (availablePorts.size() < numPorts) {
      throw new InsufficientResourcesException(
          String.format("Could not get %d ports from %s", numPorts, offer));
    }

    Collections.shuffle(availablePorts);
    return ImmutableSet.copyOf(availablePorts.subList(0, numPorts));
  }
}

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
package org.apache.aurora.scheduler.configuration;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;

import org.apache.aurora.scheduler.base.Numbers;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.Protos.Value.Ranges;
import org.apache.mesos.Protos.Value.Scalar;
import org.apache.mesos.Protos.Value.Type;

import static java.util.Objects.requireNonNull;

/**
 * A container for multiple resource vectors.
 * TODO(wfarner): Collapse this in with ResourceAggregates AURORA-105.
 */
public class Resources {

  public static final String CPUS = "cpus";
  public static final String RAM_MB = "mem";
  public static final String DISK_MB = "disk";
  public static final String PORTS = "ports";

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

  /**
   * Creates a new resources object.
   *
   * @param numCpus Number of CPUs.
   * @param ram Amount of RAM.
   * @param disk Amount of disk.
   * @param numPorts Number of ports.
   */
  public Resources(double numCpus, Amount<Long, Data> ram, Amount<Long, Data> disk, int numPorts) {
    this.numCpus = numCpus;
    this.ram = requireNonNull(ram);
    this.disk = requireNonNull(disk);
    this.numPorts = numPorts;
  }

  /**
   * Tests whether this bundle of resources is greater than or equal to another bundle of resources.
   *
   * @param other Resources being compared to.
   * @return {@code true} if all resources in this bundle are greater than or equal to the
   *    equivalents from {@code other}, otherwise {@code false}.
   */
  public boolean greaterThanOrEqual(Resources other) {
    return numCpus >= other.numCpus
        && disk.as(Data.MB) >= other.disk.as(Data.MB)
        && ram.as(Data.MB) >= other.ram.as(Data.MB)
        && numPorts >= other.numPorts;
  }

  /**
   * Adapts this resources object to a list of mesos resources.
   *
   * @param selectedPorts The ports selected, to be applied as concrete task ranges.
   * @return Mesos resources.
   */
  public List<Resource> toResourceList(Set<Integer> selectedPorts) {
    ImmutableList.Builder<Resource> resourceBuilder =
      ImmutableList.<Resource>builder()
          .add(Resources.makeMesosResource(CPUS, numCpus))
          .add(Resources.makeMesosResource(DISK_MB, disk.as(Data.MB)))
          .add(Resources.makeMesosResource(RAM_MB, ram.as(Data.MB)));
    if (!selectedPorts.isEmpty()) {
      resourceBuilder.add(Resources.makeMesosRangeResource(Resources.PORTS, selectedPorts));
    }

    return resourceBuilder.build();
  }

  /**
   * Convenience method for adapting to mesos resources without applying a port range.
   *
   * @see {@link #toResourceList(java.util.Set)}
   * @return Mesos resources.
   */
  public List<Resource> toResourceList() {
    return toResourceList(ImmutableSet.<Integer>of());
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

  /**
   * Extracts the resources required from a task.
   *
   * @param task Task to get resources from.
   * @return The resources required by the task.
   */
  public static Resources from(ITaskConfig task) {
    requireNonNull(task);
    return new Resources(
        task.getNumCpus(),
        Amount.of(task.getRamMb(), Data.MB),
        Amount.of(task.getDiskMb(), Data.MB),
        task.getRequestedPorts().size());
  }

  /**
   * Extracts the resources specified in a list of resource objects.
   *
   * @param resources Resources to translate.
   * @return The canonical resources.
   */
  public static Resources from(List<Resource> resources) {
    requireNonNull(resources);
    return new Resources(
        getScalarValue(resources, CPUS),
        Amount.of((long) getScalarValue(resources, RAM_MB), Data.MB),
        Amount.of((long) getScalarValue(resources, DISK_MB), Data.MB),
        getNumAvailablePorts(resources)
    );
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
        getScalarValue(offer, CPUS),
        Amount.of((long) getScalarValue(offer, RAM_MB), Data.MB),
        Amount.of((long) getScalarValue(offer, DISK_MB), Data.MB),
        getNumAvailablePorts(offer.getResourcesList()));
  }

  private static final Resources NO_RESOURCES =
      new Resources(0, Amount.of(0L, Data.BITS), Amount.of(0L, Data.BITS), 0);

  private static Resources none() {
    return NO_RESOURCES;
  }

  /**
   * a - b.
   */
  public static Resources subtract(Resources a, Resources b) {
    return new Resources(
        a.getNumCpus() - b.getNumCpus(),
        Amount.of(a.getRam().as(Data.MB) - b.getRam().as(Data.MB), Data.MB),
        Amount.of(a.getDisk().as(Data.MB) - b.getDisk().as(Data.MB), Data.MB),
        a.getNumPorts() - b.getNumPorts());
  }

  /**
   * sum(a, b).
   */
  public static Resources sum(Resources a, Resources b) {
    return sum(ImmutableList.of(a, b));
  }

  /**
   * sum(rs).
   */
  public static Resources sum(Iterable<Resources> rs) {
    Resources sum = none();

    for (Resources r : rs) {
      double numCpus = sum.getNumCpus() + r.getNumCpus();
      Amount<Long, Data> disk =
          Amount.of(sum.getDisk().as(Data.BYTES) + r.getDisk().as(Data.BYTES), Data.BYTES);
      Amount<Long, Data> ram =
          Amount.of(sum.getRam().as(Data.BYTES) + r.getRam().as(Data.BYTES), Data.BYTES);
      int ports = sum.getNumPorts() + r.getNumPorts();
      sum =  new Resources(numCpus, ram, disk, ports);
    }

    return sum;
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
      return Iterables.find(resource, withName(key), null);
  }

  private static Predicate<Resource> withName(final String name) {
    return new Predicate<Resource>() {
      @Override
      public boolean apply(Resource resource) {
        return resource.getName().equals(name);
      }
    };
  }

  private static Iterable<Range> getPortRanges(List<Resource> resources) {
    Resource resource = getResource(resources, Resources.PORTS);
    if (resource == null) {
      return ImmutableList.of();
    }

    return resource.getRanges().getRangeList();
  }

  /**
   * Creates a scalar mesos resource.
   *
   * @param name Name of the resource.
   * @param value Value for the resource.
   * @return A mesos resource.
   */
  public static Resource makeMesosResource(String name, double value) {
    return Resource.newBuilder().setName(name).setType(Type.SCALAR)
        .setScalar(Scalar.newBuilder().setValue(value)).build();
  }

  private static final Function<com.google.common.collect.Range<Integer>, Range> RANGE_TRANSFORM =
      new Function<com.google.common.collect.Range<Integer>, Range>() {
        @Override
        public Range apply(com.google.common.collect.Range<Integer> input) {
          return Range.newBuilder()
              .setBegin(input.lowerEndpoint())
              .setEnd(input.upperEndpoint())
              .build();
        }
      };

  /**
   * Creates a mesos resource of integer ranges.
   *
   * @param name Name of the resource
   * @param values Values to translate into ranges.
   * @return A mesos ranges resource.
   */
  @VisibleForTesting
  public static Resource makeMesosRangeResource(String name, Set<Integer> values) {
    return Resource.newBuilder()
        .setName(name)
        .setType(Type.RANGES)
        .setRanges(Ranges.newBuilder()
            .addAllRange(Iterables.transform(Numbers.toRanges(values), RANGE_TRANSFORM)))
        .build();
  }

  /**
   * Number of CPUs.
   *
   * @return CPUs.
   */
  public double getNumCpus() {
    return numCpus;
  }

  /**
   * Disk amount.
   *
   * @return Disk.
   */
  public Amount<Long, Data> getDisk() {
    return disk;
  }

  /**
   * RAM amount.
   *
   * @return RAM.
   */
  public Amount<Long, Data> getRam() {
    return ram;
  }

  /**
   * Number of ports.
   *
   * @return Port count.
   */
  public int getNumPorts() {
    return numPorts;
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

  /**
   * A Resources object is greater than another iff _all_ of its resource components are greater
   * or equal. A Resources object compares as equal if some but not all components are greater than
   * or equal to the other.
   */
  public static final Ordering<Resources> RESOURCE_ORDER = new Ordering<Resources>() {
    @Override
    public int compare(Resources left, Resources right) {
      int diskC = left.getDisk().compareTo(right.getDisk());
      int ramC = left.getRam().compareTo(right.getRam());
      int portC = Integer.compare(left.getNumPorts(), right.getNumPorts());
      int cpuC = Double.compare(left.getNumCpus(), right.getNumCpus());

      FluentIterable<Integer> vector =
          FluentIterable.from(ImmutableList.of(diskC, ramC, portC, cpuC));

      if (vector.allMatch(IS_ZERO))  {
        return 0;
      }

      if (vector.filter(Predicates.not(IS_ZERO)).allMatch(IS_POSITIVE)) {
        return 1;
      }

      if (vector.filter(Predicates.not(IS_ZERO)).allMatch(IS_NEGATIVE)) {
        return -1;
      }

      return 0;
    }
  };

  private static final Predicate<Integer> IS_POSITIVE = new Predicate<Integer>() {
    @Override
    public boolean apply(Integer input) {
      return input > 0;
    }
  };

  private static final Predicate<Integer> IS_NEGATIVE = new Predicate<Integer>() {
    @Override
    public boolean apply(Integer input) {
      return input < 0;
    }
  };

  private static final Predicate<Integer> IS_ZERO = new Predicate<Integer>() {
    @Override
    public boolean apply(Integer input) {
      return input == 0;
    }
  };
}

/*
 * Copyright 2013 Twitter, Inc.
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
package com.twitter.aurora.scheduler.configuration;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.Protos.Value.Ranges;
import org.apache.mesos.Protos.Value.Scalar;
import org.apache.mesos.Protos.Value.Type;

import com.twitter.aurora.scheduler.base.Numbers;
import com.twitter.aurora.scheduler.storage.entities.ITaskConfig;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A container for multiple resource vectors.
 */
public class Resources {

  public static final String CPUS = "cpus";
  public static final String RAM_MB = "mem";
  public static final String DISK_MB = "disk";
  public static final String PORTS = "ports";

  private static final Function<Range, Set<Integer>> RANGE_TO_MEMBERS =
      new Function<Range, Set<Integer>>() {
        @Override public Set<Integer> apply(Range range) {
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
    this.ram = checkNotNull(ram);
    this.disk = checkNotNull(disk);
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
    return (numCpus >= other.numCpus)
        && (disk.as(Data.MB) >= other.disk.as(Data.MB))
        && (ram.as(Data.MB) >= other.ram.as(Data.MB))
        && (numPorts >= other.numPorts);
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
    if (selectedPorts.size() > 0) {
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
    return new EqualsBuilder()
        .append(numCpus, other.numCpus)
        .append(ram, other.ram)
        .append(disk, other.disk)
        .append(numPorts, other.numPorts)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(numCpus, ram, disk, numPorts);
  }

  /**
   * Extracts the resources required from a task.
   *
   * @param task Task to get resources from.
   * @return The resources required by the task.
   */
  public static Resources from(ITaskConfig task) {
    checkNotNull(task);
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
    checkNotNull(resources);
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
    checkNotNull(offer);
    return new Resources(
        getScalarValue(offer, CPUS),
        Amount.of((long) getScalarValue(offer, RAM_MB), Data.MB),
        Amount.of((long) getScalarValue(offer, DISK_MB), Data.MB),
        getNumAvailablePorts(offer.getResourcesList()));
  }

  private static int getNumAvailablePorts(List<Resource> resource) {
    int offeredPorts = 0;
    for (Range range : getPortRanges(resource)) {
      offeredPorts += 1 + (range.getEnd() - range.getBegin());
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
      @Override public boolean apply(Resource resource) {
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
        @Override public Range apply(com.google.common.collect.Range<Integer> input) {
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
  static Resource makeMesosRangeResource(String name, Set<Integer> values) {
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

    checkNotNull(offer);

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

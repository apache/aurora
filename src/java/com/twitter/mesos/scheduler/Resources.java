package com.twitter.mesos.scheduler;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.DiscreteDomains;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Resource.Range;
import org.apache.mesos.Protos.Resource.Ranges;
import org.apache.mesos.Protos.Resource.Scalar;
import org.apache.mesos.Protos.Resource.Type;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.mesos.gen.TwitterTaskInfo;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A container for multiple resource vectors.
 *
 * @author William Farner
 */
public class Resources {

  public static final String CPUS = "cpus";
  public static final String RAM_MB = "mem";
  public static final String PORTS = "ports";

  private final double numCpus;
  private final Amount<Long, Data> ram;
  private final int numPorts;

  @VisibleForTesting
  Resources(double numCpus, Amount<Long, Data> ram, int numPorts) {
    this.numCpus = numCpus;
    this.ram = checkNotNull(ram);
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
        && (ram.as(Data.MB) >= other.ram.as(Data.MB))
        && (numPorts >= other.numPorts);
  }

  /**
   * Gets the number of ports in the resources.
   *
   * @return Number of ports.
   */
  public int getNumPorts() {
    return numPorts;
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
        .append(numPorts, other.numPorts)
        .isEquals();
  }

  /**
   * Extracts the resources required from a task.
   *
   * @param task Task to get resources from.
   * @return The resources required by the task.
   */
  public static Resources from(TwitterTaskInfo task) {
    checkNotNull(task);
    return new Resources(
        task.getNumCpus(),
        Amount.of(task.getRamMb(), Data.MB),
        CommandLineExpander.getNumPortsRequested(task.getStartCommand()));
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
        getNumAvailablePorts(offer));
  }

  private static int getNumAvailablePorts(Offer offer) {
    int offeredPorts = 0;
    for (Range range : getPortRanges(offer)) {
      offeredPorts += (1 + range.getEnd() - range.getBegin());
    }
    return offeredPorts;
  }

  private static double getScalarValue(Offer offer, String key) {
    Resource resource = getResource(offer, key);
    if (resource == null) {
      return 0;
    }

    return resource.getScalar().getValue();
  }

  private static Resource getResource(Offer offer, String key) {
    return Iterables.find(offer.getResourcesList(), withName(key), null);
  }

  private static Predicate<Resource> withName(final String name) {
    return new Predicate<Resource>() {
      @Override public boolean apply(Resource resource) {
        return resource.getName().equals(name);
      }
    };
  }

  private static Iterable<Range> getPortRanges(Offer offer) {
    Resource resource = getResource(offer, Resources.PORTS);
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
  static Resource makeMesosResource(String name, double value) {
    return Resource.newBuilder().setName(name).setType(Type.SCALAR)
        .setScalar(Scalar.newBuilder().setValue(value)).build();
  }

  /**
   * Creates a mesos resource of integer ranges.
   *
   * @param name Name of the resource
   * @param values Values to translate into ranges.
   * @return A mesos ranges resource.
   */
  static Resource makeMesosRangeResource(String name, Set<Integer> values) {
    Ranges.Builder builder = Ranges.newBuilder();

    PeekingIterator<Integer> iterator =
        Iterators.peekingIterator(Sets.newTreeSet(values).iterator());

    // Build ranges until there are no numbers left.
    while (iterator.hasNext()) {
      // Start a new range.
      int start = iterator.next();
      int end = start;
      // Increment the end until the range is non-contiguous.
      while (iterator.hasNext() && (iterator.peek() == (end + 1))) {
        end++;
        iterator.next();
      }

      builder.addRange(Range.newBuilder().setBegin(start).setEnd(end));
    }

    return Resource.newBuilder().setName(name).setType(Type.RANGES).setRanges(builder).build();
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
        @Override public Set<Integer> apply(Range range) {
          return com.google.common.collect.Ranges.closed(
              (int) range.getBegin(), (int) range.getEnd()).asSet(DiscreteDomains.integers());
        }
      };

  /**
   * Attempts to grab {@code numPorts} from the given resource {@code offer}.
   *
   * @param offer The offer to grab ports from.
   * @param numPorts The number of ports to grab.
   * @return The set of ports grabbed.
   * @throws InsufficientResourcesException if not enough ports were available.
   */
  static Set<Integer> getPorts(Offer offer, int numPorts) throws InsufficientResourcesException {
    checkNotNull(offer);

    if (numPorts == 0) {
      return ImmutableSet.of();
    }

    List<Integer> availablePorts = Lists.newArrayList(Sets.newHashSet(
        Iterables.concat(Iterables.transform(getPortRanges(offer), RANGE_TO_MEMBERS))));

    if (availablePorts.size() < numPorts) {
      throw new InsufficientResourcesException(
          String.format("Could not get %d ports from %s", numPorts, offer));
    }

    Collections.shuffle(availablePorts);
    return ImmutableSet.copyOf(availablePorts.subList(0, numPorts));
  }
}

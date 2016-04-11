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

import java.util.List;
import java.util.Set;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.scheduler.TierInfo;
import org.apache.aurora.scheduler.base.Numbers;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;

import static java.util.Objects.requireNonNull;

/**
 * Allocate resources from an accepted Mesos Offer to TaskInfo and ExecutorInfo.
 */
public final class AcceptedOffer {

  public static final String DEFAULT_ROLE_NAME = "*";

  /**
   * Reserved resource filter.
   */
  public static final Predicate<Resource> RESERVED =
      e -> e.hasRole() && !e.getRole().equals(DEFAULT_ROLE_NAME);

  /**
   * Non reserved resource filter.
   */
  public static final Predicate<Resource> NOT_RESERVED = Predicates.not(RESERVED);

  /**
   * Helper function to check a resource value is small enough to be considered zero.
   */
  public static boolean nearZero(double value) {
    return Math.abs(value) < EPSILON;
  }

  /**
   * Get proper value for {@link org.apache.mesos.Protos.TaskInfo}'s resources.
   * @return A list of Resource used for TaskInfo.
   */
  public List<Resource> getTaskResources() {
    return taskResources;
  }

  /**
   * Get proper value for {@link org.apache.mesos.Protos.ExecutorInfo}'s resources.
   * @return A list of Resource used for ExecutorInfo.
   */
  public List<Resource> getExecutorResources() {
    return executorResources;
  }

  /**
   * Use this epsilon value to avoid comparison with zero.
   */
  private static final double EPSILON = 1e-6;

  private final List<Resource> taskResources;
  private final List<Resource> executorResources;

  public static AcceptedOffer create(
      Offer offer,
      ResourceSlot taskSlot,
      ResourceSlot executorSlot,
      Set<Integer> selectedPorts,
      TierInfo tierInfo) throws Resources.InsufficientResourcesException {

    List<Resource> reservedFirst = ImmutableList.<Resource>builder()
        .addAll(Iterables.filter(offer.getResourcesList(), RESERVED))
        .addAll(Iterables.filter(offer.getResourcesList(), NOT_RESERVED))
        .build();

    boolean revocable = tierInfo.isRevocable();
    List<Resource.Builder> cpuResources = filterToBuilders(
        reservedFirst,
        ResourceType.CPUS.getName(),
        revocable ? Resources.REVOCABLE : Resources.NON_REVOCABLE);
    List<Resource.Builder> memResources = filterToBuilderNonRevocable(
        reservedFirst, ResourceType.RAM_MB.getName());
    List<Resource.Builder> diskResources = filterToBuilderNonRevocable(
        reservedFirst, ResourceType.DISK_MB.getName());
    List<Resource.Builder> portsResources = filterToBuilderNonRevocable(
        reservedFirst, ResourceType.PORTS.getName());

    List<Resource> taskResources = ImmutableList.<Resource>builder()
        .addAll(allocateScalarType(cpuResources, taskSlot.getNumCpus(), revocable))
        .addAll(allocateScalarType(memResources, taskSlot.getRam().as(Data.MB), false))
        .addAll(allocateScalarType(diskResources, taskSlot.getDisk().as(Data.MB), false))
        .addAll(allocateRangeType(portsResources, selectedPorts))
        .build();

    List<Resource> executorResources = ImmutableList.<Resource>builder()
        .addAll(allocateScalarType(cpuResources, executorSlot.getNumCpus(), revocable))
        .addAll(allocateScalarType(memResources, executorSlot.getRam().as(Data.MB), false))
        .addAll(allocateScalarType(diskResources, executorSlot.getDisk().as(Data.MB), false))
        .build();

    return new AcceptedOffer(taskResources, executorResources);
  }

  private AcceptedOffer(
      List<Resource> taskResources,
      List<Resource> executorResources) {

    this.taskResources = requireNonNull(taskResources);
    this.executorResources = requireNonNull(executorResources);
  }

  private static List<Resource> allocateRangeType(
      List<Resource.Builder> from,
      Set<Integer> valueSet) throws Resources.InsufficientResourcesException {

    Set<Integer> leftOver = Sets.newHashSet(valueSet);
    ImmutableList.Builder<Resource> result = ImmutableList.<Resource>builder();
    for (Resource.Builder r : from) {
      Set<Integer> fromResource = Sets.newHashSet(Iterables.concat(
          Iterables.transform(r.getRanges().getRangeList(), Resources.RANGE_TO_MEMBERS)));
      Set<Integer> available = Sets.newHashSet(Sets.intersection(leftOver, fromResource));
      if (available.isEmpty()) {
        continue;
      }
      Resource newResource = makeMesosRangeResource(r.build(), available);
      result.add(newResource);
      leftOver.removeAll(available);
      if (leftOver.isEmpty()) {
        break;
      }
    }
    if (!leftOver.isEmpty()) {
      // NOTE: this will not happen as long as Veto logic from TaskAssigner.maybeAssign is
      // consistent.
      // Maybe we should consider implementing resource veto with this class to ensure that.
      throw new Resources.InsufficientResourcesException(
          "Insufficient resource for range type when allocating from offer");
    }
    return result.build();
  }

  /**
   * Creates a mesos resource of integer ranges from given prototype.
   *
   * @param prototype Resource prototype.
   * @param values    Values to translate into ranges.
   * @return A new mesos ranges resource.
   */
  static Resource makeMesosRangeResource(
      Resource prototype,
      Set<Integer> values) {

    return Protos.Resource.newBuilder(prototype)
        .setRanges(Protos.Value.Ranges.newBuilder()
            .addAllRange(
                Iterables.transform(Numbers.toRanges(values), ResourceSlot.RANGE_TRANSFORM)))
        .build();
  }

  private static List<Resource> allocateScalarType(
      List<Resource.Builder> from,
      double amount,
      boolean revocable) throws Resources.InsufficientResourcesException {

    double remaining = amount;
    ImmutableList.Builder<Resource> result = ImmutableList.builder();
    for (Resource.Builder r : from) {
      if (nearZero(remaining)) {
        break;
      }
      final double available = r.getScalar().getValue();
      if (nearZero(available)) {
        // Skip resource slot that is already used up.
        continue;
      }
      final double used = Math.min(remaining, available);
      remaining -= used;
      Resource.Builder newResource =
          Resource.newBuilder(r.build())
              .setScalar(Protos.Value.Scalar.newBuilder().setValue(used).build());
      if (revocable) {
        newResource.setRevocable(Resource.RevocableInfo.newBuilder());
      }
      result.add(newResource.build());
      r.getScalarBuilder().setValue(available - used);
    }
    if (!nearZero(remaining)) {
      // NOTE: this will not happen as long as Veto logic from TaskAssigner.maybeAssign is
      // consistent.
      // Maybe we should consider implementing resource veto with this class to ensure that.
      throw new Resources.InsufficientResourcesException(
          "Insufficient resource when allocating from offer");
    }
    return result.build();
  }

  private static List<Resource.Builder> filterToBuilders(
      List<Resource> resources,
      String name,
      Predicate<Resource> additionalFilter) {

    return FluentIterable.from(resources)
        .filter(e -> e.getName().equals(name))
        .filter(additionalFilter)
        .transform(Resource::toBuilder)
        .toList();
  }

  private static List<Resource.Builder> filterToBuilderNonRevocable(
      List<Resource> resources,
      String name) {

    return filterToBuilders(resources, name, Resources.NON_REVOCABLE);
  }
}

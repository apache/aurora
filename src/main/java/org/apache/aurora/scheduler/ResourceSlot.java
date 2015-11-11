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

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Range;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.scheduler.base.Numbers;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Resource.Builder;
import org.apache.mesos.Protos.TaskInfo;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.common.quantity.Data.BYTES;
import static org.apache.aurora.scheduler.ResourceType.CPUS;
import static org.apache.aurora.scheduler.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.ResourceType.PORTS;
import static org.apache.aurora.scheduler.ResourceType.RAM_MB;

/**
 * Represents a single task/host aggregate resource vector unaware of any Mesos resource traits.
 */
public final class ResourceSlot {

  private final double numCpus;
  private final Amount<Long, Data> disk;
  private final Amount<Long, Data> ram;
  private final int numPorts;

  /**
   * Empty ResourceSlot value.
   */
  public static final ResourceSlot NONE =
      new ResourceSlot(0, Amount.of(0L, Data.BITS), Amount.of(0L, Data.BITS), 0);

  public ResourceSlot(
      double numCpus,
      Amount<Long, Data> ram,
      Amount<Long, Data> disk,
      int numPorts) {

    this.numCpus = numCpus;
    this.ram = requireNonNull(ram);
    this.disk = requireNonNull(disk);
    this.numPorts = numPorts;
  }

  /**
   * Extracts the resources required from a task.
   *
   * @param task Task to get resources from.
   * @return The resources required by the task.
   */
  public static ResourceSlot from(ITaskConfig task) {
    requireNonNull(task);
    return new ResourceSlot(
        task.getNumCpus(),
        Amount.of(task.getRamMb(), Data.MB),
        Amount.of(task.getDiskMb(), Data.MB),
        task.getRequestedPorts().size());
  }

  /**
   * Adapts this slot object to a list of Mesos resources.
   *
   * @param selectedPorts The ports selected, to be applied as concrete task ranges.
   * @param tierInfo Task tier info.
   * @return Mesos resources.
   */
  public List<Protos.Resource> toResourceList(Set<Integer> selectedPorts, TierInfo tierInfo) {
    ImmutableList.Builder<Protos.Resource> resourceBuilder =
        ImmutableList.<Protos.Resource>builder()
            .add(makeMesosResource(CPUS, numCpus, tierInfo.isRevocable()))
            .add(makeMesosResource(DISK_MB, disk.as(Data.MB), false))
            .add(makeMesosResource(RAM_MB, ram.as(Data.MB), false));
    if (!selectedPorts.isEmpty()) {
      resourceBuilder.add(makeMesosRangeResource(PORTS, selectedPorts));
    }

    return resourceBuilder.build();
  }

  /**
   * Ensures that the revocable setting on the executor and task CPU resources match.
   *
   * @param task Task to check for resource type alignment.
   * @return A possibly-modified task, with aligned CPU resource types.
   */
  public static TaskInfo matchResourceTypes(TaskInfo task) {
    TaskInfo.Builder taskBuilder = task.toBuilder();

    Optional<Resource> revocableTaskCpu = taskBuilder.getResourcesList().stream()
        .filter(r -> r.getName().equals(CPUS.getName()))
        .filter(Resource::hasRevocable)
        .findFirst();
    ExecutorInfo.Builder executorBuilder = taskBuilder.getExecutorBuilder();

    Consumer<Builder> matchRevocable = new Consumer<Builder>() {
      @Override
      public void accept(Builder builder) {
        if (revocableTaskCpu.isPresent()) {
          builder.setRevocable(revocableTaskCpu.get().getRevocable());
        } else {
          builder.clearRevocable();
        }
      }
    };

    executorBuilder.getResourcesBuilderList().stream()
        .filter(r -> r.getName().equals(CPUS.getName()))
        .forEach(matchRevocable);

    return taskBuilder.build();
  }

  /**
   * Convenience method for adapting to Mesos resources without applying a port range.
   *
   * @see {@link #toResourceList(java.util.Set, TierInfo)}
   * @param tierInfo Task tier info.
   * @return Mesos resources.
   */
  public List<Protos.Resource> toResourceList(TierInfo tierInfo) {
    return toResourceList(ImmutableSet.of(), tierInfo);
  }

  /**
   * Creates a mesos resource of integer ranges.
   *
   * @param resourceType Resource type.
   * @param values Values to translate into ranges.
   * @return A mesos ranges resource.
   */
  @VisibleForTesting
  static Protos.Resource makeMesosRangeResource(
      ResourceType resourceType,
      Set<Integer> values) {

    return Protos.Resource.newBuilder()
        .setName(resourceType.getName())
        .setType(Protos.Value.Type.RANGES)
        .setRanges(Protos.Value.Ranges.newBuilder()
            .addAllRange(Iterables.transform(Numbers.toRanges(values), RANGE_TRANSFORM)))
        .build();
  }

  /**
   * Creates a scalar mesos resource.
   *
   * @param resourceType Resource type.
   * @param value Value for the resource.
   * @param revocable Flag indicating if this resource is revocable.
   * @return A mesos resource.
   */
  @VisibleForTesting
  static Protos.Resource makeMesosResource(
      ResourceType resourceType,
      double value,
      boolean revocable) {

    Protos.Resource.Builder builder = Protos.Resource.newBuilder()
        .setName(resourceType.getName())
        .setType(Protos.Value.Type.SCALAR)
        .setScalar(Protos.Value.Scalar.newBuilder().setValue(value));

    if (revocable) {
      builder.setRevocable(Protos.Resource.RevocableInfo.newBuilder());
    }

    return builder.build();
  }

  /**
   * Generates a ResourceSlot where each resource component is a max out of the two components.
   *
   * @param a A resource to compare.
   * @param b A resource to compare.
   *
   * @return Returns a ResourceSlot instance where each component is a max of the two components.
   */
  @VisibleForTesting
  static ResourceSlot maxElements(ResourceSlot a, ResourceSlot b) {
    double maxCPU = Math.max(a.getNumCpus(), b.getNumCpus());
    Amount<Long, Data> maxRAM = Amount.of(
        Math.max(a.getRam().as(Data.MB), b.getRam().as(Data.MB)),
        Data.MB);
    Amount<Long, Data> maxDisk = Amount.of(
        Math.max(a.getDisk().as(Data.MB), b.getDisk().as(Data.MB)),
        Data.MB);
    int maxPorts = Math.max(a.getNumPorts(), b.getNumPorts());

    return new ResourceSlot(maxCPU, maxRAM, maxDisk, maxPorts);
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

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ResourceSlot)) {
      return false;
    }

    ResourceSlot other = (ResourceSlot) o;
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
   * Sums up all resources in {@code slots}.
   *
   * @param slots Resource slots to sum up.
   * @return Sum of all resource slots.
   */
  public static ResourceSlot sum(Iterable<ResourceSlot> slots) {
    ResourceSlot sum = NONE;

    for (ResourceSlot r : slots) {
      sum = sum.add(r);
    }

    return sum;
  }

  /**
   * Adds {@code other}.
   *
   * @param other Resource slot to add.
   * @return Result.
   */
  public ResourceSlot add(ResourceSlot other) {
    return new ResourceSlot(
        getNumCpus() + other.getNumCpus(),
        Amount.of(getRam().as(BYTES) + other.getRam().as(BYTES), BYTES),
        Amount.of(getDisk().as(BYTES) + other.getDisk().as(BYTES), BYTES),
        getNumPorts() + other.getNumPorts());
  }

  /**
   * Subtracts {@code other}.
   *
   * @param other Resource slot to subtract.
   * @return Result.
   */
  public ResourceSlot subtract(ResourceSlot other) {
    return new ResourceSlot(
        getNumCpus() - other.getNumCpus(),
        Amount.of(getRam().as(BYTES) - other.getRam().as(BYTES), BYTES),
        Amount.of(getDisk().as(BYTES) - other.getDisk().as(BYTES), BYTES),
        getNumPorts() - other.getNumPorts());
  }

  /**
   * A Resources object is greater than another iff _all_ of its resource components are greater
   * or equal. A Resources object compares as equal if some but not all components are greater than
   * or equal to the other.
   */
  public static final Ordering<ResourceSlot> ORDER = new Ordering<ResourceSlot>() {
    @Override
    public int compare(ResourceSlot left, ResourceSlot right) {
      int diskC = left.getDisk().compareTo(right.getDisk());
      int ramC = left.getRam().compareTo(right.getRam());
      int portC = Integer.compare(left.getNumPorts(), right.getNumPorts());
      int cpuC = Double.compare(left.getNumCpus(), right.getNumCpus());

      List<Integer> vector = ImmutableList.of(diskC, ramC, portC, cpuC);

      if (vector.stream().allMatch(IS_ZERO))  {
        return 0;
      }

      if (vector.stream().filter(IS_ZERO.negate()).allMatch(e -> e > 0)) {
        return 1;
      }

      if (vector.stream().filter(IS_ZERO.negate()).allMatch(e -> e < 0)) {
        return -1;
      }

      return 0;
    }
  };

  private static final Predicate<Integer> IS_ZERO = e -> e == 0;

  private static final Function<Range<Integer>, Protos.Value.Range> RANGE_TRANSFORM =
      new Function<Range<Integer>, Protos.Value.Range>() {
        @Override
        public Protos.Value.Range apply(Range<Integer> input) {
          return Protos.Value.Range.newBuilder()
              .setBegin(input.lowerEndpoint())
              .setEnd(input.upperEndpoint())
              .build();
        }
      };
}

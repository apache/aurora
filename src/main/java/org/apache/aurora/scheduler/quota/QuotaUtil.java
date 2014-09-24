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
package org.apache.aurora.scheduler.quota;

import java.util.Set;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.scheduler.base.ResourceAggregates;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.entities.IInstanceTaskConfig;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateInstructions;
import org.apache.aurora.scheduler.storage.entities.IRange;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

/**
 * Static utility helpers for quota validation.
 */
public final class QuotaUtil {
  private QuotaUtil() {
    // Utility class.
  }

  /**
   * Converts a set of production {@link ITaskConfig} templates into a {@link IResourceAggregate}.
   * <p>
   * Discards any templates with {@code isProduction = False}.
   *
   * @param tasks Set of tasks to convert.
   * @return Aggregate resources consumed by {@code tasks}.
   */
  public static IResourceAggregate prodResourcesFromTasks(Iterable<ITaskConfig> tasks) {
    return fromTasks(FluentIterable.from(tasks).filter(Tasks.IS_PRODUCTION));
  }

  /**
   * Converts a {@link IJobUpdate} into a {@link IResourceAggregate}.
   * <p>
   * This function calculates max aggregate production resources consumed by the
   * {@code jobUpdate}. The max is calculated between existing and desired task configs on per
   * resource basis. This means max CPU, RAM and DISK values are computed individually and may
   * come from different task configurations. While it may not be the most accurate representation
   * of job update resources during the update, it does guarantee none of the individual resource
   * values is exceeded during the forward/back roll.
   *
   * @param jobUpdate Job update to convert.
   * @return Max production aggregate resources represented by the {@code jobUpdate}.
   */
  public static IResourceAggregate prodResourcesFromJobUpdate(IJobUpdate jobUpdate) {
    IJobUpdateInstructions instructions = jobUpdate.getInstructions();

    // Calculate existing prod task consumption.
    double existingCpu = 0;
    int existingRamMb = 0;
    int existingDiskMb = 0;
    for (IInstanceTaskConfig group : instructions.getInitialState()) {
      ITaskConfig task = group.getTask();
      if (task.isProduction()) {
        for (IRange range : group.getInstances()) {
          int numInstances = range.getLast() - range.getFirst() + 1;
          existingCpu += task.getNumCpus() * numInstances;
          existingRamMb += task.getRamMb() * numInstances;
          existingDiskMb += task.getDiskMb() * numInstances;
        }
      }
    }

    // Calculate desired prod task consumption.
    ITaskConfig desiredConfig = instructions.getDesiredState().getTask();
    IResourceAggregate desired = desiredConfig.isProduction()
        ? ResourceAggregates.scale(
        prodResourcesFromTasks(ImmutableSet.of(desiredConfig)),
        getUpdateInstanceCount(instructions.getDesiredState().getInstances()))
        : ResourceAggregates.EMPTY;

    // Calculate result as max(existing, desired) per resource.
    return IResourceAggregate.build(new ResourceAggregate()
        .setNumCpus(Math.max(existingCpu, desired.getNumCpus()))
        .setRamMb(Math.max(existingRamMb, desired.getRamMb()))
        .setDiskMb(Math.max(existingDiskMb, desired.getDiskMb())));
  }

  /**
   * Converts a set of {@link ITaskConfig} templates into a {@link IResourceAggregate}.
   * <p>
   * @param tasks Set of tasks to convert.
   * @return Aggregate resources consumed by {@code tasks}.
   */
  public static IResourceAggregate fromTasks(Iterable<ITaskConfig> tasks) {
    double cpu = 0;
    int ramMb = 0;
    int diskMb = 0;
    for (ITaskConfig task : tasks) {
      cpu += task.getNumCpus();
      ramMb += task.getRamMb();
      diskMb += task.getDiskMb();
    }

    return IResourceAggregate.build(new ResourceAggregate()
        .setNumCpus(cpu)
        .setRamMb(ramMb)
        .setDiskMb(diskMb));
  }

  private static int getUpdateInstanceCount(Set<IRange> ranges) {
    int instanceCount = 0;
    for (IRange range : ranges) {
      instanceCount += range.getLast() - range.getFirst() + 1;
    }

    return instanceCount;
  }
}

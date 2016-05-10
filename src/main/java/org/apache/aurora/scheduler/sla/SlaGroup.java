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
package org.apache.aurora.scheduler.sla;

import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Range;

import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

import static org.apache.aurora.scheduler.resources.ResourceBag.EMPTY;
import static org.apache.aurora.scheduler.resources.ResourceBag.LARGE;
import static org.apache.aurora.scheduler.resources.ResourceBag.MEDIUM;
import static org.apache.aurora.scheduler.resources.ResourceBag.SMALL;
import static org.apache.aurora.scheduler.resources.ResourceBag.XLARGE;
import static org.apache.aurora.scheduler.resources.ResourceManager.getTaskResources;
import static org.apache.aurora.scheduler.resources.ResourceManager.quantityOf;
import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;

/**
 * Defines a logical grouping criteria to be applied over a set of tasks.
 */
interface SlaGroup {

  /**
   * Generates named groups based on the set of provided tasks.
   *
   * @param tasks Set of tasks to generate named groups for.
   * @return Multimap of group names and relevant tasks.
   */
  Multimap<String, IScheduledTask> createNamedGroups(Iterable<IScheduledTask> tasks);

  /**
   * Pre-configured SLA groupings.
   */
  enum GroupType {
    JOB(new Job()),
    CLUSTER(new Cluster()),
    RESOURCE_CPU(new Resource<>(
        ImmutableMap.of(
            "sla_cpu_small_", Range.closed(fromBag(EMPTY, CPUS), fromBag(SMALL, CPUS)),
            "sla_cpu_medium_", Range.openClosed(fromBag(SMALL, CPUS), fromBag(MEDIUM, CPUS)),
            "sla_cpu_large_", Range.openClosed(fromBag(MEDIUM, CPUS), fromBag(LARGE, CPUS)),
            "sla_cpu_xlarge_", Range.openClosed(fromBag(LARGE, CPUS), fromBag(XLARGE, CPUS)),
            "sla_cpu_xxlarge_", Range.greaterThan(fromBag(XLARGE, CPUS))),
        task -> quantityOf(getTaskResources(task.getAssignedTask().getTask(), CPUS))
    )),
    RESOURCE_RAM(new Resource<>(
        ImmutableMap.of(
            "sla_ram_small_", Range.closed(fromBag(EMPTY, RAM_MB), fromBag(SMALL, RAM_MB)),
            "sla_ram_medium_", Range.openClosed(fromBag(SMALL, RAM_MB), fromBag(MEDIUM, RAM_MB)),
            "sla_ram_large_", Range.openClosed(fromBag(MEDIUM, RAM_MB), fromBag(LARGE, RAM_MB)),
            "sla_ram_xlarge_", Range.openClosed(fromBag(LARGE, RAM_MB), fromBag(XLARGE, RAM_MB)),
            "sla_ram_xxlarge_", Range.greaterThan(fromBag(XLARGE, RAM_MB))),
        task -> quantityOf(getTaskResources(task.getAssignedTask().getTask(), RAM_MB))
    )),
    RESOURCE_DISK(new Resource<>(
        ImmutableMap.of(
            "sla_disk_small_", Range.closed(fromBag(EMPTY, DISK_MB), fromBag(SMALL, DISK_MB)),
            "sla_disk_medium_", Range.openClosed(fromBag(SMALL, DISK_MB), fromBag(MEDIUM, DISK_MB)),
            "sla_disk_large_", Range.openClosed(fromBag(MEDIUM, DISK_MB), fromBag(LARGE, DISK_MB)),
            "sla_disk_xlarge_", Range.openClosed(fromBag(LARGE, DISK_MB), fromBag(XLARGE, DISK_MB)),
            "sla_disk_xxlarge_", Range.greaterThan(fromBag(XLARGE, DISK_MB))),
        task -> quantityOf(getTaskResources(task.getAssignedTask().getTask(), DISK_MB))
    ));

    private SlaGroup group;
    GroupType(SlaGroup group) {
      this.group = group;
    }

    SlaGroup getSlaGroup() {
      return group;
    }

    // TODO(maxim): Refactor SLA management to build groups dynamically from
    // all available ResourceType values.
    private static Double fromBag(ResourceBag bag, ResourceType type) {
      return bag.valueOf(type);
    }
  }

  /**
   * Groups tasks by job.
   */
  class Job implements SlaGroup {
    @Override
    public Multimap<String, IScheduledTask> createNamedGroups(Iterable<IScheduledTask> tasks) {
      return Multimaps.index(tasks, Functions.compose(new Function<IJobKey, String>() {
        @Override
        public String apply(IJobKey jobKey) {
          return "sla_" + JobKeys.canonicalString(jobKey) + "_";
        }
      }, Tasks::getJob));
    }
  }

  /**
   * Groups all tasks available in the cluster.
   */
  class Cluster implements SlaGroup {
    @Override
    public Multimap<String, IScheduledTask> createNamedGroups(Iterable<IScheduledTask> tasks) {
      return Multimaps.index(tasks, task -> "sla_cluster_");
    }
  }

  /**
   * Groups all tasks by their specified resource value.
   *
   * @param <T> Type of resource to group by.
   */
  final class Resource<T extends Number & Comparable<T>> implements SlaGroup {

    private final Map<String, Range<T>> map;
    private final Function<IScheduledTask, T> function;

    private Resource(Map<String, Range<T>> map, Function<IScheduledTask, T> function) {
      this.map = map;
      this.function = function;
    }

    @Override
    public Multimap<String, IScheduledTask> createNamedGroups(Iterable<IScheduledTask> tasks) {
      ImmutableListMultimap.Builder<String, IScheduledTask> result =
          ImmutableListMultimap.builder();

      for (final Map.Entry<String, Range<T>> entry : map.entrySet()) {
        result.putAll(entry.getKey(), Iterables.filter(tasks, task -> {
          return entry.getValue().contains(function.apply(task));
        }));
      }
      return result.build();
    }
  }
}

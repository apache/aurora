/**
 * Copyright 2014 Apache Software Foundation
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
package org.apache.aurora.scheduler.sla;

import java.util.Map;

import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Range;
import com.twitter.common.base.Function;

import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

import static org.apache.aurora.scheduler.base.ResourceAggregates.EMPTY;
import static org.apache.aurora.scheduler.base.ResourceAggregates.LARGE;
import static org.apache.aurora.scheduler.base.ResourceAggregates.MEDIUM;
import static org.apache.aurora.scheduler.base.ResourceAggregates.SMALL;
import static org.apache.aurora.scheduler.base.ResourceAggregates.XLARGE;

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
            "sla_cpu_small_", Range.closed(EMPTY.getNumCpus(), SMALL.getNumCpus()),
            "sla_cpu_medium_", Range.openClosed(SMALL.getNumCpus(), MEDIUM.getNumCpus()),
            "sla_cpu_large_", Range.openClosed(MEDIUM.getNumCpus(), LARGE.getNumCpus()),
            "sla_cpu_xlarge_", Range.openClosed(LARGE.getNumCpus(), XLARGE.getNumCpus()),
            "sla_cpu_xxlarge_", Range.greaterThan(XLARGE.getNumCpus())),
        new Function<IScheduledTask, Double>() {
          @Override
          public Double apply(IScheduledTask task) {
            return task.getAssignedTask().getTask().getNumCpus();
          }
        }
    )),
    RESOURCE_RAM(new Resource<>(
        ImmutableMap.of(
            "sla_ram_small_", Range.closed(EMPTY.getRamMb(), SMALL.getRamMb()),
            "sla_ram_medium_", Range.openClosed(SMALL.getRamMb(), MEDIUM.getRamMb()),
            "sla_ram_large_", Range.openClosed(MEDIUM.getRamMb(), LARGE.getRamMb()),
            "sla_ram_xlarge_", Range.openClosed(LARGE.getRamMb(), XLARGE.getRamMb()),
            "sla_ram_xxlarge_", Range.greaterThan(XLARGE.getRamMb())),
        new Function<IScheduledTask, Long>() {
          @Override
          public Long apply(IScheduledTask task) {
            return task.getAssignedTask().getTask().getRamMb();
          }
        }
    )),
    RESOURCE_DISK(new Resource<>(
        ImmutableMap.of(
            "sla_disk_small_", Range.closed(EMPTY.getDiskMb(), SMALL.getDiskMb()),
            "sla_disk_medium_", Range.openClosed(SMALL.getDiskMb(), MEDIUM.getDiskMb()),
            "sla_disk_large_", Range.openClosed(MEDIUM.getDiskMb(), LARGE.getDiskMb()),
            "sla_disk_xlarge_", Range.openClosed(LARGE.getDiskMb(), XLARGE.getDiskMb()),
            "sla_disk_xxlarge_", Range.greaterThan(XLARGE.getDiskMb())),
        new Function<IScheduledTask, Long>() {
          @Override
          public Long apply(IScheduledTask task) {
            return task.getAssignedTask().getTask().getDiskMb();
          }
        }
    ));

    private SlaGroup group;
    GroupType(SlaGroup group) {
      this.group = group;
    }

    SlaGroup getSlaGroup() {
      return group;
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
          return "sla_" + JobKeys.toPath(jobKey) + "_";
        }
      }, Tasks.SCHEDULED_TO_JOB_KEY));
    }
  }

  /**
   * Groups all tasks available in the cluster.
   */
  class Cluster implements SlaGroup {
    @Override
    public Multimap<String, IScheduledTask> createNamedGroups(Iterable<IScheduledTask> tasks) {
      return Multimaps.index(tasks, new Function<IScheduledTask, String>() {
        @Override
        public String apply(IScheduledTask task) {
          return "sla_cluster_";
        }
      });
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
        result.putAll(entry.getKey(), Iterables.filter(tasks, new Predicate<IScheduledTask>() {
          @Override
          public boolean apply(IScheduledTask task) {
            return entry.getValue().contains(function.apply(task));
          }
        }));
      }
      return result.build();
    }
  }
}

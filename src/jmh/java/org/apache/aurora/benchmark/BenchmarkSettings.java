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
package org.apache.aurora.benchmark;

import java.util.Set;

import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

import static java.util.Objects.requireNonNull;

/**
 * Benchmark test settings.
 */
final class BenchmarkSettings {
  private final Set<IHostAttributes> hostAttributes;
  private final double siblingClusterUtilization;
  private final double victimClusterUtilization;
  private final boolean allVictimsEligibleForPreemption;
  private final Set<IScheduledTask> tasks;

  private BenchmarkSettings(
      double siblingClusterUtilization,
      double victimClusterUtilization,
      boolean allVictimsEligibleForPreemption,
      Set<IHostAttributes> hostAttributes,
      Set<IScheduledTask> tasks) {

    this.siblingClusterUtilization = siblingClusterUtilization;
    this.victimClusterUtilization = victimClusterUtilization;
    this.allVictimsEligibleForPreemption = allVictimsEligibleForPreemption;
    this.hostAttributes = requireNonNull(hostAttributes);
    this.tasks = requireNonNull(tasks);
  }

  /**
   * Gets the cluster utilization factor specifying what percentage of hosts in the cluster
   * already have a job instance assigned to them as used during the scheduling benchmark.
   *
   * @return Cluster utilization (default: 0.25).
   */
  double getSiblingClusterUtilization() {
    return siblingClusterUtilization;
  }

  /**
   * Gets the cluster utilization factor specifying what percentage of hosts in the cluster
   * have a task of a foreign job assigned to them, i.e. potential preemption victims.
   *
   * @return Cluster utilization (default: 0.25).
   */
  double getVictimClusterUtilization() {
    return victimClusterUtilization;
  }

  /**
   * Flag indicating whether all existing assigned tasks are available for preemption.
   *
   * @return Victim preemption eligibility (default: false).
   */
  boolean areAllVictimsEligibleForPreemption() {
    return allVictimsEligibleForPreemption;
  }

  /**
   * Gets a set of cluster host attributes.
   *
   * @return Set of {@link IHostAttributes}.
   */
  Set<IHostAttributes> getHostAttributes() {
    return hostAttributes;
  }

  /**
   * Gets a benchmark task.
   *
   * @return Task to run a benchmark for.
   */
  Set<IScheduledTask> getTasks() {
    return tasks;
  }

  static class Builder {
    private double siblingClusterUtilization = 0.25;
    private double victimClusterUtilization = 0.25;
    private boolean allVictimsEligibleForPreemption;
    private Set<IHostAttributes> hostAttributes;
    private Set<IScheduledTask> tasks;

    Builder setSiblingClusterUtilization(double newClusterUtilization) {
      siblingClusterUtilization = newClusterUtilization;
      return this;
    }

    Builder setVictimClusterUtilization(double newClusterUtilization) {
      victimClusterUtilization = newClusterUtilization;
      return this;
    }

    Builder setVictimPreemptionEligibilty(boolean newPreemptionEligibility) {
      allVictimsEligibleForPreemption = newPreemptionEligibility;
      return this;
    }

    Builder setHostAttributes(Set<IHostAttributes> newHostAttributes) {
      hostAttributes = newHostAttributes;
      return this;
    }

    Builder setTasks(Set<IScheduledTask> newTasks) {
      tasks = newTasks;
      return this;
    }

    BenchmarkSettings build() {
      return new BenchmarkSettings(
          siblingClusterUtilization,
          victimClusterUtilization,
          allVictimsEligibleForPreemption,
          hostAttributes,
          tasks);
    }
  }
}

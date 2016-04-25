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
package org.apache.aurora.scheduler.storage.log;

import java.util.Set;

import org.apache.aurora.GuavaUtils;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.Resource;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IResource;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;

/**
 * Helps migrating thrift schema by populating deprecated and/or replacement fields.
 */
public final class ThriftBackfill {
  private ThriftBackfill() {
    // Utility class.
  }

  private static Resource getResource(TaskConfig config, ResourceType type) {
    return config.getResources().stream()
        .filter(e -> ResourceType.fromResource(IResource.build(e)).equals(type))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Missing resource definition for " + type));
  }

  /**
   * Ensures TaskConfig.resources and correspondent task-level fields are all populated.
   *
   * @param config TaskConfig to backfill.
   * @return Backfilled TaskConfig.
   */
  public static TaskConfig backfillTask(TaskConfig config) {
    if (!config.isSetResources() || config.getResources().isEmpty()) {
      config.addToResources(Resource.numCpus(config.getNumCpus()));
      config.addToResources(Resource.ramMb(config.getRamMb()));
      config.addToResources(Resource.diskMb(config.getDiskMb()));
      if (config.isSetRequestedPorts()) {
        for (String port : config.getRequestedPorts()) {
          config.addToResources(Resource.namedPort(port));
        }
      }
    } else {
      config.setNumCpus(getResource(config, CPUS).getNumCpus());
      config.setRamMb(getResource(config, RAM_MB).getRamMb());
      config.setDiskMb(getResource(config, DISK_MB).getDiskMb());
      Set<String> ports = config.getResources().stream()
          .filter(e -> ResourceType.fromResource(IResource.build(e)).equals(PORTS))
          .map(Resource::getNamedPort)
          .collect(GuavaUtils.toImmutableSet());
      if (!ports.isEmpty()) {
        config.setRequestedPorts(ports);
      }
    }
    return config;
  }

  /**
   * Backfills JobConfiguration. See {@link #backfillTask(TaskConfig)}.
   *
   * @param jobConfig JobConfiguration to backfill.
   * @return Backfilled JobConfiguration.
   */
  public static IJobConfiguration backfillJobConfiguration(JobConfiguration jobConfig) {
    backfillTask(jobConfig.getTaskConfig());
    return IJobConfiguration.build(jobConfig);
  }

  /**
   * Backfills set of tasks. See {@link #backfillTask(TaskConfig)}.
   *
   * @param tasks Set of tasks to backfill.
   * @return Backfilled set of tasks.
   */
  public static Set<IScheduledTask> backfillTasks(Set<ScheduledTask> tasks) {
    return tasks.stream()
        .map(ThriftBackfill::backfillScheduledTask)
        .map(IScheduledTask::build)
        .collect(GuavaUtils.toImmutableSet());
  }

  private static ScheduledTask backfillScheduledTask(ScheduledTask task) {
    backfillTask(task.getAssignedTask().getTask());
    return task;
  }

  /**
   * Backfills JobUpdate. See {@link #backfillTask(TaskConfig)}.
   *
   * @param update JobUpdate to backfill.
   * @return Backfilled job update.
   */
  static IJobUpdate backFillJobUpdate(JobUpdate update) {
    JobUpdateInstructions instructions = update.getInstructions();
    if (instructions.isSetDesiredState()) {
      backfillTask(instructions.getDesiredState().getTask());
    }

    instructions.getInitialState().forEach(e -> backfillTask(e.getTask()));

    return IJobUpdate.build(update);
  }
}

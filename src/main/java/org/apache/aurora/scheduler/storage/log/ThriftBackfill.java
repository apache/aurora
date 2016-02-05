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
import java.util.stream.Collectors;

import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

/**
 * Populates deprecated fields to ensure backwards compatibility between scheduler releases.
 * See AURORA-1603 for more details.
 */
final class ThriftBackfill {

  private ThriftBackfill() {
    // Utility class.
  }

  static Set<IScheduledTask> backFillScheduledTasks(Set<ScheduledTask> tasks) {
    tasks.stream().forEach(t -> backFillTaskConfig(t.getAssignedTask().getTask()));
    return tasks.stream().map(t -> IScheduledTask.build(t)).collect(Collectors.toSet());
  }

  static IJobConfiguration backFillJobConfiguration(JobConfiguration jobConfiguration) {
    backFillTaskConfig(jobConfiguration.getTaskConfig());
    jobConfiguration.getOwner().setRole(jobConfiguration.getKey().getRole());
    return IJobConfiguration.build(jobConfiguration);
  }

  static IJobUpdate backFillJobUpdate(JobUpdate update) {
    JobUpdateInstructions instructions = update.getInstructions();
    if (instructions.isSetDesiredState()) {
      backFillTaskConfig(instructions.getDesiredState().getTask());
    }

    for (InstanceTaskConfig instanceConfig : instructions.getInitialState()) {
      backFillTaskConfig(instanceConfig.getTask());
    }

    return IJobUpdate.build(update);
  }

  private static TaskConfig backFillTaskConfig(TaskConfig task) {
    task.setJobName(task.getJob().getName()).setEnvironment(task.getJob().getEnvironment());
    if (task.isSetOwner()) {
      task.getOwner().setRole(task.getJob().getRole());
    }

    return task;
  }
}

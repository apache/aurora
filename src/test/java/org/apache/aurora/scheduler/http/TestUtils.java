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
package org.apache.aurora.scheduler.http;

import com.google.common.annotations.VisibleForTesting;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

public final class TestUtils {

  private TestUtils() { }

  /**
   * Make task and set status to the given {@link ScheduleStatus}.
   *
   * @param jobKey The task group key.
   * @param id The task id.
   * @param instanceId The id of the instance of the task.
   * @param taskStatus The status of the task.
   * @param numCPUs The number of CPUs required for the task.
   * @param ramMB The amount of RAM (in MegaBytes) required for the task.
   * @param diskMB The amount of disk space (in MegaBytes) required for the task.
   * @return Task.
   */
  @VisibleForTesting
  public static IScheduledTask makeTask(
      IJobKey jobKey, String id, int instanceId,
      ScheduleStatus taskStatus, double numCPUs, long ramMB, long diskMB) {
    return IScheduledTask.build(new ScheduledTask()
        .setStatus(taskStatus)
        .setAssignedTask(new AssignedTask()
            .setInstanceId(instanceId)
            .setTaskId(id)
            .setTask(new TaskConfig()
                .setJob(jobKey.newBuilder())
                .setNumCpus(numCPUs)
                .setRamMb(ramMB)
                .setDiskMb(diskMB))));
  }
}

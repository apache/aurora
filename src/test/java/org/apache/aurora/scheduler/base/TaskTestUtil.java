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
package org.apache.aurora.scheduler.base;

import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

/**
 * Convenience methods for working with tasks.
 */
public final class TaskTestUtil {

  private TaskTestUtil() {
    // Utility class.
  }

  public static IScheduledTask makeTask(ScheduleStatus status, List<TaskEvent> taskEvents) {
    return IScheduledTask.build(new ScheduledTask()
        .setStatus(status)
        .setAssignedTask(new AssignedTask()
            .setTaskId("task_id")
            .setSlaveHost("host1")
            .setTask(new TaskConfig()
                .setJobName("job_a")
                .setOwner(new Identity("role_a", "role_a" + "-user"))))
        .setTaskEvents(taskEvents));
  }

  public static List<TaskEvent> makeTaskEvents(
      long startTs,
      ScheduleStatus status,
      ScheduleStatus... statuses) {

    ImmutableList.Builder<TaskEvent> taskEvents = ImmutableList.builder();
    taskEvents.add(makeTaskEvent(startTs, status));
    long increment = startTs;
    for (ScheduleStatus st : statuses) {
      taskEvents.add(makeTaskEvent(startTs + increment++, st));
    }
    return taskEvents.build();
  }

  private static TaskEvent makeTaskEvent(long ts, ScheduleStatus status) {
    return new TaskEvent().setTimestamp(ts).setStatus(status);
  }
}

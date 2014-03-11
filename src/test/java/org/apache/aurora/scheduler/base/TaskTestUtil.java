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
package org.apache.aurora.scheduler.base;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

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

  public static IScheduledTask makeTask(ScheduleStatus status, long startTime) {
    return makeTask(status, makeTaskEvents(startTime, 3));
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

  private static List<TaskEvent> makeTaskEvents(long startTs, int count) {
    List<TaskEvent> taskEvents = Lists.newArrayListWithCapacity(3);
    for (int i = 0; i < count; i++) {
      taskEvents.add(makeTaskEvent(startTs - (i * 10)));
    }
    Collections.reverse(taskEvents);
    return taskEvents;
  }

  private static TaskEvent makeTaskEvent(long ts) {
    return new TaskEvent().setTimestamp(ts);
  }
}

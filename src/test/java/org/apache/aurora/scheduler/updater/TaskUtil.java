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
package org.apache.aurora.scheduler.updater;

import java.util.List;
import java.util.Objects;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static org.apache.aurora.gen.ScheduleStatus.ASSIGNED;
import static org.apache.aurora.gen.ScheduleStatus.KILLING;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;

final class TaskUtil {

  private final FakeClock clock;

  TaskUtil(FakeClock clock) {
    this.clock = Objects.requireNonNull(clock);
  }

  IScheduledTask makeTask(ITaskConfig config, ScheduleStatus status) {
    List<TaskEvent> events = Lists.newArrayList();
    if (status != PENDING) {
      events.add(new TaskEvent().setTimestamp(clock.nowMillis()).setStatus(PENDING));
    }
    if (Tasks.isTerminated(status) || status == KILLING) {
      events.add(new TaskEvent().setTimestamp(clock.nowMillis()).setStatus(ASSIGNED));
      events.add(new TaskEvent().setTimestamp(clock.nowMillis()).setStatus(RUNNING));
    }

    events.add(new TaskEvent().setTimestamp(clock.nowMillis()).setStatus(status));

    return IScheduledTask.build(
        new ScheduledTask()
            .setStatus(status)
            .setTaskEvents(ImmutableList.copyOf(events))
            .setAssignedTask(
                new AssignedTask()
                    .setTask(config.newBuilder())));
  }
}

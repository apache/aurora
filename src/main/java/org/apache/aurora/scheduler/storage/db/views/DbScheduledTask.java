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
package org.apache.aurora.scheduler.storage.db.views;

import java.util.List;

import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

public final class DbScheduledTask {
  private DbAssignedTask assignedTask;
  private ScheduleStatus status;
  private int failureCount;
  private List<TaskEvent> taskEvents;
  private String ancestorId;

  private DbScheduledTask() {
  }

  public IScheduledTask toImmutable() {
    return IScheduledTask.build(
        new ScheduledTask()
            .setAssignedTask(assignedTask.toThrift())
            .setStatus(status)
            .setFailureCount(failureCount)
            // Must be sorting a copy because taskEvents is populated by MyBatis and it might
            // reuse the same instance.
            .setTaskEvents(BY_TIMESTAMP.immutableSortedCopy(taskEvents))
            .setAncestorId(ancestorId));
  }

  private static final Ordering<TaskEvent> BY_TIMESTAMP = new Ordering<TaskEvent>() {
    @Override
    public int compare(TaskEvent left, TaskEvent right) {
      return Longs.compare(left.getTimestamp(), right.getTimestamp());
    }
  };
}

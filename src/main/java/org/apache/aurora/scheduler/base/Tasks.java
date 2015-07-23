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

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.apiConstants;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.entities.ITaskEvent;

/**
 * Utility class providing convenience functions relating to tasks.
 */
public final class Tasks {

  @VisibleForTesting
  static final List<ScheduleStatus> ORDERED_TASK_STATUSES = ImmutableList.<ScheduleStatus>builder()
          .addAll(apiConstants.TERMINAL_STATES)
          .addAll(apiConstants.ACTIVE_STATES)
          .build();

  public static ITaskConfig getConfig(IScheduledTask scheduledTask) {
    return scheduledTask.getAssignedTask().getTask();
  }

  public static int getInstanceId(IScheduledTask scheduledTask) {
    return scheduledTask.getAssignedTask().getInstanceId();
  }

  public static IJobKey getJob(IAssignedTask assignedTask) {
    return assignedTask.getTask().getJob();
  }

  public static IJobKey getJob(IScheduledTask scheduledTask) {
    return getJob(scheduledTask.getAssignedTask());
  }

  public static String scheduledToSlaveHost(IScheduledTask scheduledTask) {
    return scheduledTask.getAssignedTask().getSlaveHost();
  }

  /**
   * Different states that an active task may be in.
   */
  public static final EnumSet<ScheduleStatus> ACTIVE_STATES =
      EnumSet.copyOf(apiConstants.ACTIVE_STATES);

  /**
   * Terminal states, which a task should not move from.
   */
  public static final Set<ScheduleStatus> TERMINAL_STATES =
      EnumSet.copyOf(apiConstants.TERMINAL_STATES);

  /**
   * Tasks a state can be in when associated with a slave machine.
   */
  public static final Set<ScheduleStatus> SLAVE_ASSIGNED_STATES =
      EnumSet.copyOf(apiConstants.SLAVE_ASSIGNED_STATES);

  private Tasks() {
    // Utility class.
  }

  /**
   * A utility method that returns a multi-map of tasks keyed by IJobKey.
   *
   * @param tasks A list of tasks to be keyed by map
   * @return A multi-map of tasks keyed by job key.
   */
  public static Multimap<IJobKey, IScheduledTask> byJobKey(Iterable<IScheduledTask> tasks) {
    return Multimaps.index(tasks, Tasks::getJob);
  }

  public static boolean isActive(ScheduleStatus status) {
    return ACTIVE_STATES.contains(status);
  }

  public static boolean isTerminated(ScheduleStatus status) {
    return TERMINAL_STATES.contains(status);
  }

  public static String id(IScheduledTask task) {
    return task.getAssignedTask().getTaskId();
  }

  // TODO(William Farner: Remove this once the code base is switched to IScheduledTask.
  public static String id(ScheduledTask task) {
    return task.getAssignedTask().getTaskId();
  }

  public static Set<String> ids(Iterable<IScheduledTask> tasks) {
    return ImmutableSet.copyOf(Iterables.transform(tasks, Tasks::id));
  }

  public static Set<String> ids(IScheduledTask... tasks) {
    return ids(ImmutableList.copyOf(tasks));
  }

  public static Map<String, IScheduledTask> mapById(Iterable<IScheduledTask> tasks) {
    return Maps.uniqueIndex(tasks, Tasks::id);
  }

  /**
   * Get the latest active task or the latest inactive task if no active task exists.
   *
   * @param tasks a collection of tasks
   * @return the task that transitioned most recently.
   */
  public static IScheduledTask getLatestActiveTask(Iterable<IScheduledTask> tasks) {
    Preconditions.checkArgument(Iterables.size(tasks) != 0);

    return Ordering.explicit(ORDERED_TASK_STATUSES)
        .onResultOf(IScheduledTask::getStatus)
        .compound(LATEST_ACTIVITY)
        .max(tasks);
  }

  public static ITaskEvent getLatestEvent(IScheduledTask task) {
    return Iterables.getLast(task.getTaskEvents());
  }

  public static final Ordering<IScheduledTask> LATEST_ACTIVITY = Ordering.natural()
      .onResultOf(new Function<IScheduledTask, Long>() {
        @Override
        public Long apply(IScheduledTask task) {
          return getLatestEvent(task).getTimestamp();
        }
      });
}

/*
 * Copyright 2013 Twitter, Inc.
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
package com.twitter.aurora.scheduler.base;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;

import com.twitter.aurora.gen.Constants;
import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.scheduler.storage.entities.IAssignedTask;
import com.twitter.aurora.scheduler.storage.entities.IJobKey;
import com.twitter.aurora.scheduler.storage.entities.IScheduledTask;
import com.twitter.aurora.scheduler.storage.entities.ITaskConfig;

/**
 * Utility class providing convenience functions relating to tasks.
 */
public final class Tasks {

  public static final Function<IScheduledTask, IAssignedTask> SCHEDULED_TO_ASSIGNED =
      new Function<IScheduledTask, IAssignedTask>() {
        @Override public IAssignedTask apply(IScheduledTask task) {
          return task.getAssignedTask();
        }
      };

  public static final Function<IAssignedTask, ITaskConfig> ASSIGNED_TO_INFO =
      new Function<IAssignedTask, ITaskConfig>() {
        @Override public ITaskConfig apply(IAssignedTask task) {
          return task.getTask();
        }
      };

  public static final Function<IScheduledTask, ITaskConfig> SCHEDULED_TO_INFO =
      Functions.compose(ASSIGNED_TO_INFO, SCHEDULED_TO_ASSIGNED);

  public static final Function<IAssignedTask, String> ASSIGNED_TO_ID =
      new Function<IAssignedTask, String>() {
        @Override public String apply(IAssignedTask task) {
          return task.getTaskId();
        }
      };

  public static final Function<IScheduledTask, String> SCHEDULED_TO_ID =
      Functions.compose(ASSIGNED_TO_ID, SCHEDULED_TO_ASSIGNED);

  public static final Function<ITaskConfig, Integer> INFO_TO_SHARD_ID =
      new Function<ITaskConfig, Integer>() {
        @Override public Integer apply(ITaskConfig task) {
          return task.getInstanceId();
        }
      };

  public static final Function<IScheduledTask, Integer> SCHEDULED_TO_SHARD_ID =
      Functions.compose(INFO_TO_SHARD_ID, SCHEDULED_TO_INFO);

  public static final Function<ITaskConfig, IJobKey> INFO_TO_JOB_KEY =
      new Function<ITaskConfig, IJobKey>() {
        @Override public IJobKey apply(ITaskConfig task) {
          return JobKeys.from(task.getOwner().getRole(), task.getEnvironment(), task.getJobName());
        }
      };

  public static final Function<IAssignedTask, IJobKey> ASSIGNED_TO_JOB_KEY =
      Functions.compose(INFO_TO_JOB_KEY, ASSIGNED_TO_INFO);

  public static final Function<IScheduledTask, IJobKey> SCHEDULED_TO_JOB_KEY =
      Functions.compose(ASSIGNED_TO_JOB_KEY, SCHEDULED_TO_ASSIGNED);

  /**
   * Different states that an active task may be in.
   */
  public static final EnumSet<ScheduleStatus> ACTIVE_STATES =
      EnumSet.copyOf(Constants.ACTIVE_STATES);

  /**
   * Terminal states, which a task should not move from.
   */
  public static final Set<ScheduleStatus> TERMINAL_STATES =
      EnumSet.copyOf(Constants.TERMINAL_STATES);

  public static final Predicate<ITaskConfig> IS_PRODUCTION =
      new Predicate<ITaskConfig>() {
        @Override public boolean apply(ITaskConfig task) {
          return task.isProduction();
        }
      };

  public static final Function<IScheduledTask, ScheduleStatus> GET_STATUS =
      new Function<IScheduledTask, ScheduleStatus>() {
        @Override public ScheduleStatus apply(IScheduledTask task) {
          return task.getStatus();
        }
      };

  /**
   * Order by production flag (true, then false), subsorting by task ID.
   */
  public static final Ordering<IAssignedTask> SCHEDULING_ORDER =
      Ordering.explicit(true, false)
          .onResultOf(Functions.compose(Functions.forPredicate(IS_PRODUCTION), ASSIGNED_TO_INFO))
          .compound(Ordering.natural().onResultOf(ASSIGNED_TO_ID));

  private Tasks() {
    // Utility class.
  }

  /**
   * A utility method that returns a multi-map of tasks keyed by IJobKey.
   * @param tasks A list of tasks to be keyed by map
   * @return A multi-map of tasks keyed by job key.
   */
  public static Multimap<IJobKey, IScheduledTask> byJobKey(Iterable<IScheduledTask> tasks) {
    return Multimaps.index(tasks, Tasks.SCHEDULED_TO_JOB_KEY);
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
    return ImmutableSet.copyOf(Iterables.transform(tasks, SCHEDULED_TO_ID));
  }

  public static Set<String> ids(IScheduledTask... tasks) {
    return ids(ImmutableList.copyOf(tasks));
  }

  public static Map<String, IScheduledTask> mapById(Iterable<IScheduledTask> tasks) {
    return Maps.uniqueIndex(tasks, SCHEDULED_TO_ID);
  }

  public static String getRole(IScheduledTask task) {
    return task.getAssignedTask().getTask().getOwner().getRole();
  }

  public static String getJob(IScheduledTask task) {
    return task.getAssignedTask().getTask().getJobName();
  }

  public static final Ordering<IScheduledTask> LATEST_ACTIVITY = Ordering.natural()
      .onResultOf(new Function<IScheduledTask, Long>() {
        @Override public Long apply(IScheduledTask task) {
          return Iterables.getLast(task.getTaskEvents()).getTimestamp();
        }
      });
}

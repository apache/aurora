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
import com.google.common.collect.Ordering;

import com.twitter.aurora.gen.AssignedTask;
import com.twitter.aurora.gen.Constants;
import com.twitter.aurora.gen.JobKey;
import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.TaskConfig;

/**
 * Utility class providing convenience functions relating to tasks.
 */
public final class Tasks {

  public static final Function<ScheduledTask, AssignedTask> SCHEDULED_TO_ASSIGNED =
      new Function<ScheduledTask, AssignedTask>() {
        @Override public AssignedTask apply(ScheduledTask task) {
          return task.getAssignedTask();
        }
      };

  public static final Function<AssignedTask, TaskConfig> ASSIGNED_TO_INFO =
      new Function<AssignedTask, TaskConfig>() {
        @Override public TaskConfig apply(AssignedTask task) {
          return task.getTask();
        }
      };

  public static final Function<ScheduledTask, TaskConfig> SCHEDULED_TO_INFO =
      Functions.compose(ASSIGNED_TO_INFO, SCHEDULED_TO_ASSIGNED);

  public static final Function<AssignedTask, String> ASSIGNED_TO_ID =
      new Function<AssignedTask, String>() {
        @Override public String apply(AssignedTask task) {
          return task.getTaskId();
        }
      };

  public static final Function<ScheduledTask, String> SCHEDULED_TO_ID =
      Functions.compose(ASSIGNED_TO_ID, SCHEDULED_TO_ASSIGNED);

  public static final Function<TaskConfig, Integer> INFO_TO_SHARD_ID =
      new Function<TaskConfig, Integer>() {
        @Override public Integer apply(TaskConfig task) {
          return task.getShardId();
        }
      };

  public static final Function<ScheduledTask, Integer> SCHEDULED_TO_SHARD_ID =
      Functions.compose(INFO_TO_SHARD_ID, SCHEDULED_TO_INFO);

  public static final Function<TaskConfig, JobKey> INFO_TO_JOB_KEY =
      new Function<TaskConfig, JobKey>() {
        @Override public JobKey apply(TaskConfig task) {
          return JobKeys.from(task.getOwner().getRole(), task.getEnvironment(), task.getJobName());
        }
      };

  public static final Function<AssignedTask, JobKey> ASSIGNED_TO_JOB_KEY =
      Functions.compose(INFO_TO_JOB_KEY, ASSIGNED_TO_INFO);

  public static final Function<ScheduledTask, JobKey> SCHEDULED_TO_JOB_KEY =
      Functions.compose(ASSIGNED_TO_JOB_KEY, SCHEDULED_TO_ASSIGNED);

  public static final Function<ScheduledTask, ScheduledTask> DEEP_COPY_SCHEDULED =
      new Function<ScheduledTask, ScheduledTask>() {
        @Override public ScheduledTask apply(ScheduledTask task) {
          return task.deepCopy();
        }
      };

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

  public static final Predicate<TaskConfig> IS_PRODUCTION =
      new Predicate<TaskConfig>() {
        @Override public boolean apply(TaskConfig task) {
          return task.isProduction();
        }
      };

  public static final Function<ScheduledTask, ScheduleStatus> GET_STATUS =
      new Function<ScheduledTask, ScheduleStatus>() {
        @Override public ScheduleStatus apply(ScheduledTask task) {
          return task.getStatus();
        }
      };

  /**
   * Order by production flag (true, then false), subsorting by task ID.
   */
  public static final Ordering<AssignedTask> SCHEDULING_ORDER =
      Ordering.explicit(true, false)
          .onResultOf(Functions.compose(Functions.forPredicate(IS_PRODUCTION), ASSIGNED_TO_INFO))
          .compound(Ordering.natural().onResultOf(ASSIGNED_TO_ID));

  private Tasks() {
    // Utility class.
  }

  public static boolean isActive(ScheduleStatus status) {
    return ACTIVE_STATES.contains(status);
  }

  public static boolean isTerminated(ScheduleStatus status) {
    return TERMINAL_STATES.contains(status);
  }

  public static String id(ScheduledTask task) {
    return task.getAssignedTask().getTaskId();
  }

  public static Set<String> ids(Iterable<ScheduledTask> tasks) {
    return ImmutableSet.copyOf(Iterables.transform(tasks, SCHEDULED_TO_ID));
  }

  public static Set<String> ids(ScheduledTask... tasks) {
    return ids(ImmutableList.copyOf(tasks));
  }

  public static Map<String, ScheduledTask> mapById(Iterable<ScheduledTask> tasks) {
    return Maps.uniqueIndex(tasks, SCHEDULED_TO_ID);
  }

  public static String getRole(ScheduledTask task) {
    return task.getAssignedTask().getTask().getOwner().getRole();
  }

  public static String getJob(ScheduledTask task) {
    return task.getAssignedTask().getTask().getJobName();
  }
}

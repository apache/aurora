package com.twitter.mesos;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.LiveTask;
import com.twitter.mesos.gen.LiveTaskInfo;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.TaskStore.TaskState;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;

import static com.twitter.mesos.gen.ScheduleStatus.*;

/**
 * Utility class providing convenience functions relating to tasks.
 *
 * @author wfarner
 */
public class Tasks {

  public static final Function<TaskState, ScheduledTask> STATE_TO_SCHEDULED =
      new Function<TaskState, ScheduledTask>() {
        @Override public ScheduledTask apply(TaskState state) {
          return state.task;
        }
      };

  public static final Function<ScheduledTask, AssignedTask> SCHEDULED_TO_ASSIGNED =
      new Function<ScheduledTask, AssignedTask>() {
        @Override public AssignedTask apply(ScheduledTask task) {
          return task.getAssignedTask();
        }
      };

  public static final Function<AssignedTask, TwitterTaskInfo> ASSIGNED_TO_INFO =
      new Function<AssignedTask, TwitterTaskInfo>() {
        @Override public TwitterTaskInfo apply(AssignedTask task) {
          return task.getTask();
        }
      };

  public static final Function<TaskState, AssignedTask> STATE_TO_ASSIGNED =
      Functions.compose(SCHEDULED_TO_ASSIGNED, STATE_TO_SCHEDULED);

  public static final Function<AssignedTask, Integer> ASSIGNED_TO_ID =
      new Function<AssignedTask, Integer>() {
        @Override public Integer apply(AssignedTask task) {
          return task.getTaskId();
        }
      };

  public static final Function<ScheduledTask, Integer> SCHEDULED_TO_ID =
      Functions.compose(ASSIGNED_TO_ID, SCHEDULED_TO_ASSIGNED);

  public static final Function<TaskState, Integer> STATE_TO_ID =
      Functions.compose(SCHEDULED_TO_ID, STATE_TO_SCHEDULED);

  public static final Function<LiveTaskInfo, Integer> LIVE_TO_ID =
      new Function<LiveTaskInfo, Integer>() {
        @Override public Integer apply(LiveTaskInfo info) { return info.getTaskId(); }
      };

  public static final Function<TwitterTaskInfo, Integer> INFO_TO_SHARD_ID =
      new Function<TwitterTaskInfo, Integer>() {
        @Override public Integer apply(TwitterTaskInfo task) {
          return task.getShardId();
        }
      };

  public static final Function<AssignedTask, Integer> ASSIGNED_TO_SHARD_ID =
      Functions.compose(INFO_TO_SHARD_ID, ASSIGNED_TO_INFO);

  public static final Function<TaskState, LiveTask> STATE_TO_LIVE =
      new Function<TaskState, LiveTask>() {
        @Override public LiveTask apply(TaskState state) {
          return new LiveTask(state.task, state.volatileState.resources);
        }
      };

  public static final Function<TaskState, String> STATE_TO_JOB_KEY =
      new Function<TaskState, String>() {
        @Override public String apply(TaskState state) {
          return jobKey(state);
        }
      };

  /**
   * Different states that an active task may be in.
   */
  public static final Set<ScheduleStatus> ACTIVE_STATES = EnumSet.of(
      PENDING, STARTING, RUNNING);

  /**
   * Terminal states, which a task should not move from.
   */
  public static final Set<ScheduleStatus> TERMINAL_STATES = EnumSet.of(
      FAILED, FINISHED, KILLED, KILLED_BY_CLIENT, LOST, NOT_FOUND
  );

  /**
   * Filter that includes only active tasks.
   */
  public static final Predicate<TaskState> ACTIVE_FILTER = new Predicate<TaskState>() {
      @Override public boolean apply(TaskState state) {
        return isActive(state.task.getStatus());
      }
    };

  /**
   * Filter that includes only terminated tasks.
   */
  public static final Predicate<TaskState> TERMINATED_FILTER = new Predicate<TaskState>() {
      @Override public boolean apply(TaskState state) {
        return isTerminated(state.task.getStatus());
      }
    };

  private Tasks() {
    // Utility class.
  }

  public static boolean isActive(ScheduleStatus status) {
    return ACTIVE_STATES.contains(status);
  }

  public static boolean isTerminated(ScheduleStatus status) {
    return TERMINAL_STATES.contains(status);
  }

  public static Predicate<TaskState> hasStatus(ScheduleStatus... statuses) {
    final Set<ScheduleStatus> filter = EnumSet.copyOf(Arrays.asList(statuses));

    return new Predicate<TaskState>() {
      @Override public boolean apply(TaskState state) {
        return filter.contains(state.task.getStatus());
      }
    };
  }

  public static String jobKey(String owner, String jobName) {
    return owner + "/" + jobName;
  }

  public static String jobKey(TwitterTaskInfo task) {
    return jobKey(task.getOwner(), task.getJobName());
  }

  public static String jobKey(JobConfiguration job) {
    return jobKey(job.getOwner(), job.getName());
  }

  public static String jobKey(AssignedTask task) {
    return jobKey(task.getTask());
  }

  public static String jobKey(ScheduledTask task) {
    return jobKey(task.getAssignedTask());
  }

  public static String jobKey(TaskState state) {
    return jobKey(state.task);
  }

  public static int id(ScheduledTask task) {
    return task.getAssignedTask().getTaskId();
  }

  public static int id(TaskState state) {
    return id(state.task);
  }
}

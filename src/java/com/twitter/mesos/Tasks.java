package com.twitter.mesos;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;

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

  public static final Function<ScheduledTask, Integer> GET_TASK_ID =
      new Function<ScheduledTask, Integer>() {
        @Override public Integer apply(ScheduledTask task) {
          return task.getAssignedTask().getTaskId();
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
  public static final Predicate<ScheduledTask> ACTIVE_FILTER = new Predicate<ScheduledTask>() {
      @Override public boolean apply(ScheduledTask task) {
        return isActive(task.getStatus());
      }
    };

  /**
   * Filter that includes only terminal tasks.
   */
  public static final Predicate<ScheduledTask> TERMINATED_FILTER = new Predicate<ScheduledTask>() {
      @Override public boolean apply(ScheduledTask task) {
        return isTerminated(task.getStatus());
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

  public static Predicate<ScheduledTask> makeStatusFilter(ScheduleStatus... statuses) {
    final Set<ScheduleStatus> filter = EnumSet.copyOf(Arrays.asList(statuses));

    return new Predicate<ScheduledTask>() {
      @Override public boolean apply(ScheduledTask task) {
        Preconditions.checkNotNull(task);
        return filter.contains(task.getStatus());
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
}

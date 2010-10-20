package com.twitter.mesos;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.TrackedTask;


import java.util.EnumSet;
import java.util.Set;

import static com.twitter.mesos.gen.ScheduleStatus.*;

/**
 * Utility class providing convenience functions relating to tasks.
 *
 * @author wfarner
 */
public class Tasks {

  public static final Function<TrackedTask, Integer> GET_TASK_ID =
      new Function<TrackedTask, Integer>() {
        @Override public Integer apply(TrackedTask task) {
          return task.getTaskId();
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
  public static final Predicate<TrackedTask> ACTIVE_FILTER = new Predicate<TrackedTask>() {
      @Override public boolean apply(TrackedTask task) {
        return isActive(task.getStatus());
      }
    };

  /**
   * Filter that includes only terminal tasks.
   */
  public static final Predicate<TrackedTask> TERMINATED_FILTER = new Predicate<TrackedTask>() {
      @Override public boolean apply(TrackedTask task) {
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
}

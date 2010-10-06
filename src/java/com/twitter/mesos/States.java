package com.twitter.mesos;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.TrackedTask;

import java.util.Set;

/**
 * Utility class to handle common tasks related to scheduling states.
 *
 * @author wfarner
 */
public class States {

  /**
   * Different states that an active task may be in.
   */
  public static final Set<ScheduleStatus> ACTIVE_STATES = Sets.newHashSet(
      ScheduleStatus.PENDING, ScheduleStatus.STARTING, ScheduleStatus.RUNNING);

  public static boolean isActive(ScheduleStatus status) {
    return ACTIVE_STATES.contains(status);
  }

  /**
   * Filter that includes only active tasks.
   */
  public static final Predicate<TrackedTask> ACTIVE_FILTER = new Predicate<TrackedTask>() {
      @Override public boolean apply(TrackedTask task) {
        return States.isActive(task.getStatus());
      }
    };
}

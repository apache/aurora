package com.twitter.mesos;

import com.google.common.base.Function;
import com.twitter.mesos.gen.TrackedTask;

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

  private Tasks() {
    // Utility class.
  }
}

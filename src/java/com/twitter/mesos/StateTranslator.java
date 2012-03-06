package com.twitter.mesos;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import org.apache.mesos.Protos.TaskState;

import com.twitter.mesos.gen.ScheduleStatus;

/**
 * Translates between mesos {@link TaskState} and the internal {@link ScheduleStatus}.
 *
 * @author William Farner
 */
public final class StateTranslator {

  // Maps from mesos state to scheduler interface state.
  private static final BiMap<TaskState, ScheduleStatus> STATE_TRANSLATION =
      new ImmutableBiMap.Builder<TaskState, ScheduleStatus>()
          .put(TaskState.TASK_STARTING, ScheduleStatus.STARTING)
          .put(TaskState.TASK_RUNNING, ScheduleStatus.RUNNING)
          .put(TaskState.TASK_FINISHED, ScheduleStatus.FINISHED)
          .put(TaskState.TASK_FAILED, ScheduleStatus.FAILED)
          .put(TaskState.TASK_KILLED, ScheduleStatus.KILLED)
          .put(TaskState.TASK_LOST, ScheduleStatus.LOST)
          .build();

  private StateTranslator() {
    // Utility class.
  }

  public static ScheduleStatus get(TaskState taskState) {
    ScheduleStatus status = STATE_TRANSLATION.get(taskState);
    Preconditions.checkArgument(status != null, "Unrecognized task state " + taskState);
    return status;
  }

  public static TaskState get(ScheduleStatus scheduleStatus) {
    TaskState state = STATE_TRANSLATION.inverse().get(scheduleStatus);
    Preconditions.checkArgument(state != null, "Unrecognized status " + scheduleStatus);
    return state;
  }
}

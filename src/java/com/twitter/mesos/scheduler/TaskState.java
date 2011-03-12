package com.twitter.mesos.scheduler;

import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Maps;

import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.LiveTask;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;

/**
 * A composition of both the persisted and non-persisted state related to a task.
 *
 * @author William Farner
 */
public class TaskState {

  public static final Function<TaskState, ScheduledTask> STATE_TO_SCHEDULED =
      new Function<TaskState, ScheduledTask>() {
        @Override public ScheduledTask apply(TaskState state) {
          return state.task;
        }
      };

  public static final Function<TaskState, String> STATE_TO_ID =
      Functions.compose(Tasks.SCHEDULED_TO_ID, STATE_TO_SCHEDULED);

  public static final Function<TaskState, AssignedTask> STATE_TO_ASSIGNED =
      Functions.compose(Tasks.SCHEDULED_TO_ASSIGNED, STATE_TO_SCHEDULED);

  public static final Function<TaskState, TwitterTaskInfo> STATE_TO_INFO =
      Functions.compose(Tasks.ASSIGNED_TO_INFO, STATE_TO_ASSIGNED);

  public static final Function<TaskState, Integer> STATE_TO_SHARD_ID =
      Functions.compose(Tasks.INFO_TO_SHARD_ID, STATE_TO_INFO);

  public static final Function<TaskState, LiveTask> STATE_TO_LIVE =
      new Function<TaskState, LiveTask>() {
        @Override public LiveTask apply(TaskState state) {
          return new LiveTask(state.task, state.volatileState.resources);
        }
      };

  public final ScheduledTask task;
  public final VolatileTaskState volatileState;

  public TaskState(ScheduledTask task, VolatileTaskState volatileTaskState) {
    this.task = new ScheduledTask(task);
    this.volatileState = new VolatileTaskState(volatileTaskState);
  }

  public TaskState(TaskState toCopy) {
    this.task = new ScheduledTask(toCopy.task);
    this.volatileState = new VolatileTaskState(toCopy.volatileState);
  }

  public static String jobKey(TaskState state) {
    return Tasks.jobKey(state.task);
  }

  public static String id(TaskState state) {
    return Tasks.id(state.task);
  }

  public static Map<Integer, TaskState> mapStateByShardId(Iterable<TaskState> tasks) {
    return Maps.uniqueIndex(tasks, STATE_TO_SHARD_ID);
  }

  @Override
  public int hashCode() {
    return task.hashCode();
  }

  @Override
  public boolean equals(Object that) {
    return that instanceof TaskState && ((TaskState) that).task.equals(this.task);
  }
}

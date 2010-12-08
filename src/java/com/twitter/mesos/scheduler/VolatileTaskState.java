package com.twitter.mesos.scheduler;

import com.twitter.mesos.gen.ResourceConsumption;

import javax.annotation.Nullable;

/**
 * Information about a task that should not be persisted.
 *
 * @author wfarner
 */
public class VolatileTaskState {
  public final int taskId;
  @Nullable
  public ResourceConsumption resources;

  VolatileTaskState(int taskId) {
    this.taskId = taskId;
  }

  VolatileTaskState(VolatileTaskState toCopy) {
    this.taskId = toCopy.taskId;
    this.resources = toCopy.resources == null ? null : new ResourceConsumption(toCopy.resources);
  }
}

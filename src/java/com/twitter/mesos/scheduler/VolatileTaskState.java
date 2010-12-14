package com.twitter.mesos.scheduler;

import com.twitter.mesos.gen.ResourceConsumption;

import javax.annotation.Nullable;

import static com.twitter.common.base.MorePreconditions.checkNotBlank;

/**
 * Information about a task that should not be persisted.
 *
 * @author wfarner
 */
public class VolatileTaskState {
  public final String taskId;
  @Nullable
  public ResourceConsumption resources;

  VolatileTaskState(String taskId) {
    this.taskId = checkNotBlank(taskId);
  }

  VolatileTaskState(VolatileTaskState toCopy) {
    this.taskId = toCopy.taskId;
    this.resources = toCopy.resources == null ? null : new ResourceConsumption(toCopy.resources);
  }
}

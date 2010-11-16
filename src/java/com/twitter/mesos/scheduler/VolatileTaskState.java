package com.twitter.mesos.scheduler;

import com.twitter.mesos.gen.ResourceConsumption;

import javax.annotation.Nullable;

/**
 * Information about a task that should not be persisted.
 *
 * @author wfarner
 */
class VolatileTaskState {
  final int taskId;
  @Nullable
  ResourceConsumption resources;

  VolatileTaskState(int taskId) {
    this.taskId = taskId;
  }
}

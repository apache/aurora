package com.twitter.mesos.scheduler;

import com.twitter.mesos.gen.ResourceConsumption;

import javax.annotation.Nullable;

import static com.twitter.common.base.MorePreconditions.checkNotBlank;

/**
 * Information about a task that should not be persisted.
 *
 * @author William Farner
 */
public class VolatileTaskState {
  @Nullable
  public ResourceConsumption resources;

  VolatileTaskState() {
    this.resources = null;
  }

  VolatileTaskState(VolatileTaskState toCopy) {
    this.resources = toCopy.resources == null ? null : new ResourceConsumption(toCopy.resources);
  }
}

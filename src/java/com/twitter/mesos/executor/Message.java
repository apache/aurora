package com.twitter.mesos.executor;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import com.twitter.mesos.gen.comm.SchedulerMessage;

/**
 * A message in the mesos system, between a scheduler and executor.
 *
 * @author William Farner
 */
public class Message {
  private final String slaveId;
  private final SchedulerMessage message;

  public Message(SchedulerMessage message) {
    this(null, message);
  }

  public Message(@Nullable String slaveId, SchedulerMessage message) {
    this.slaveId = slaveId;
    this.message = Preconditions.checkNotNull(message);
  }

  @Nullable public String getSlaveId() {
    return slaveId;
  }

  public SchedulerMessage getMessage() {
    return message;
  }

  @Override
  public String toString() {
    return "To " + slaveId + ": " + message;
  }
}

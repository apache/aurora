package com.twitter.mesos;

import com.google.common.base.Preconditions;
import org.apache.thrift.TBase;

import javax.annotation.Nullable;

/**
 * A message in the mesos system, between a scheduler and executor.
 *
 * @author William Farner
 */
public class Message {
  private final String slaveId;
  private final TBase message;

  public Message(TBase message) {
    this(null, message);
  }

  public Message(@Nullable String slaveId, TBase message) {
    this.slaveId = slaveId;
    this.message = Preconditions.checkNotNull(message);
  }

  @Nullable public String getSlaveId() {
    return slaveId;
  }

  public TBase getMessage() {
    return message;
  }

  @Override
  public String toString() {
    return "To " + slaveId + ": " + message;
  }
}

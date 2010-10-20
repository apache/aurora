package com.twitter.mesos;

import com.google.common.base.Preconditions;
import org.apache.thrift.TBase;

/**
 * A message in the mesos system, between a scheduler and executor.
 *
 * @author wfarner
 */
public class Message {
  private final String slaveId;
  private final TBase message;

  public Message(String slaveId, TBase message) {
    this.slaveId = Preconditions.checkNotNull(slaveId);
    this.message = Preconditions.checkNotNull(message);
  }

  public String getSlaveId() {
    return slaveId;
  }

  public TBase getMessage() {
    return message;
  }
}

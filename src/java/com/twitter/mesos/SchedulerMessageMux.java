package com.twitter.mesos;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.twitter.common.base.Closure;
import com.twitter.mesos.codec.Codec;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.gen.ExecutorStatus;
import com.twitter.mesos.gen.RegisteredTaskUpdate;
import com.twitter.mesos.gen.SchedulerMessage;
import com.twitter.mesos.gen.SchedulerMessageType;
import org.apache.thrift.TBase;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
* Multiplexer/demultiplexer to handle sending/serialization and deserialization of scheduler
* messages.
*
* @author wfarner
*/
public class SchedulerMessageMux<T> {
  private static final Logger LOG = Logger.getLogger(SchedulerMessageMux.class.getName());

  @VisibleForTesting
  static final Map<SchedulerMessageType, Class<? extends TBase>> TYPE_MAPPING =
      ImmutableMap.<SchedulerMessageType, Class<? extends TBase>>of(
        SchedulerMessageType.EXECUTOR_STATUS, ExecutorStatus.class,
        SchedulerMessageType.REGISTERED_TASK_UPDATE, RegisteredTaskUpdate.class);

  public static <T extends TBase> SchedulerMessage mux(SchedulerMessageType messageType,
      Class<T> messageClass, T message) throws Codec.CodingException {

    Preconditions.checkNotNull(messageType);
    Preconditions.checkNotNull(messageClass);
    Preconditions.checkNotNull(message);

    SchedulerMessage schedulerMessage = new SchedulerMessage();
    schedulerMessage.setMessageType(messageType);

    Codec<T, byte[]> codec = new ThriftBinaryCodec<T>(messageClass);
    schedulerMessage.setPayload(codec.encode(message));

    return schedulerMessage;
  }

  public static void demux(SchedulerMessage message,
      Map<SchedulerMessageType, Closure<TBase>> callbacks) {

    Preconditions.checkNotNull(message);
    Preconditions.checkNotNull(callbacks);

    Closure<TBase> callback = callbacks.get(message.getMessageType());
    if (callback == null) {
      LOG.severe("No callback available for scheduler message " + message);
      return;
    }

    Class<? extends TBase> typeClass = TYPE_MAPPING.get(message.getMessageType());
    if (typeClass == null) {
      LOG.severe("No class mapping for message " + message);
      return;
    }

    @SuppressWarnings("unchecked")
    Codec<? extends TBase, byte[]> codec = new ThriftBinaryCodec(typeClass);
    TBase object;
    try {
      object = codec.decode(message.getPayload());
    } catch (Codec.CodingException e) {
      LOG.log(Level.SEVERE, "Failed to decode scheduler message " + message, e);
      return;
    }

    callback.execute(object);
  }

  private SchedulerMessageMux() {
    // Utility class.
  }
}

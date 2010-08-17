package com.twitter.mesos;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.twitter.common.base.Closure;
import com.twitter.mesos.codec.Codec;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.gen.ExecutorMessage;
import com.twitter.mesos.gen.ExecutorMessageType;
import com.twitter.mesos.gen.MachineDrain;
import com.twitter.mesos.gen.RestartExecutor;
import org.apache.thrift.TBase;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
* Multiplexer/demultiplexer to handle sending/serialization and deserialization of executor
* messages.
*
* @author wfarner
*/
public class ExecutorMessageMux<T> {
  private static final Logger LOG = Logger.getLogger(ExecutorMessageMux.class.getName());

  @VisibleForTesting
  static final Map<ExecutorMessageType, Class<? extends TBase>> TYPE_MAPPING =
      ImmutableMap.<ExecutorMessageType, Class<? extends TBase>>of(
        ExecutorMessageType.MACHINE_DRAIN, MachineDrain.class,
        ExecutorMessageType.RESTART_EXECUTOR, RestartExecutor.class);

  public static <T extends TBase> ExecutorMessage mux(ExecutorMessageType messageType,
      Class<T> messageClass, T message) throws Codec.CodingException {

    Preconditions.checkNotNull(messageType);
    Preconditions.checkNotNull(messageClass);
    Preconditions.checkNotNull(message);

    ExecutorMessage executorMessage = new ExecutorMessage();
    executorMessage.setMessageType(messageType);

    Codec<T, byte[]> codec = new ThriftBinaryCodec<T>(messageClass);
    executorMessage.setPayload(codec.encode(message));

    return executorMessage;
  }

  public static void demux(ExecutorMessage message,
      Map<ExecutorMessageType, Closure<TBase>> callbacks) {

    Preconditions.checkNotNull(message);
    Preconditions.checkNotNull(callbacks);

    Closure<TBase> callback = callbacks.get(message.getMessageType());
    if (callback == null) {
      LOG.severe("No callback available for executor message " + message);
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
      LOG.log(Level.SEVERE, "Failed to decode executor message " + message, e);
      return;
    }

    callback.execute(object);
  }

  private ExecutorMessageMux() {
    // Utility class.
  }
}

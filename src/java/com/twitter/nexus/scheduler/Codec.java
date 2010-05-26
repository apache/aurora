package com.twitter.nexus.scheduler;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

/**
 * Serialization/deserialization functions used in the scheduler.
 *
 * @author wfarner
 */
public class Codec {
  public static byte[] serialize(TBase t) throws SerializationException {
      try {
        return new TSerializer().serialize(t);
      } catch (TException e) {
        throw new SerializationException("Failed to serialize: " + t, e);
      }
  }

  public static <T extends TBase> void deserialize(T t, byte[] data) throws SerializationException {
    try {
      new TDeserializer().deserialize(t, data);
    } catch (TException e) {
      throw new SerializationException("Failed to deserialize thrift object.", e);
    }
  }

  public static class SerializationException extends Exception {
    public SerializationException(String msg, Throwable t) {
      super(msg, t);
    }
  }

  private Codec() {
    // Utility class.
  }
}

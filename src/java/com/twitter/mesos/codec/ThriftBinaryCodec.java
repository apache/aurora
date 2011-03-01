package com.twitter.mesos.codec;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

import javax.annotation.Nullable;

/**
 * Codec that works for thrift objects.
 *
 * @author William Farner
 */
public class ThriftBinaryCodec {

  /**
   * Protocol factory used for all thrift encoding and decoding.
   */
  public static final TProtocolFactory PROTOCOL_FACTORY = new TBinaryProtocol.Factory();

  @Nullable
  public static <T extends TBase> T decode(Class<T> clazz, byte[] buffer) throws CodingException {
    if (buffer == null) return null;

    T t;
    try {
      t = clazz.newInstance();
    } catch (InstantiationException e) {
      throw new CodingException("Failed to instantiate target type.", e);
    } catch (IllegalAccessException e) {
      throw new CodingException("Failed to access constructor for target type.", e);
    }

    try {
      new TDeserializer(PROTOCOL_FACTORY).deserialize(t, buffer);
      return t;
    } catch (TException e) {
      throw new CodingException("Failed to deserialize thrift object.", e);
    }
  }

  @Nullable
  public static byte[] encode(TBase tBase) throws CodingException {
    if (tBase == null) return null;

    try {
      return new TSerializer(PROTOCOL_FACTORY).serialize(tBase);
    } catch (TException e) {
      throw new CodingException("Failed to serialize: " + tBase, e);
    }
  }

  public static class CodingException extends Exception {
    public CodingException(String msg) {
      super(msg);
    }
    public CodingException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }
}

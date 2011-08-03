package com.twitter.mesos.codec;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

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
    if (buffer == null) {
      return null;
    }
    return decodeNonNull(clazz, buffer);
  }

  public static <T extends TBase> T decodeNonNull(Class<T> clazz, byte[] buffer)
      throws CodingException {

    Preconditions.checkNotNull(clazz);
    Preconditions.checkNotNull(buffer);

    try {
      T t = clazz.newInstance();
      new TDeserializer(PROTOCOL_FACTORY).deserialize(t, buffer);
      return t;
    } catch (IllegalAccessException e) {
      throw new CodingException("Failed to access constructor for target type.", e);
    } catch (InstantiationException e) {
      throw new CodingException("Failed to instantiate target type.", e);
    } catch (TException e) {
      throw new CodingException("Failed to deserialize thrift object.", e);
    }
  }

  @Nullable
  public static byte[] encode(TBase tBase) throws CodingException {
    if (tBase == null) {
      return null;
    }
    return encodeNonNull(tBase);
  }

  public static byte[] encodeNonNull(TBase tBase) throws CodingException {
    Preconditions.checkNotNull(tBase);

    try {
      return new TSerializer(PROTOCOL_FACTORY).serialize(tBase);
    } catch (TException e) {
      throw new CodingException("Failed to serialize: " + tBase, e);
    }
  }

  public static class CodingException extends Exception {
    public CodingException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }
}

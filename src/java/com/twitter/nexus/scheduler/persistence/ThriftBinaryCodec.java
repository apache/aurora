package com.twitter.nexus.scheduler.persistence;

import com.google.inject.internal.Preconditions;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import javax.annotation.Nullable;

/**
 * Codec that works for thrift objects.  Must be extended to provide instances of target type.
 *
 * @author wfarner
 */
public class ThriftBinaryCodec<T extends TBase> implements Codec<T, byte[]>{

  private final Class<T> thriftClassType;

  public ThriftBinaryCodec(Class<T> thriftClassType) {
    this.thriftClassType = Preconditions.checkNotNull(thriftClassType);
  }

  @Override
  @Nullable
  public T decode(byte[] bytes) throws CodingException {
    if (bytes == null) return null;

    T t;
    try {
      t = thriftClassType.newInstance();
    } catch (InstantiationException e) {
      throw new CodingException("Failed to instantiate target type.", e);
    } catch (IllegalAccessException e) {
      throw new CodingException("Failed to access constructor for target type.", e);
    }

    try {
      new TDeserializer().deserialize(t, bytes);
      return t;
    } catch (TException e) {
      throw new CodingException("Failed to deserialize thrift object.", e);
    }
  }

  @Override
  @Nullable
  public byte[] encode(TBase tBase) throws CodingException {
    if (tBase == null) return null;

    try {
      return new TSerializer().serialize(tBase);
    } catch (TException e) {
      throw new CodingException("Failed to serialize: " + tBase, e);
    }
  }
}

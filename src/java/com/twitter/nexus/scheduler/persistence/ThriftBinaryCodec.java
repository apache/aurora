package com.twitter.nexus.scheduler.persistence;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

/**
 * Codec that works for thrift objects.  Must be extended to provide instances of target type.
 *
 * @author wfarner
 */
public abstract class ThriftBinaryCodec<T extends TBase> implements Codec<T, byte[]>{

  @Override
  public T decode(byte[] bytes) throws CodingException {
    if (bytes == null) return null;

    T t = createInputInstance();

    try {
      new TDeserializer().deserialize(t, bytes);
      return t;
    } catch (TException e) {
      throw new CodingException("Failed to deserialize thrift object.", e);
    }
  }

  @Override
  public byte[] encode(TBase tBase) throws CodingException {
    if (tBase == null) return null;

    try {
      return new TSerializer().serialize(tBase);
    } catch (TException e) {
      throw new CodingException("Failed to serialize: " + tBase, e);
    }
  }

  @Override
  public byte[] createOutputInstance() {
    return new byte[0];
  }
}

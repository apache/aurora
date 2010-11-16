package com.twitter.mesos.scheduler.persistence;

import com.google.common.base.Preconditions;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.gen.NonVolatileSchedulerState;

/**
 * A persistence layer that uses a codec to translate objects.
 *
 * @author wfarner
 */
public class EncodingPersistenceLayer implements PersistenceLayer<NonVolatileSchedulerState> {
  private final PersistenceLayer<byte[]> wrapped;

  public EncodingPersistenceLayer(PersistenceLayer<byte[]> wrapped) {
    this.wrapped = Preconditions.checkNotNull(wrapped);
  }

  @Override
  public NonVolatileSchedulerState fetch() throws PersistenceException {
    try {
      return ThriftBinaryCodec.decode(NonVolatileSchedulerState.class, wrapped.fetch());
    } catch (ThriftBinaryCodec.CodingException e) {
      throw new PersistenceException("Failed to decode data.", e);
    }
  }

  @Override
  public void commit(NonVolatileSchedulerState data) throws PersistenceException {
    try {
      wrapped.commit(ThriftBinaryCodec.encode(data));
    } catch (ThriftBinaryCodec.CodingException e) {
      throw new PersistenceException("Failed to encode data.", e);
    }
  }
}

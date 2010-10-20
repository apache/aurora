package com.twitter.mesos.scheduler.persistence;

import com.google.common.base.Preconditions;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.gen.SchedulerState;

/**
 * A persistence layer that uses a codec to translate objects.
 *
 * @author wfarner
 */
public class EncodingPersistenceLayer implements PersistenceLayer<SchedulerState> {
  private final PersistenceLayer<byte[]> wrapped;

  public EncodingPersistenceLayer(PersistenceLayer<byte[]> wrapped) {
    this.wrapped = Preconditions.checkNotNull(wrapped);
  }

  @Override
  public SchedulerState fetch() throws PersistenceException {
    try {
      return ThriftBinaryCodec.decode(SchedulerState.class, wrapped.fetch());
    } catch (ThriftBinaryCodec.CodingException e) {
      throw new PersistenceException("Failed to decode data.", e);
    }
  }

  @Override
  public void commit(SchedulerState data) throws PersistenceException {
    try {
      wrapped.commit(ThriftBinaryCodec.encode(data));
    } catch (ThriftBinaryCodec.CodingException e) {
      throw new PersistenceException("Failed to encode data.", e);
    }
  }
}

package com.twitter.nexus.scheduler.persistence;

import com.google.common.base.Preconditions;

/**
 * A persistence layer that uses a codec to translate objects.
 *
 * @author wfarner
 */
public class EncodingPersistenceLayer<T, U> implements PersistenceLayer<T> {
  private final Codec<T, U> codec;
  private final PersistenceLayer<U> wrapped;

  public EncodingPersistenceLayer(PersistenceLayer<U> wrapped, Codec<T, U> codec) {
    this.wrapped = Preconditions.checkNotNull(wrapped);
    this.codec = Preconditions.checkNotNull(codec);
  }

  @Override
  public T fetch() throws PersistenceException {
    try {
      return codec.decode(wrapped.fetch());
    } catch (Codec.CodingException e) {
      throw new PersistenceException("Failed to decode data.", e);
    }
  }

  @Override
  public void commit(T data) throws PersistenceException {
    try {
      wrapped.commit(codec.encode(data));
    } catch (Codec.CodingException e) {
      throw new PersistenceException("Failed to encode data.", e);
    }
  }
}

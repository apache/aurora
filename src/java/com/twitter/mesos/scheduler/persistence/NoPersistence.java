package com.twitter.mesos.scheduler.persistence;

/**
 * A persistence layer that does not persist anything.
 *
 * @author wfarner
 */
public class NoPersistence<T> implements PersistenceLayer<T> {
  @Override
  public T fetch() throws PersistenceException {
    // No-op.
    return null;
  }

  @Override
  public void commit(T data) throws PersistenceException {
    // No-op.
  }
}

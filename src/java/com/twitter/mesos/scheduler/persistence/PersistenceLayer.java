package com.twitter.mesos.scheduler.persistence;

import javax.annotation.Nullable;

/**
 * Interface that defines the methods required for scheduler state persistence.
 *
 * @author William Farner
 */
public interface PersistenceLayer<T> {
  @Nullable
  public T fetch() throws PersistenceException;

  public void commit(T data) throws PersistenceException;

  class PersistenceException extends Exception {
    public PersistenceException(String msg, Throwable t) {
      super(msg, t);
    }

    public PersistenceException(String msg) {
      super(msg);
    }
  }
}

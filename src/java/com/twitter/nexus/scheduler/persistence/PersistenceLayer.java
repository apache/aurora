package com.twitter.nexus.scheduler.persistence;

/**
 * Interface that defines the methods required for scheduler state persistence.
 *
 * @author wfarner
 */
public interface PersistenceLayer<T> {
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

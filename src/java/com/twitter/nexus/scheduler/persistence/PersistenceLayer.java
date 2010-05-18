package com.twitter.nexus.scheduler.persistence;

/**
 * Interface that defines the methods required for scheduler state persistence.
 *
 * @author wfarner
 */
public interface PersistenceLayer {
  public byte[] fetch() throws PersistenceException;

  public void commit(byte[] data) throws PersistenceException;

  class PersistenceException extends Exception {
    public PersistenceException(String msg, Throwable t) {
      super(msg, t);
    }
  }
}

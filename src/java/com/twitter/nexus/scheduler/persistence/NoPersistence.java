package com.twitter.nexus.scheduler.persistence;

/**
 * A persistence layer that does not persist anything.
 *
 * @author wfarner
 */
public class NoPersistence implements PersistenceLayer {
  @Override
  public byte[] fetch() throws PersistenceException {
    // No-op.
    return null;
  }

  @Override
  public void commit(byte[] data) throws PersistenceException {
    // No-op.
  }
}

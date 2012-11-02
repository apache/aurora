package com.twitter.mesos.scheduler.storage;

/**
 * A state-bearing entity that honors transactions.  Any state modifications made since the most
 * recent {@link #commit()} or {@link #rollback()} must be considered transient.
 *
 * TODO(William Farner): Rename this to Revertable.
 */
public interface Transactional {

  /**
   * Commits any modifications made since the last commit or rollback.  Upon completion of a
   * commit, the committed modifications are no longer considered transient.
   *
   * TODO(William Farner): Rename this to start(), and update docs.
   */
  void commit();

  /**
   * Reverts any modifications made since the last commit or rollback.  Prior state should be
   * restored and transient modifications discarded.
   *
   * TODO(William Farner): Rename this to revert().
   */
  void rollback();
}

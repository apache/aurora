package com.twitter.mesos.scheduler.storage;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import com.twitter.common.util.Clock;
import com.twitter.mesos.gen.storage.Snapshot;

/**
 * Storage mechanism that is able to create complete snapshots of the local storage system state
 * and apply these to restore local storage from a snapshotted baseline.
 *
 * @author John Sirois
 */
public interface SnapshotStore<T> {

  /**
   * Creates a consistent snapshot of the local storage system.
   *
   * @return A blob that can be used to recover local storage via {@link #applySnapshot(Object)}.
   */
   T createSnapshot();

  /**
   * Applies a snapshot blob to the local storage system, wiping out all existing data and
   * resetting with the contents of the snapshot.
   *
   * @param snapshot A snapshot blob created by {@link #createSnapshot()}.
   */
  void applySnapshot(T snapshot);

}

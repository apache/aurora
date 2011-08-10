package com.twitter.mesos.scheduler.storage;

import javax.annotation.Nullable;

/**
 * Storage mechanism that is able to create complete snapshots of the local storage system state
 * and apply these to restore local storage from a snapshotted baseline.
 *
 * @author John Sirois
 */
public interface CheckpointStore {

  /**
   * Creates a consistent snapshot of the local storage system.
   *
   * @return A blob that can be used to recover local storage via {@link #applySnapshot(byte[])}.
   */
  byte[] createSnapshot();

  /**
   * Applies a snapshot blob to the local storage system, wiping out all existing data and
   * resetting with the contents of the snapshot.
   *
   * @param snapshot A snapshot blob created by {@link #createSnapshot()}.
   */
  void applySnapshot(byte[] snapshot);

  /**
   * Records a checkpoint signifying a known good state of the underlying storage system.
   *
   * @param handle An opaque identity for the checkpoint.
   */
  void checkpoint(byte[] handle);

  /**
   * Retrieves the most recent checkpoint.
   *
   * @return The most recent checkpoint or else {@code null} if there have benn none performed.
   */
  @Nullable
  byte[] fetchCheckpoint();
}

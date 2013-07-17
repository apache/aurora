package com.twitter.aurora.scheduler.storage;

import com.twitter.aurora.codec.ThriftBinaryCodec.CodingException;
import com.twitter.aurora.gen.storage.Snapshot;

/**
 * A distributed snapshot store that supports persisting globally-visible snapshots.
 */
public interface DistributedSnapshotStore {
  /**
   * Writes a snapshot to the distributed storage system.
   * TODO(William Farner): Currently we're hiding some exceptions (which happen to be
   * RuntimeExceptions).  Clean these up to be checked, and throw another exception type here.
   *
   * @param snapshot Snapshot to write.
   * @throws CodingException If the snapshot could not be serialized.
   */
  void persist(Snapshot snapshot) throws CodingException;
}

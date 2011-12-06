package com.twitter.mesos.scheduler.storage.log;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import com.twitter.common.util.Clock;
import com.twitter.mesos.gen.storage.Snapshot;
import com.twitter.mesos.scheduler.storage.SnapshotStore;

/**
 * Snapshot store implementation that delegates to underlying snapshot stores by
 * extracting/applying fields in a snapshot thrift struct.
 */
public final class SnapshotStoreImpl implements SnapshotStore<Snapshot> {

  private final Clock clock;
  private final SnapshotStore<byte[]> binarySnapshotStore;

  @Inject
  public SnapshotStoreImpl(Clock clock, SnapshotStore<byte[]> binarySnapshotStore) {
    this.clock = Preconditions.checkNotNull(clock);
    this.binarySnapshotStore = Preconditions.checkNotNull(binarySnapshotStore);
  }

  @Override public Snapshot createSnapshot() {
    return new Snapshot()
        .setTimestamp(clock.nowMillis())
        .setData(binarySnapshotStore.createSnapshot());
  }

  @Override public void applySnapshot(Snapshot snapshot) {
    Preconditions.checkNotNull(snapshot);
    binarySnapshotStore.applySnapshot(snapshot.getData());
  }
}

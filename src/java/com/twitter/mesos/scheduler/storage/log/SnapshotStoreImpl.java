package com.twitter.mesos.scheduler.storage.log;

import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import com.twitter.common.util.Clock;
import com.twitter.mesos.gen.HostAttributes;
import com.twitter.mesos.gen.storage.Snapshot;
import com.twitter.mesos.scheduler.storage.SnapshotStore;

/**
 * Snapshot store implementation that delegates to underlying snapshot stores by
 * extracting/applying fields in a snapshot thrift struct.
 */
public final class SnapshotStoreImpl implements SnapshotStore<Snapshot> {

  private final Clock clock;
  private final SnapshotStore<byte[]> binarySnapshotStore;
  private final SnapshotStore<Set<HostAttributes>> hostAttributeSnapshotStore;

  @Inject
  public SnapshotStoreImpl(Clock clock, SnapshotStore<byte[]> binarySnapshotStore,
                           SnapshotStore<Set<HostAttributes>> hostAttributeSnapshotStore) {
    this.clock = Preconditions.checkNotNull(clock);
    this.binarySnapshotStore = Preconditions.checkNotNull(binarySnapshotStore);
    this.hostAttributeSnapshotStore = hostAttributeSnapshotStore;
  }

  @Override public Snapshot createSnapshot() {
    return new Snapshot()
        .setTimestamp(clock.nowMillis())
        .setData(binarySnapshotStore.createSnapshot())
        .setHostAttributes(hostAttributeSnapshotStore.createSnapshot());
  }

  @Override public void applySnapshot(Snapshot snapshot) {
    Preconditions.checkNotNull(snapshot);
    binarySnapshotStore.applySnapshot(snapshot.getData());
    hostAttributeSnapshotStore.applySnapshot(snapshot.getHostAttributes());
  }
}

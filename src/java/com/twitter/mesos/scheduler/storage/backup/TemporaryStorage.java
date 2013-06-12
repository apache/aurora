package com.twitter.mesos.scheduler.storage.backup;

import java.util.Set;

import com.google.common.base.Function;

import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.storage.Snapshot;
import com.twitter.mesos.scheduler.storage.SnapshotStore;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.log.SnapshotStoreImpl;
import com.twitter.mesos.scheduler.storage.mem.MemStorage;

/**
 * A short-lived in-memory storage system that can be converted to a {@link Snapshot}.
 */
interface TemporaryStorage {

  /**
   * Deletes all tasks matching a query.  Deleted tasks will not be reflected in the snapshot when
   * {@link #toSnapshot()} is executed.
   *
   * @param query Query for tasks to delete.
   */
  void deleteTasks(TaskQuery query);

  /**
   * Fetches tasks matching a query.
   *
   * @param query Query for tasks to fetch.
   * @return Matching tasks.
   */
  Set<ScheduledTask> fetchTasks(TaskQuery query);

  /**
   * Creates a snapshot of the contents of the temporary storage.
   *
   * @return Temporary storage snapshot.
   */
  Snapshot toSnapshot();

  /**
   * A factory that creates temporary storage instances, detached from the rest of the system.
   */
  class TemporaryStorageFactory implements Function<Snapshot, TemporaryStorage> {
    @Override public TemporaryStorage apply(Snapshot snapshot) {
      final Storage storage = MemStorage.newEmptyStorage();
      FakeClock clock = new FakeClock();
      clock.setNowMillis(snapshot.getTimestamp());
      final SnapshotStore<Snapshot> snapshotStore = new SnapshotStoreImpl(clock, storage);
      snapshotStore.applySnapshot(snapshot);

      return new TemporaryStorage() {
        @Override public void deleteTasks(final TaskQuery query) {
          storage.write(new MutateWork.NoResult.Quiet() {
            @Override protected void execute(MutableStoreProvider storeProvider) {
              storeProvider.getUnsafeTaskStore()
                  .deleteTasks(storeProvider.getTaskStore().fetchTaskIds(query));
            }
          });
        }

        @Override public Set<ScheduledTask> fetchTasks(final TaskQuery query) {
          return storage.consistentRead(new Work.Quiet<Set<ScheduledTask>>() {
            @Override public Set<ScheduledTask> apply(StoreProvider storeProvider) {
              return storeProvider.getTaskStore().fetchTasks(query);
            }
          });
        }

        @Override public Snapshot toSnapshot() {
          return snapshotStore.createSnapshot();
        }
      };
    }
  }
}

/*
 * Copyright 2013 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.aurora.scheduler.storage.backup;

import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;

import com.twitter.aurora.gen.storage.Snapshot;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.storage.SnapshotStore;
import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.aurora.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.aurora.scheduler.storage.Storage.MutateWork;
import com.twitter.aurora.scheduler.storage.Storage.StoreProvider;
import com.twitter.aurora.scheduler.storage.Storage.Work;
import com.twitter.aurora.scheduler.storage.entities.IScheduledTask;
import com.twitter.aurora.scheduler.storage.log.SnapshotStoreImpl;
import com.twitter.aurora.scheduler.storage.mem.MemStorage;
import com.twitter.common.util.testing.FakeClock;

/**
 * A short-lived in-memory storage system that can be converted to a {@link Snapshot}.
 */
interface TemporaryStorage {

  /**
   * Deletes all tasks matching a query.  Deleted tasks will not be reflected in the snapshot when
   * {@link #toSnapshot()} is executed.
   *
   * @param query Query builder for tasks to delete.
   */
  void deleteTasks(Query.Builder query);

  /**
   * Fetches tasks matching a query.
   *
   * @param query Query builder for tasks to fetch.
   * @return Matching tasks.
   */
  Set<IScheduledTask> fetchTasks(Query.Builder query);

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
        @Override public void deleteTasks(final Query.Builder query) {
          storage.write(new MutateWork.NoResult.Quiet() {
            @Override protected void execute(MutableStoreProvider storeProvider) {
              Set<String> ids = FluentIterable.from(storeProvider.getTaskStore().fetchTasks(query))
                  .transform(Tasks.SCHEDULED_TO_ID)
                  .toSet();
              storeProvider.getUnsafeTaskStore().deleteTasks(ids);
            }
          });
        }

        @Override public Set<IScheduledTask> fetchTasks(final Query.Builder query) {
          return storage.consistentRead(new Work.Quiet<Set<IScheduledTask>>() {
            @Override public Set<IScheduledTask> apply(StoreProvider storeProvider) {
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

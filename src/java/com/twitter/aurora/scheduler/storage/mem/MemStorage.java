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
package com.twitter.aurora.scheduler.storage.mem;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;

import com.twitter.aurora.scheduler.storage.AttributeStore;
import com.twitter.aurora.scheduler.storage.JobStore;
import com.twitter.aurora.scheduler.storage.LockManager;
import com.twitter.aurora.scheduler.storage.QuotaStore;
import com.twitter.aurora.scheduler.storage.SchedulerStore;
import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.aurora.scheduler.storage.TaskStore;
import com.twitter.aurora.scheduler.storage.UpdateStore;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.stats.Stats;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A storage implementation comprised of individual in-memory store implementations.
 * <p>
 * This storage has a global read-write lock, which is used when invoking
 * {@link #consistentRead(Work)} and {@link #write(MutateWork)}.  However, no locks are used at this
 * level for {@link #weaklyConsistentRead(Work)}. It is the responsibility of the
 * individual stores to ensure that read operations are thread-safe (optimally supporting
 * concurrency).  Store implementations may assume that all methods invoked on {@code Mutable}
 * store interfaces are protected by the global write lock, and thus invoked serially.
 */
public class MemStorage implements Storage {
  private final AtomicLong readLockWaitNanos = Stats.exportLong("read_lock_wait_nanos");
  private final AtomicLong writeLockWaitNanos = Stats.exportLong("write_lock_wait_nanos");

  private final MutableStoreProvider storeProvider;
  private final LockManager lockManager = new LockManager();

  @Inject
  MemStorage(
      final SchedulerStore.Mutable schedulerStore,
      final JobStore.Mutable jobStore,
      final TaskStore.Mutable taskStore,
      final UpdateStore.Mutable updateStore,
      final QuotaStore.Mutable quotaStore,
      final AttributeStore.Mutable attributeStore) {

    storeProvider = new MutableStoreProvider() {
      @Override public SchedulerStore.Mutable getSchedulerStore() {
        return schedulerStore;
      }

      @Override public JobStore.Mutable getJobStore() {
        return jobStore;
      }

      @Override public TaskStore getTaskStore() {
        return taskStore;
      }

      @Override public TaskStore.Mutable getUnsafeTaskStore() {
        return taskStore;
      }

      @Override public UpdateStore.Mutable getUpdateStore() {
        return updateStore;
      }

      @Override public QuotaStore.Mutable getQuotaStore() {
        return quotaStore;
      }

      @Override public AttributeStore.Mutable getAttributeStore() {
        return attributeStore;
      }
    };
  }

  /**
   * Creates a new empty in-memory storage for use in testing.
   */
  @VisibleForTesting
  public static MemStorage newEmptyStorage() {
    return new MemStorage(
        new MemSchedulerStore(),
        new MemJobStore(),
        new MemTaskStore(),
        new MemUpdateStore(),
        new MemQuotaStore(),
        new MemAttributeStore());
  }

  @Timed("mem_storage_consistent_read_operation")
  @Override
  public <T, E extends Exception> T consistentRead(Work<T, E> work) throws StorageException, E {
    checkNotNull(work);

    long lockStartNanos = System.nanoTime();
    boolean topLevelOperation = lockManager.readLock();
    if (topLevelOperation) {
      readLockWaitNanos.addAndGet(System.nanoTime() - lockStartNanos);
    }
    try {
      return work.apply(storeProvider);
    } finally {
      lockManager.readUnlock();
    }
  }

  @Timed("mem_storage_weakly_consistent_read_operation")
  @Override
  public <T, E extends Exception> T weaklyConsistentRead(Work<T, E> work)
      throws StorageException, E {

    return work.apply(storeProvider);
  }

  @Timed("mem_storage_write_operation")
  @Override
  public <T, E extends Exception> T write(MutateWork<T, E> work)
      throws StorageException, E {

    checkNotNull(work);

    long lockStartNanos = System.nanoTime();
    boolean topLevelOperation = lockManager.writeLock();
    if (topLevelOperation) {
      writeLockWaitNanos.addAndGet(System.nanoTime() - lockStartNanos);
    }
    try {
      return work.apply(storeProvider);
    } finally {
      lockManager.writeUnlock();
    }
  }

  @Override
  public void snapshot() {
    // No-op.
  }
}

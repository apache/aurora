package com.twitter.mesos.scheduler.storage.mem;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;

import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.stats.Stats;
import com.twitter.mesos.scheduler.storage.AttributeStore;
import com.twitter.mesos.scheduler.storage.JobStore;
import com.twitter.mesos.scheduler.storage.LockManager;
import com.twitter.mesos.scheduler.storage.QuotaStore;
import com.twitter.mesos.scheduler.storage.SchedulerStore;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.TaskStore;
import com.twitter.mesos.scheduler.storage.UpdateStore;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A storage implementation comprised of individual in-memory store implementations.
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

  @Timed("mem_storage_read_operation")
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
}

package com.twitter.mesos.scheduler.storage.mem;

import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;

import com.twitter.mesos.scheduler.storage.AttributeStore;
import com.twitter.mesos.scheduler.storage.AttributeStore.AttributeStoreImpl;
import com.twitter.mesos.scheduler.storage.JobStore;
import com.twitter.mesos.scheduler.storage.LockManager;
import com.twitter.mesos.scheduler.storage.QuotaStore;
import com.twitter.mesos.scheduler.storage.SchedulerStore;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork.NoResult.Quiet;
import com.twitter.mesos.scheduler.storage.TaskStore;
import com.twitter.mesos.scheduler.storage.UpdateStore;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A storage implementation comprised of individual in-memory store implementations.
 */
public class MemStorage implements Storage {
  private static final Logger LOG = Logger.getLogger(MemStorage.class.getName());

  private final MutableStoreProvider storeProvider;
  private final LockManager lockManager = new LockManager();

  /**
   * Creates a new empty in-memory storage.
   */
  @VisibleForTesting
  public MemStorage() {
    final SchedulerStore.Mutable schedulerStore = new MemSchedulerStore();
    final JobStore.Mutable jobStore = new MemJobStore();
    final TaskStore.Mutable taskStore = new MemTaskStore();
    final UpdateStore.Mutable updateStore = new MemUpdateStore();
    final QuotaStore.Mutable quotaStore = new MemQuotaStore();
    final AttributeStore.Mutable attributeStore = new AttributeStoreImpl();

    storeProvider = new MutableStoreProvider() {
      @Override public SchedulerStore.Mutable getSchedulerStore() {
        return schedulerStore;
      }

      @Override public JobStore.Mutable getJobStore() {
        return jobStore;
      }

      @Override public TaskStore.Mutable getTaskStore() {
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

  @Override
  public void prepare() {
    // No-op.
  }

  @Override
  public void start(final Quiet initializationLogic) {
    checkNotNull(initializationLogic);

    doInWriteTransaction(initializationLogic);
    LOG.info("Applied initialization logic.");
  }

  @Override
  public <T, E extends Exception> T doInTransaction(Work<T, E> work) throws StorageException, E {
    checkNotNull(work);

    lockManager.readLock();
    try {
      return work.apply(storeProvider);
    } finally {
      lockManager.readUnlock();
    }
  }

  @Override
  public <T, E extends Exception> T doInWriteTransaction(MutateWork<T, E> work)
      throws StorageException, E {

    checkNotNull(work);

    lockManager.writeLock();
    try {
      return work.apply(storeProvider);
    } finally {
      lockManager.writeUnlock();
    }
  }

  @Override
  public void stop() {
    // No-op.
  }
}

/**
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
package org.apache.aurora.scheduler.storage.durability;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import javax.inject.Inject;

import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.stats.SlidingStats;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.scheduler.base.SchedulerException;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.CronJobStore;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.SchedulerStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.Storage.NonVolatileStorage;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.durability.Persistence.PersistenceException;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * A storage implementation that ensures storage mutations are written to a persistence layer.
 *
 * <p>In the classic write-ahead log usage we'd perform mutations as follows:
 * <ol>
 *   <li>record op</li>
 *   <li>perform op locally</li>
 *   <li>persist ops</li>
 * </ol>
 *
 * <p>Writing the operation to persistences ensures we have a record of our mutation in case we
 * should need to recover state later after a crash or on a new host (assuming the scheduler is
 * distributed).  We then apply the mutation to a local (in-memory) data structure for serving fast
 * read requests.
 *
 * <p>This implementation leverages a local transaction to handle this:
 * <ol>
 *   <li>start local transaction</li>
 *   <li>perform op locally (uncommitted!)</li>
 *   <li>write op to persistence</li>
 * </ol>
 *
 * <p>If the op fails to apply to local storage we will never persist the op, and if the op
 * fails to persist, it'll throw and abort the local storage operation as well.
 */
public class DurableStorage implements NonVolatileStorage {

  /**
   * A maintainer for context about open transactions. Assumes that an external entity is
   * responsible for opening and closing transactions.
   */
  interface TransactionManager {

    /**
     * Checks whether there is an open transaction.
     *
     * @return {@code true} if there is an open transaction, {@code false} otherwise.
     */
    boolean hasActiveTransaction();

    /**
     * Adds an operation to the existing transaction.
     *
     * @param op Operation to include in the existing transaction.
     */
    void log(Op op);
  }

  private final Persistence persistence;
  private final Storage writeBehindStorage;
  private final ReentrantLock writeLock;
  private final ThriftBackfill thriftBackfill;

  private final WriteRecorder writeRecorder;

  private TransactionRecorder transaction = null;

  private final SlidingStats writerWaitStats = new SlidingStats("storage_write_lock_wait", "ns");

  @Inject
  DurableStorage(
      Persistence persistence,
      @Volatile Storage delegateStorage,
      @Volatile SchedulerStore.Mutable schedulerStore,
      @Volatile CronJobStore.Mutable jobStore,
      @Volatile TaskStore.Mutable taskStore,
      @Volatile QuotaStore.Mutable quotaStore,
      @Volatile AttributeStore.Mutable attributeStore,
      @Volatile JobUpdateStore.Mutable jobUpdateStore,
      EventSink eventSink,
      ReentrantLock writeLock,
      ThriftBackfill thriftBackfill) {

    this.persistence = requireNonNull(persistence);

    // DurableStorage has two distinct operating modes: pre- and post-recovery.  When recovering,
    // we write directly to the writeBehind stores since we are replaying what's already persisted.
    // After that, all writes must succeed in Persistence before they may be considered successful.
    this.writeBehindStorage = requireNonNull(delegateStorage);
    this.writeLock = requireNonNull(writeLock);
    this.thriftBackfill = requireNonNull(thriftBackfill);
    TransactionManager transactionManager = new TransactionManager() {
      @Override
      public boolean hasActiveTransaction() {
        return transaction != null;
      }

      @Override
      public void log(Op op) {
        transaction.add(op);
      }
    };
    this.writeRecorder = new WriteRecorder(
        transactionManager,
        schedulerStore,
        jobStore,
        taskStore,
        quotaStore,
        attributeStore,
        jobUpdateStore,
        LoggerFactory.getLogger(WriteRecorder.class),
        eventSink);
  }

  @Override
  @Timed("scheduler_storage_prepare")
  public synchronized void prepare() {
    writeBehindStorage.prepare();
    persistence.prepare();
  }

  @Override
  @Timed("scheduler_storage_start")
  public void start(final MutateWork.NoResult.Quiet initializationLogic) {
    writeLock.lock();
    try {
      // We recover directly into the forwarded system to avoid persisting replayed operations.
      writeBehindStorage.write((NoResult.Quiet) this::recover);

      // Now that we're recovered we should persist any mutations done in initializationLogic, so
      // run it in one of our transactions.
      write(initializationLogic);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void stop() {
    // No-op.
  }

  @Timed("scheduler_storage_recover")
  void recover(MutableStoreProvider stores) throws RecoveryFailedException {
    try {
      Loader.load(stores, thriftBackfill, persistence.recover());
    } catch (PersistenceException e) {
      throw new RecoveryFailedException(e);
    }
  }

  private static final class RecoveryFailedException extends SchedulerException {
    RecoveryFailedException(Throwable cause) {
      super(cause);
    }
  }

  private <T, E extends Exception> T doInTransaction(final MutateWork<T, E> work)
      throws StorageException, E {

    // The transaction has already been set up so we just need to delegate with our store provider
    // so any mutations may be persisted.
    if (transaction != null) {
      return work.apply(writeRecorder);
    }

    transaction = new TransactionRecorder();
    try {
      return writeBehindStorage.write(unused -> {
        T result = work.apply(writeRecorder);
        List<Op> ops = transaction.getOps();
        if (!ops.isEmpty()) {
          try {
            persistence.persist(ops.stream());
          } catch (PersistenceException e) {
            throw new StorageException("Failed to persist storage changes", e);
          }
        }
        return result;
      });
    } finally {
      transaction = null;
    }
  }

  @Override
  public <T, E extends Exception> T write(final MutateWork<T, E> work) throws StorageException, E {
    long waitStart = System.nanoTime();
    writeLock.lock();
    try {
      writerWaitStats.accumulate(System.nanoTime() - waitStart);
      return doInTransaction(work);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public <T, E extends Exception> T read(Work<T, E> work) throws StorageException, E {
    return writeBehindStorage.read(work);
  }
}

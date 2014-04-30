/**
 * Copyright 2013 Apache Software Foundation
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
package org.apache.aurora.scheduler.storage.log;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Date;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.BindingAnnotation;
import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.base.Closure;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.concurrent.ExecutorServiceShutdown;

import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.gen.storage.LogEntry;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.RewriteTask;
import org.apache.aurora.gen.storage.SaveAcceptedJob;
import org.apache.aurora.gen.storage.SaveQuota;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.base.SchedulerException;
import org.apache.aurora.scheduler.log.Log.Stream.InvalidPositionException;
import org.apache.aurora.scheduler.log.Log.Stream.StreamAccessException;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.DistributedSnapshotStore;
import org.apache.aurora.scheduler.storage.JobStore;
import org.apache.aurora.scheduler.storage.LockStore;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.SchedulerStore;
import org.apache.aurora.scheduler.storage.SnapshotStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.NonVolatileStorage;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.ILockKey;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.log.LogManager.StreamManager;
import org.apache.aurora.scheduler.storage.log.LogManager.StreamManager.StreamTransaction;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A storage implementation that ensures committed transactions are written to a log.
 *
 * <p>In the classic write-ahead log usage we'd perform mutations as follows:
 * <ol>
 *   <li>write op to log</li>
 *   <li>perform op locally</li>
 *   <li>*checkpoint</li>
 * </ol>
 *
 * <p>Writing the operation to the log provides us with a fast persistence mechanism to ensure we
 * have a record of our mutation in case we should need to recover state later after a crash or on
 * a new host (assuming the log is distributed).  We then apply the mutation to a local (in-memory)
 * data structure for serving fast read requests and then optionally write down the position of the
 * log entry we wrote in the first step to stable storage to allow for quicker recovery after a
 * crash. Instead of reading the whole log, we can read all entries past the checkpoint.  This
 * design implies that all mutations must be idempotent and free from constraint and thus
 * replayable over newer operations when recovering from an old checkpoint.
 *
 * <p>The important detail in our case is the possibility of writing an op to the log, and then
 * failing to commit locally since we use a local database instead of an in-memory data structure.
 * If we die after such a failure, then another instance can read and apply the logged op
 * erroneously.
 *
 * <p>This implementation leverages a local transaction to handle this:
 * <ol>
 *   <li>start local transaction</li>
 *   <li>perform op locally (uncommitted!)</li>
 *   <li>write op to log</li>
 *   <li>commit local transaction</li>
 *   <li>*checkpoint</li>
 * </ol>
 *
 * <p>If the op fails to apply to local storage we will never write the op to the log and if the op
 * fails to apply to the log, it'll throw and abort the local storage transaction as well.
 */
public class LogStorage implements NonVolatileStorage, DistributedSnapshotStore {

  /**
   * A service that can schedule an action to be executed periodically.
   */
  @VisibleForTesting
  interface SchedulingService {

    /**
     * Schedules an action to execute periodically.
     *
     * @param interval The time period to wait until running the {@code action} again.
     * @param action The action to execute periodically.
     */
    void doEvery(Amount<Long, Time> interval, Runnable action);
  }

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

  private static class ScheduledExecutorSchedulingService implements SchedulingService {
    private final ScheduledExecutorService scheduledExecutor;

    ScheduledExecutorSchedulingService(ShutdownRegistry shutdownRegistry,
        Amount<Long, Time> shutdownGracePeriod) {
      scheduledExecutor = AsyncUtil.singleThreadLoggingScheduledExecutor("LogStorage-%d", LOG);
      shutdownRegistry.addAction(
          new ExecutorServiceShutdown(scheduledExecutor, shutdownGracePeriod));
    }

    @Override
    public void doEvery(Amount<Long, Time> interval, Runnable action) {
      checkNotNull(interval);
      checkNotNull(action);

      long delay = interval.getValue();
      TimeUnit timeUnit = interval.getUnit().getTimeUnit();
      scheduledExecutor.scheduleWithFixedDelay(action, delay, delay, timeUnit);
    }
  }

  private static final Logger LOG = Logger.getLogger(LogStorage.class.getName());

  private final LogManager logManager;
  private final SchedulingService schedulingService;
  private final SnapshotStore<Snapshot> snapshotStore;
  private final Amount<Long, Time> snapshotInterval;
  private final Storage writeBehindStorage;
  private final SchedulerStore.Mutable writeBehindSchedulerStore;
  private final JobStore.Mutable writeBehindJobStore;
  private final TaskStore.Mutable writeBehindTaskStore;
  private final LockStore.Mutable writeBehindLockStore;
  private final QuotaStore.Mutable writeBehindQuotaStore;
  private final AttributeStore.Mutable writeBehindAttributeStore;

  private StreamManager streamManager;
  private final WriteAheadStorage writeAheadStorage;

  // TODO(wfarner): It should be possible to remove this flag now, since all call stacks when
  // recovering are controlled at this layer (they're all calls to Mutable store implementations).
  // The more involved change is changing SnapshotStore to accept a Mutable store provider to
  // avoid a call to Storage.write() when we replay a Snapshot.
  private boolean recovered = false;
  private StreamTransaction transaction = null;

  /**
   * Identifies the grace period to give in-process snapshots and checkpoints to complete during
   * shutdown.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.PARAMETER, ElementType.METHOD })
  @BindingAnnotation
  public @interface ShutdownGracePeriod { }

  /**
   * Identifies the interval between snapshots of local storage truncating the log.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.PARAMETER, ElementType.METHOD })
  @BindingAnnotation
  public @interface SnapshotInterval { }

  /**
   * Identifies a local storage layer that is written to only after first ensuring the write
   * operation is persisted in the log.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.PARAMETER, ElementType.METHOD })
  @BindingAnnotation
  public @interface WriteBehind { }

  @Inject
  LogStorage(LogManager logManager,
             ShutdownRegistry shutdownRegistry,
             @ShutdownGracePeriod Amount<Long, Time> shutdownGracePeriod,
             SnapshotStore<Snapshot> snapshotStore,
             @SnapshotInterval Amount<Long, Time> snapshotInterval,
             @WriteBehind Storage storage,
             @WriteBehind SchedulerStore.Mutable schedulerStore,
             @WriteBehind JobStore.Mutable jobStore,
             @WriteBehind TaskStore.Mutable taskStore,
             @WriteBehind LockStore.Mutable lockStore,
             @WriteBehind QuotaStore.Mutable quotaStore,
             @WriteBehind AttributeStore.Mutable attributeStore) {

    this(logManager,
        new ScheduledExecutorSchedulingService(shutdownRegistry, shutdownGracePeriod),
        snapshotStore,
        snapshotInterval,
        storage,
        schedulerStore,
        jobStore,
        taskStore,
        lockStore,
        quotaStore,
        attributeStore);
  }

  @VisibleForTesting
  LogStorage(LogManager logManager,
             SchedulingService schedulingService,
             SnapshotStore<Snapshot> snapshotStore,
             Amount<Long, Time> snapshotInterval,
             Storage delegateStorage,
             SchedulerStore.Mutable schedulerStore,
             JobStore.Mutable jobStore,
             TaskStore.Mutable taskStore,
             LockStore.Mutable lockStore,
             QuotaStore.Mutable quotaStore,
             AttributeStore.Mutable attributeStore) {

    this.logManager = checkNotNull(logManager);
    this.schedulingService = checkNotNull(schedulingService);
    this.snapshotStore = checkNotNull(snapshotStore);
    this.snapshotInterval = checkNotNull(snapshotInterval);

    // Log storage has two distinct operating modes: pre- and post-recovery.  When recovering,
    // we write directly to the writeBehind stores since we are replaying what's already persisted.
    // After that, all writes must succeed in the distributed log before they may be considered
    // successful.
    this.writeBehindStorage = checkNotNull(delegateStorage);
    this.writeBehindSchedulerStore = checkNotNull(schedulerStore);
    this.writeBehindJobStore = checkNotNull(jobStore);
    this.writeBehindTaskStore = checkNotNull(taskStore);
    this.writeBehindLockStore = checkNotNull(lockStore);
    this.writeBehindQuotaStore = checkNotNull(quotaStore);
    this.writeBehindAttributeStore = checkNotNull(attributeStore);
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
    this.writeAheadStorage = new WriteAheadStorage(
        transactionManager,
        schedulerStore,
        jobStore,
        taskStore,
        lockStore,
        quotaStore,
        attributeStore);
  }

  @Override
  public synchronized void prepare() {
    // Open the log to make a log replica available to the scheduler group.
    try {
      streamManager = logManager.open();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to open the log, cannot continue", e);
    }

    // TODO(John Sirois): start incremental recovery here from the log and do a final recovery
    // catchup in start after shutting down the incremental syncer.
  }

  @Override
  public synchronized void start(final MutateWork.NoResult.Quiet initializationLogic) {
    write(new MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(MutableStoreProvider unused) {
        // Must have the underlying storage started so we can query it for the last checkpoint.
        // We replay these entries in the forwarded storage system's transactions but not ours - we
        // do not want to re-record these ops to the log.
        recover();
        recovered = true;

        // Now that we're recovered we should let any mutations done in initializationLogic append
        // to the log, so run it in one of our transactions.
        write(initializationLogic);
      }
    });

    scheduleSnapshots();
  }

  @Override
  public void stop() {
    // No-op.
  }

  @Timed("scheduler_log_recover")
  void recover() throws RecoveryFailedException {
    try {
      streamManager.readFromBeginning(new Closure<LogEntry>() {
        @Override
        public void execute(LogEntry logEntry) {
          replay(logEntry);
        }
      });
    } catch (CodingException | InvalidPositionException | StreamAccessException e) {
      throw new RecoveryFailedException(e);
    }
  }

  private static final class RecoveryFailedException extends SchedulerException {
    private RecoveryFailedException(Throwable cause) {
      super(cause);
    }
  }

  void replay(final LogEntry logEntry) {
    switch (logEntry.getSetField()) {
      case SNAPSHOT:
        Snapshot snapshot = logEntry.getSnapshot();
        LOG.info("Applying snapshot taken on " + new Date(snapshot.getTimestamp()));
        snapshotStore.applySnapshot(snapshot);
        break;

      case TRANSACTION:
        write(new MutateWork.NoResult.Quiet() {
          @Override
          protected void execute(MutableStoreProvider unused) {
            for (Op op : logEntry.getTransaction().getOps()) {
              replayOp(op);
            }
          }
        });
        break;

      case NOOP:
        // Nothing to do here
        break;

      case DEFLATED_ENTRY:
        throw new IllegalArgumentException("Deflated entries are not handled at this layer.");

      case FRAME:
        throw new IllegalArgumentException("Framed entries are not handled at this layer.");

      default:
        throw new IllegalStateException("Unknown log entry type: " + logEntry);
    }
  }

  private void replayOp(Op op) {
    switch (op.getSetField()) {
      case SAVE_FRAMEWORK_ID:
        writeBehindSchedulerStore.saveFrameworkId(op.getSaveFrameworkId().getId());
        break;

      case SAVE_ACCEPTED_JOB:
        SaveAcceptedJob acceptedJob = op.getSaveAcceptedJob();
        writeBehindJobStore.saveAcceptedJob(
            acceptedJob.getManagerId(),
            IJobConfiguration.build(acceptedJob.getJobConfig()));
        break;

      case REMOVE_JOB:
        writeBehindJobStore.removeJob(IJobKey.build(op.getRemoveJob().getJobKey()));
        break;

      case SAVE_TASKS:
        writeBehindTaskStore.saveTasks(
            IScheduledTask.setFromBuilders(op.getSaveTasks().getTasks()));
        break;

      case REWRITE_TASK:
        RewriteTask rewriteTask = op.getRewriteTask();
        writeBehindTaskStore.unsafeModifyInPlace(
            rewriteTask.getTaskId(),
            ITaskConfig.build(rewriteTask.getTask()));
        break;

      case REMOVE_TASKS:
        writeBehindTaskStore.deleteTasks(op.getRemoveTasks().getTaskIds());
        break;

      case SAVE_QUOTA:
        SaveQuota saveQuota = op.getSaveQuota();
        writeBehindQuotaStore.saveQuota(
            saveQuota.getRole(),
            IResourceAggregate.build(saveQuota.getQuota()));
        break;

      case REMOVE_QUOTA:
        writeBehindQuotaStore.removeQuota(op.getRemoveQuota().getRole());
        break;

      case SAVE_HOST_ATTRIBUTES:
        writeBehindAttributeStore.saveHostAttributes(op.getSaveHostAttributes().hostAttributes);
        break;

      case SAVE_LOCK:
        writeBehindLockStore.saveLock(ILock.build(op.getSaveLock().getLock()));
        break;

      case REMOVE_LOCK:
        writeBehindLockStore.removeLock(ILockKey.build(op.getRemoveLock().getLockKey()));
        break;

      default:
        throw new IllegalStateException("Unknown transaction op: " + op);
    }
  }

  private void scheduleSnapshots() {
    if (snapshotInterval.getValue() > 0) {
      schedulingService.doEvery(snapshotInterval, new Runnable() {
        @Override
        public void run() {
          try {
            snapshot();
          } catch (StorageException e) {
            if (e.getCause() != null) {
              LOG.log(Level.WARNING, e.getMessage(), e.getCause());
            } else {
              LOG.log(Level.WARNING, "StorageException when attempting to snapshot.", e);
            }
          }
        }
      });
    }
  }

  /**
   * Forces a snapshot of the storage state.
   *
   * @throws CodingException If there is a problem encoding the snapshot.
   * @throws InvalidPositionException If the log stream cursor is invalid.
   * @throws StreamAccessException If there is a problem writing the snapshot to the log stream.
   */
  @Timed("scheduler_log_snapshot")
  void doSnapshot() throws CodingException, InvalidPositionException, StreamAccessException {
    write(new MutateWork.NoResult<CodingException>() {
      @Override
      protected void execute(MutableStoreProvider unused)
          throws CodingException, InvalidPositionException, StreamAccessException {

        LOG.info("Creating snapshot.");
        Snapshot snapshot = snapshotStore.createSnapshot();
        persist(snapshot);
        LOG.info("Snapshot complete."
                 + " host attrs: " + snapshot.getHostAttributesSize()
                 + ", jobs: " + snapshot.getJobsSize()
                 + ", locks: " + snapshot.getLocksSize()
                 + ", quota confs: " + snapshot.getQuotaConfigurationsSize()
                 + ", tasks: " + snapshot.getTasksSize());
      }
    });
  }

  @Timed("scheduler_log_snapshot_persist")
  @Override
  public void persist(Snapshot snapshot)
      throws CodingException, InvalidPositionException, StreamAccessException {

    streamManager.snapshot(snapshot);
  }

  @Override
  public synchronized <T, E extends Exception> T write(final MutateWork<T, E> work)
      throws StorageException, E {

    // We don't want to use the log when recovering from it, we just want to update the underlying
    // store - so pass mutations straight through to the underlying storage.
    if (!recovered) {
      return writeBehindStorage.write(work);
    }

    // The log stream transaction has already been set up so we just need to delegate with our
    // store provider so any mutations performed by work get logged.
    if (transaction != null) {
      return work.apply(writeAheadStorage);
    }

    transaction = streamManager.startTransaction();
    try {
      return writeBehindStorage.write(new MutateWork<T, E>() {
        @Override
        public T apply(MutableStoreProvider unused) throws E {
          T result = work.apply(writeAheadStorage);
          try {
            transaction.commit();
          } catch (CodingException e) {
            throw new IllegalStateException(
                "Problem encoding transaction operations to the log stream", e);
          } catch (StreamAccessException e) {
            throw new StorageException(
                "There was a problem committing the transaction to the log.", e);
          }
          return result;
        }
      });
    } finally {
      transaction = null;
    }
  }

  @Override
  public <T, E extends Exception> T consistentRead(Work<T, E> work) throws StorageException, E {
    return writeBehindStorage.consistentRead(work);
  }

  @Override
  public <T, E extends Exception> T weaklyConsistentRead(Work<T, E> work)
      throws StorageException, E {

    return writeBehindStorage.weaklyConsistentRead(work);
  }

  @Override
  public void snapshot() throws StorageException {
    try {
      doSnapshot();
    } catch (CodingException e) {
      throw new StorageException("Failed to encode a snapshot", e);
    } catch (InvalidPositionException e) {
      throw new StorageException("Saved snapshot but failed to truncate entries preceding it", e);
    } catch (StreamAccessException e) {
      throw new StorageException("Failed to create a snapshot", e);
    }
  }
}

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
package org.apache.aurora.scheduler.storage.log;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.common.application.ShutdownRegistry;
import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.SlidingStats;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.storage.LogEntry;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.SaveCronJob;
import org.apache.aurora.gen.storage.SaveJobInstanceUpdateEvent;
import org.apache.aurora.gen.storage.SaveJobUpdateEvent;
import org.apache.aurora.gen.storage.SaveQuota;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.base.SchedulerException;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.log.Log.Stream.InvalidPositionException;
import org.apache.aurora.scheduler.log.Log.Stream.StreamAccessException;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.CronJobStore;
import org.apache.aurora.scheduler.storage.DistributedSnapshotStore;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.SchedulerStore;
import org.apache.aurora.scheduler.storage.SnapshotStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.Storage.NonVolatileStorage;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

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
      shutdownRegistry.addAction(() -> MoreExecutors.shutdownAndAwaitTermination(
          scheduledExecutor,
          shutdownGracePeriod.getValue(),
          shutdownGracePeriod.getUnit().getTimeUnit()));
    }

    @Override
    public void doEvery(Amount<Long, Time> interval, Runnable action) {
      requireNonNull(interval);
      requireNonNull(action);

      long delay = interval.getValue();
      TimeUnit timeUnit = interval.getUnit().getTimeUnit();
      scheduledExecutor.scheduleWithFixedDelay(action, delay, delay, timeUnit);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(LogStorage.class);

  private final LogManager logManager;
  private final SchedulingService schedulingService;
  private final SnapshotStore<Snapshot> snapshotStore;
  private final Amount<Long, Time> snapshotInterval;
  private final Storage writeBehindStorage;
  private final SchedulerStore.Mutable writeBehindSchedulerStore;
  private final CronJobStore.Mutable writeBehindJobStore;
  private final TaskStore.Mutable writeBehindTaskStore;
  private final QuotaStore.Mutable writeBehindQuotaStore;
  private final AttributeStore.Mutable writeBehindAttributeStore;
  private final JobUpdateStore.Mutable writeBehindJobUpdateStore;
  private final ReentrantLock writeLock;
  private final ThriftBackfill thriftBackfill;

  private StreamManager streamManager;
  private final WriteAheadStorage writeAheadStorage;

  // TODO(wfarner): It should be possible to remove this flag now, since all call stacks when
  // recovering are controlled at this layer (they're all calls to Mutable store implementations).
  // The more involved change is changing SnapshotStore to accept a Mutable store provider to
  // avoid a call to Storage.write() when we replay a Snapshot.
  private boolean recovered = false;
  private StreamTransaction transaction = null;

  private final SlidingStats writerWaitStats =
      new SlidingStats("log_storage_write_lock_wait", "ns");

  private final Map<LogEntry._Fields, Consumer<LogEntry>> logEntryReplayActions;
  private final Map<Op._Fields, Consumer<Op>> transactionReplayActions;

  @Inject
  LogStorage(
      LogManager logManager,
      ShutdownRegistry shutdownRegistry,
      Settings settings,
      SnapshotStore<Snapshot> snapshotStore,
      @Volatile Storage storage,
      @Volatile SchedulerStore.Mutable schedulerStore,
      @Volatile CronJobStore.Mutable jobStore,
      @Volatile TaskStore.Mutable taskStore,
      @Volatile QuotaStore.Mutable quotaStore,
      @Volatile AttributeStore.Mutable attributeStore,
      @Volatile JobUpdateStore.Mutable jobUpdateStore,
      EventSink eventSink,
      ReentrantLock writeLock,
      ThriftBackfill thriftBackfill) {

    this(logManager,
        new ScheduledExecutorSchedulingService(shutdownRegistry, settings.getShutdownGracePeriod()),
        snapshotStore,
        settings.getSnapshotInterval(),
        storage,
        schedulerStore,
        jobStore,
        taskStore,
        quotaStore,
        attributeStore,
        jobUpdateStore,
        eventSink,
        writeLock,
        thriftBackfill);
  }

  @VisibleForTesting
  LogStorage(
      LogManager logManager,
      SchedulingService schedulingService,
      SnapshotStore<Snapshot> snapshotStore,
      Amount<Long, Time> snapshotInterval,
      Storage delegateStorage,
      SchedulerStore.Mutable schedulerStore,
      CronJobStore.Mutable jobStore,
      TaskStore.Mutable taskStore,
      QuotaStore.Mutable quotaStore,
      AttributeStore.Mutable attributeStore,
      JobUpdateStore.Mutable jobUpdateStore,
      EventSink eventSink,
      ReentrantLock writeLock,
      ThriftBackfill thriftBackfill) {

    this.logManager = requireNonNull(logManager);
    this.schedulingService = requireNonNull(schedulingService);
    this.snapshotStore = requireNonNull(snapshotStore);
    this.snapshotInterval = requireNonNull(snapshotInterval);

    // Log storage has two distinct operating modes: pre- and post-recovery.  When recovering,
    // we write directly to the writeBehind stores since we are replaying what's already persisted.
    // After that, all writes must succeed in the distributed log before they may be considered
    // successful.
    this.writeBehindStorage = requireNonNull(delegateStorage);
    this.writeBehindSchedulerStore = requireNonNull(schedulerStore);
    this.writeBehindJobStore = requireNonNull(jobStore);
    this.writeBehindTaskStore = requireNonNull(taskStore);
    this.writeBehindQuotaStore = requireNonNull(quotaStore);
    this.writeBehindAttributeStore = requireNonNull(attributeStore);
    this.writeBehindJobUpdateStore = requireNonNull(jobUpdateStore);
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
    this.writeAheadStorage = new WriteAheadStorage(
        transactionManager,
        schedulerStore,
        jobStore,
        taskStore,
        quotaStore,
        attributeStore,
        jobUpdateStore,
        LoggerFactory.getLogger(WriteAheadStorage.class),
        eventSink);

    this.logEntryReplayActions = buildLogEntryReplayActions();
    this.transactionReplayActions = buildTransactionReplayActions();
  }

  @VisibleForTesting
  final Map<LogEntry._Fields, Consumer<LogEntry>> buildLogEntryReplayActions() {
    return ImmutableMap.<LogEntry._Fields, Consumer<LogEntry>>builder()
        .put(LogEntry._Fields.SNAPSHOT, logEntry -> {
          Snapshot snapshot = logEntry.getSnapshot();
          LOG.info("Applying snapshot taken on " + new Date(snapshot.getTimestamp()));
          snapshotStore.applySnapshot(snapshot);
        })
        .put(LogEntry._Fields.TRANSACTION, logEntry -> write((NoResult.Quiet) unused -> {
          for (Op op : logEntry.getTransaction().getOps()) {
            replayOp(op);
          }
        }))
        .put(LogEntry._Fields.NOOP, item -> {
          // Nothing to do here
        })
        .build();
  }

  @VisibleForTesting
  final Map<Op._Fields, Consumer<Op>> buildTransactionReplayActions() {
    return ImmutableMap.<Op._Fields, Consumer<Op>>builder()
        .put(
            Op._Fields.SAVE_FRAMEWORK_ID,
            op -> writeBehindSchedulerStore.saveFrameworkId(op.getSaveFrameworkId().getId()))
        .put(Op._Fields.SAVE_CRON_JOB, op -> {
          SaveCronJob cronJob = op.getSaveCronJob();
          writeBehindJobStore.saveAcceptedJob(
              thriftBackfill.backfillJobConfiguration(cronJob.getJobConfig()));
        })
        .put(
            Op._Fields.REMOVE_JOB,
            op -> writeBehindJobStore.removeJob(IJobKey.build(op.getRemoveJob().getJobKey())))
        .put(
            Op._Fields.SAVE_TASKS,
            op -> writeBehindTaskStore.saveTasks(
                thriftBackfill.backfillTasks(op.getSaveTasks().getTasks())))
        .put(
            Op._Fields.REMOVE_TASKS,
            op -> writeBehindTaskStore.deleteTasks(op.getRemoveTasks().getTaskIds()))
        .put(Op._Fields.SAVE_QUOTA, op -> {
          SaveQuota saveQuota = op.getSaveQuota();
          writeBehindQuotaStore.saveQuota(
              saveQuota.getRole(),
              ThriftBackfill.backfillResourceAggregate(saveQuota.getQuota()));
        })
        .put(
            Op._Fields.REMOVE_QUOTA,
            op -> writeBehindQuotaStore.removeQuota(op.getRemoveQuota().getRole()))
        .put(Op._Fields.SAVE_HOST_ATTRIBUTES, op -> {
          HostAttributes attributes = op.getSaveHostAttributes().getHostAttributes();
          // Prior to commit 5cf760b, the store would persist maintenance mode changes for
          // unknown hosts.  5cf760b began rejecting these, but the replicated log may still
          // contain entries with a null slave ID.
          if (attributes.isSetSlaveId()) {
            writeBehindAttributeStore.saveHostAttributes(IHostAttributes.build(attributes));
          } else {
            LOG.info("Dropping host attributes with no agent ID: " + attributes);
          }
        })
        .put(Op._Fields.SAVE_JOB_UPDATE, op ->
          writeBehindJobUpdateStore.saveJobUpdate(
              thriftBackfill.backFillJobUpdate(op.getSaveJobUpdate().getJobUpdate())))
        .put(Op._Fields.SAVE_JOB_UPDATE_EVENT, op -> {
          SaveJobUpdateEvent event = op.getSaveJobUpdateEvent();
          writeBehindJobUpdateStore.saveJobUpdateEvent(
              IJobUpdateKey.build(event.getKey()),
              IJobUpdateEvent.build(op.getSaveJobUpdateEvent().getEvent()));
        })
        .put(Op._Fields.SAVE_JOB_INSTANCE_UPDATE_EVENT, op -> {
          SaveJobInstanceUpdateEvent event = op.getSaveJobInstanceUpdateEvent();
          writeBehindJobUpdateStore.saveJobInstanceUpdateEvent(
              IJobUpdateKey.build(event.getKey()),
              IJobInstanceUpdateEvent.build(op.getSaveJobInstanceUpdateEvent().getEvent()));
        })
        .put(Op._Fields.PRUNE_JOB_UPDATE_HISTORY, op -> {
          LOG.info("Dropping prune operation.  Updates will be pruned later.");
        })
        .put(Op._Fields.REMOVE_JOB_UPDATE, op ->
          writeBehindJobUpdateStore.removeJobUpdates(
              IJobUpdateKey.setFromBuilders(op.getRemoveJobUpdate().getKeys())))
        .build();
  }

  @Override
  @Timed("scheduler_storage_prepare")
  public synchronized void prepare() {
    writeBehindStorage.prepare();
    // Open the log to make a log replica available to the scheduler group.
    try {
      streamManager = logManager.open();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to open the log, cannot continue", e);
    }
  }

  @Override
  @Timed("scheduler_storage_start")
  public synchronized void start(final MutateWork.NoResult.Quiet initializationLogic) {
    write((NoResult.Quiet) unused -> {
      // Must have the underlying storage started so we can query it for the last checkpoint.
      // We replay these entries in the forwarded storage system's transactions but not ours - we
      // do not want to re-record these ops to the log.
      recover();
      recovered = true;

      // Now that we're recovered we should let any mutations done in initializationLogic append
      // to the log, so run it in one of our transactions.
      write(initializationLogic);
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
      streamManager.readFromBeginning(LogStorage.this::replay);
    } catch (CodingException | InvalidPositionException | StreamAccessException e) {
      throw new RecoveryFailedException(e);
    }
  }

  private static final class RecoveryFailedException extends SchedulerException {
    RecoveryFailedException(Throwable cause) {
      super(cause);
    }
  }

  private void replay(final LogEntry logEntry) {
    LogEntry._Fields entryField = logEntry.getSetField();
    if (!logEntryReplayActions.containsKey(entryField)) {
      throw new IllegalStateException("Unknown log entry type: " + entryField);
    }

    logEntryReplayActions.get(entryField).accept(logEntry);
  }

  private void replayOp(Op op) {
    Op._Fields opField = op.getSetField();
    if (!transactionReplayActions.containsKey(opField)) {
      throw new IllegalStateException("Unknown transaction op: " + opField);
    }

    transactionReplayActions.get(opField).accept(op);
  }

  private void scheduleSnapshots() {
    if (snapshotInterval.getValue() > 0) {
      schedulingService.doEvery(snapshotInterval, () -> {
        try {
          snapshot();
        } catch (StorageException e) {
          if (e.getCause() == null) {
            LOG.warn("StorageException when attempting to snapshot.", e);
          } else {
            LOG.warn(e.getMessage(), e.getCause());
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
    write((NoResult<CodingException>) (MutableStoreProvider unused) -> {
      LOG.info("Creating snapshot.");
      Snapshot snapshot = snapshotStore.createSnapshot();
      persist(snapshot);
      LOG.info("Snapshot complete."
          + " host attrs: " + snapshot.getHostAttributesSize()
          + ", cron jobs: " + snapshot.getCronJobsSize()
          + ", quota confs: " + snapshot.getQuotaConfigurationsSize()
          + ", tasks: " + snapshot.getTasksSize()
          + ", updates: " + snapshot.getJobUpdateDetailsSize());
    });
  }

  @Timed("scheduler_log_snapshot_persist")
  @Override
  public void persist(Snapshot snapshot)
      throws CodingException, InvalidPositionException, StreamAccessException {

    streamManager.snapshot(snapshot);
  }

  private <T, E extends Exception> T doInTransaction(final MutateWork<T, E> work)
      throws StorageException, E {

    // The log stream transaction has already been set up so we just need to delegate with our
    // store provider so any mutations performed by work get logged.
    if (transaction != null) {
      return work.apply(writeAheadStorage);
    }

    transaction = streamManager.startTransaction();
    try {
      return writeBehindStorage.write(unused -> {
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
      // We don't want to use the log when recovering from it, we just want to update the underlying
      // store - so pass mutations straight through to the underlying storage.
      if (!recovered) {
        return writeBehindStorage.write(work);
      }

      return doInTransaction(work);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public <T, E extends Exception> T read(Work<T, E> work) throws StorageException, E {
    return writeBehindStorage.read(work);
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

  /**
   * Configuration settings for log storage.
   */
  public static class Settings {
    private final Amount<Long, Time> shutdownGracePeriod;
    private final Amount<Long, Time> snapshotInterval;

    public Settings(Amount<Long, Time> shutdownGracePeriod, Amount<Long, Time> snapshotInterval) {
      this.shutdownGracePeriod = requireNonNull(shutdownGracePeriod);
      this.snapshotInterval = requireNonNull(snapshotInterval);
    }

    public Amount<Long, Time> getShutdownGracePeriod() {
      return shutdownGracePeriod;
    }

    public Amount<Long, Time> getSnapshotInterval() {
      return snapshotInterval;
    }
  }
}

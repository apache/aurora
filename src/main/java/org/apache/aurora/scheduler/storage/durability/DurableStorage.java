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
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.stats.SlidingStats;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.SaveCronJob;
import org.apache.aurora.gen.storage.SaveJobInstanceUpdateEvent;
import org.apache.aurora.gen.storage.SaveJobUpdateEvent;
import org.apache.aurora.gen.storage.SaveQuota;
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
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.slf4j.Logger;
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

  private static final Logger LOG = LoggerFactory.getLogger(DurableStorage.class);

  private final Persistence persistence;
  private final Storage writeBehindStorage;
  private final SchedulerStore.Mutable writeBehindSchedulerStore;
  private final CronJobStore.Mutable writeBehindJobStore;
  private final TaskStore.Mutable writeBehindTaskStore;
  private final QuotaStore.Mutable writeBehindQuotaStore;
  private final AttributeStore.Mutable writeBehindAttributeStore;
  private final JobUpdateStore.Mutable writeBehindJobUpdateStore;
  private final ReentrantLock writeLock;
  private final ThriftBackfill thriftBackfill;

  private final WriteAheadStorage writeAheadStorage;

  // TODO(wfarner): It should be possible to remove this flag now, since all call stacks when
  // recovering are controlled at this layer (they're all calls to Mutable store implementations).
  // The more involved change is changing SnapshotStore to accept a Mutable store provider to
  // avoid a call to Storage.write() when we replay a Snapshot.
  private boolean recovered = false;
  private TransactionRecorder transaction = null;

  private final SlidingStats writerWaitStats = new SlidingStats("storage_write_lock_wait", "ns");

  private final Map<Op._Fields, Consumer<Op>> transactionReplayActions;

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

    this.transactionReplayActions = buildTransactionReplayActions();
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
          // unknown hosts.  5cf760b began rejecting these, but the storage may still
          // contain entries with a null slave ID.
          if (attributes.isSetSlaveId()) {
            writeBehindAttributeStore.saveHostAttributes(IHostAttributes.build(attributes));
          } else {
            LOG.info("Dropping host attributes with no agent ID: " + attributes);
          }
        })
        .put(
            Op._Fields.SAVE_LOCK, // TODO(jly): Deprecated, remove in 0.21. See AURORA-1959.
            op -> { /* no-op */ })
        .put(
            Op._Fields.REMOVE_LOCK, // TODO(jly): Deprecated, remove in 0.21. See AURORA-1959.
            op -> { /* no-op */ })
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
    persistence.prepare();
  }

  @Override
  @Timed("scheduler_storage_start")
  public synchronized void start(final MutateWork.NoResult.Quiet initializationLogic) {
    write((NoResult.Quiet) unused -> {
      // Must have the underlying storage started so we can query it.
      // We replay these entries in the forwarded storage system's transactions but not ours - we
      // do not want to re-record these ops.
      recover();
      recovered = true;

      // Now that we're recovered we should persist any mutations done in initializationLogic, so
      // run it in one of our transactions.
      write(initializationLogic);
    });
  }

  @Override
  public void stop() {
    // No-op.
  }

  @Timed("scheduler_storage_recover")
  void recover() throws RecoveryFailedException {
    try {
      persistence.recover().forEach(DurableStorage.this::replayOp);
    } catch (PersistenceException e) {
      throw new RecoveryFailedException(e);
    }
  }

  private static final class RecoveryFailedException extends SchedulerException {
    RecoveryFailedException(Throwable cause) {
      super(cause);
    }
  }

  private void replayOp(Op op) {
    Op._Fields opField = op.getSetField();
    if (!transactionReplayActions.containsKey(opField)) {
      throw new IllegalStateException("Unknown transaction op: " + opField);
    }

    transactionReplayActions.get(opField).accept(op);
  }

  private <T, E extends Exception> T doInTransaction(final MutateWork<T, E> work)
      throws StorageException, E {

    // The transaction has already been set up so we just need to delegate with our store provider
    // so any mutations may be persisted.
    if (transaction != null) {
      return work.apply(writeAheadStorage);
    }

    transaction = new TransactionRecorder();
    try {
      return writeBehindStorage.write(unused -> {
        T result = work.apply(writeAheadStorage);
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
      // We don't want to persist when recovering, we just want to update the underlying
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
}

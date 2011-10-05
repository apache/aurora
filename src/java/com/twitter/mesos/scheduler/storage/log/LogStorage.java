package com.twitter.mesos.scheduler.storage.log;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;

import org.apache.commons.codec.binary.Hex;

import com.twitter.common.application.ActionRegistry;
import com.twitter.common.application.ShutdownStage;
import com.twitter.common.base.Closure;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.Clock;
import com.twitter.common.util.concurrent.ExecutorServiceShutdown;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.storage.LogEntry;
import com.twitter.mesos.gen.storage.Op;
import com.twitter.mesos.gen.storage.RemoveJob;
import com.twitter.mesos.gen.storage.RemoveJobUpdate;
import com.twitter.mesos.gen.storage.RemoveQuota;
import com.twitter.mesos.gen.storage.RemoveTasks;
import com.twitter.mesos.gen.storage.SaveAcceptedJob;
import com.twitter.mesos.gen.storage.SaveFrameworkId;
import com.twitter.mesos.gen.storage.SaveJobUpdate;
import com.twitter.mesos.gen.storage.SaveQuota;
import com.twitter.mesos.gen.storage.SaveTasks;
import com.twitter.mesos.gen.storage.Snapshot;
import com.twitter.mesos.gen.storage.TaskUpdateConfiguration;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.log.Log.Position;
import com.twitter.mesos.scheduler.log.Log.Stream.InvalidPositionException;
import com.twitter.mesos.scheduler.log.Log.Stream.StreamAccessException;
import com.twitter.mesos.scheduler.storage.CheckpointStore;
import com.twitter.mesos.scheduler.storage.ForwardingStore;
import com.twitter.mesos.scheduler.storage.JobStore;
import com.twitter.mesos.scheduler.storage.QuotaStore;
import com.twitter.mesos.scheduler.storage.SchedulerStore;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.TaskStore;
import com.twitter.mesos.scheduler.storage.UpdateStore;
import com.twitter.mesos.scheduler.storage.log.LogManager.StreamManager;
import com.twitter.mesos.scheduler.storage.log.LogManager.StreamManager.StreamTransaction;

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
 *
 * @author John Sirois
 */
public class LogStorage extends ForwardingStore {


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

  private static class ScheduledExecutorSchedulingService implements SchedulingService {
    private final ScheduledExecutorService scheduledExecutor;

    ScheduledExecutorSchedulingService(ActionRegistry shutdownRegistry,
        Amount<Long, Time> shutdownGracePeriod) {
      scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
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
  private final Clock clock;
  private final SchedulingService schedulingService;
  private final CheckpointStore checkpointStore;
  private final Amount<Long, Time> snapshotInterval;
  private final Amount<Long, Time> checkpointInterval;

  private StreamManager streamManager;

  /**
   * Identifies the grace period to give in-process snapshots and checkpoints to complete during
   * shutdown.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.PARAMETER, ElementType.METHOD})
  @BindingAnnotation
  public @interface ShutdownGracePeriod {}

  /**
   * Identifies the interval between checkpoints of the log position to local storage.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.PARAMETER, ElementType.METHOD})
  @BindingAnnotation
  public @interface CheckpointInterval {}

  /**
   * Identifies the interval between snapshots of local storage truncating the log.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.PARAMETER, ElementType.METHOD})
  @BindingAnnotation
  public @interface SnapshotInterval {}

  /**
   * Identifies a local storage layer that is written to only after first ensuring the write
   * operation is persisted in the log.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.PARAMETER, ElementType.METHOD})
  @BindingAnnotation
  public @interface WriteBehind {}

  @Inject
  LogStorage(LogManager logManager,
      Clock clock,
      @ShutdownStage ActionRegistry shutdownRegistry,
      @ShutdownGracePeriod Amount<Long, Time> shutdownGracePeriod,
      CheckpointStore checkpointStore,
      @CheckpointInterval Amount<Long, Time> checkpointInterval,
      @SnapshotInterval Amount<Long, Time> snapshotInterval,
      @WriteBehind Storage storage,
      @WriteBehind SchedulerStore schedulerStore,
      @WriteBehind JobStore jobStore,
      @WriteBehind TaskStore taskStore,
      @WriteBehind UpdateStore updateStore,
      @WriteBehind QuotaStore quotaStore) {

    this(logManager, clock,
        new ScheduledExecutorSchedulingService(shutdownRegistry, shutdownGracePeriod),
        checkpointStore, checkpointInterval, snapshotInterval, storage, schedulerStore, jobStore,
        taskStore, updateStore, quotaStore);
  }

  @VisibleForTesting
  LogStorage(LogManager logManager,
      Clock clock,
      SchedulingService schedulingService,
      CheckpointStore checkpointStore,
      Amount<Long, Time> checkpointInterval,
      Amount<Long, Time> snapshotInterval,
      Storage storage,
      SchedulerStore schedulerStore,
      JobStore jobStore,
      TaskStore taskStore,
      UpdateStore updateStore,
      QuotaStore quotaStore) {

    super(storage, schedulerStore, jobStore, taskStore, updateStore, quotaStore);
    this.logManager = checkNotNull(logManager);
    this.clock = checkNotNull(clock);
    this.schedulingService = checkNotNull(schedulingService);
    this.checkpointStore = checkNotNull(checkpointStore);
    this.checkpointInterval = checkNotNull(checkpointInterval);
    this.snapshotInterval = checkNotNull(snapshotInterval);
  }

  private boolean recovered = false;

  @Override
  public synchronized void start(final Work.NoResult.Quiet initilizationLogic) {
    try {
      streamManager = logManager.open();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to open the log, cannot continue", e);
    }

    super.start(new Work.NoResult.Quiet() {
      @Override protected void execute(StoreProvider unused) {
        // Must have the underlying storage started so we can query it for the last checkpoint.
        // We replay these entries in the forwarded storage system's transactions but not ours - we
        // do not want to re-record these ops to the log.
        checkpoint(recover());
        recovered = true;

        // Now that we're recovered we should let any mutations done in initializationLogic append
        // to the log, so run it in one of our transactions.
        doInTransaction(initilizationLogic);
      }
    });

    scheduleCheckpoints();
    scheduleSnapshots();
  }

  @Timed("scheduler_log_recover")
  @Nullable
  Position recover() throws RecoveryFailedException {
    @Nullable byte[] checkpoint = checkpointStore.fetchCheckpoint();
    try {
      return recoverFrom(checkpoint);
    } catch (RecoveryFailedException e) {
      if (checkpoint != null) {
        LOG.log(Level.WARNING,
            String.format("Failed to recover from checkpoint: 0x%s, attempting to recover using " +
                          "complete log.", Hex.encodeHexString(checkpoint)),
            e);
        return recoverFrom(null);
      }
      throw e;
    }
  }

  private static class RecoveryFailedException extends RuntimeException {
    private RecoveryFailedException(Throwable cause) {
      super(cause);
    }
  }

  private Position recoverFrom(@Nullable byte[] checkpoint) throws RecoveryFailedException {
    try {
      return streamManager.readAfter(checkpoint, new Closure<LogEntry>() {
        @Override public void execute(LogEntry logEntry) {
          replay(logEntry);
        }
      });
    } catch (CodingException e) {
      throw new RecoveryFailedException(e);
    } catch (InvalidPositionException e) {
      throw new RecoveryFailedException(e);
    } catch (StreamAccessException e) {
      throw new RecoveryFailedException(e);
    }
  }

  void replay(final LogEntry logEntry) {
    switch (logEntry.getSetField()) {
      case SNAPSHOT:
        Snapshot snapshot = logEntry.getSnapshot();
        LOG.info("Applying snapshot taken on " + new Date(snapshot.getTimestamp()));
        checkpointStore.applySnapshot(snapshot.getData());
        break;

      case TRANSACTION:
        for (Op op : logEntry.getTransaction().getOps()) {
          replayOp(op);
        }
        break;

      default:
        throw new IllegalStateException("Unknown log entry type: " + logEntry);
    }
  }

  private void replayOp(Op op) {
    switch (op.getSetField()) {
      case SAVE_FRAMEWORK_ID:
        saveFrameworkId(op.getSaveFrameworkId().getId());
        break;

      case SAVE_ACCEPTED_JOB:
        SaveAcceptedJob acceptedJob = op.getSaveAcceptedJob();
        saveAcceptedJob(acceptedJob.getManagerId(), acceptedJob.getJobConfig());
        break;

      case SAVE_JOB_UPDATE:
        SaveJobUpdate jobUpdate = op.getSaveJobUpdate();
        saveShardUpdateConfigs(jobUpdate.getRole(), jobUpdate.getJob(), jobUpdate.getUpdateToken(),
            jobUpdate.getDelta());
        break;

      case REMOVE_JOB_UPDATE:
        RemoveJobUpdate removeJob = op.getRemoveJobUpdate();
        removeShardUpdateConfigs(removeJob.getRole(), removeJob.getJob());
        break;

      case REMOVE_JOB:
        removeJob(op.getRemoveJob().getJobKey());
        break;

      case SAVE_TASKS:
        saveTasks(op.getSaveTasks().getTasks());
        break;

      case REMOVE_TASKS:
        removeTasks(op.getRemoveTasks().getTaskIds());
        break;

      case SAVE_QUOTA:
        SaveQuota saveQuota = op.getSaveQuota();
        saveQuota(saveQuota.getRole(), saveQuota.getQuota());
        break;

      case REMOVE_QUOTA:
        removeQuota(op.getRemoveQuota().getRole());
        break;

      default:
        throw new IllegalStateException("Unknown transaction op: " + op);
    }
  }

  // We use a single slot queue to allow the emitter side to be unaware of period policy, offered
  // checkpoints will just be silently dropped while the 1st in line awaits storage.
  private final BlockingQueue<Position> checkpoints = new ArrayBlockingQueue<Position>(1);

  @VisibleForTesting
  void checkpoint(@Nullable Position position) {
    if (position != null) {
      checkpoints.offer(position);
    }
  }

  private void scheduleCheckpoints() {
    if (checkpointInterval.getValue() > 0) {
      schedulingService.doEvery(checkpointInterval, new Runnable() {
        @Override public void run() {
          acceptCheckpoint();
        }
      });
    }
  }

  @VisibleForTesting
  void acceptCheckpoint() {
    try {
      checkpointStore.checkpoint(checkpoints.take().identity());
    } catch (InterruptedException e) {
      LOG.warning("Interrupted while waiting to accepting the next checkpoint");
      Thread.currentThread().interrupt();
    }
  }

  private void scheduleSnapshots() {
    if (snapshotInterval.getValue() > 0) {
      schedulingService.doEvery(snapshotInterval, new Runnable() {
        @Override public void run() {
          try {
            snapshot();
          } catch (CodingException e) {
            LOG.log(Level.WARNING, "Failed to encode a snapshot", e);
          } catch (InvalidPositionException e) {
            LOG.log(Level.WARNING, "Saved snapshot but failed to truncate entries preceding it", e);
          } catch (StreamAccessException e) {
            LOG.log(Level.WARNING, "Failed to create a snapshot", e);
          }
        }
      });
    }
  }

  @VisibleForTesting
  void snapshot() throws CodingException, InvalidPositionException, StreamAccessException {
    super.doInTransaction(new Work.NoResult<CodingException>() {
      @Override protected void execute(StoreProvider unused)
          throws CodingException, InvalidPositionException, StreamAccessException {

        long timestamp = clock.nowMillis();
        byte[] data = checkpointStore.createSnapshot();
        streamManager.snapshot(new Snapshot(timestamp, ByteBuffer.wrap(data)));
        LOG.info("Snapshot taken consuming " + Amount.of(data.length, Data.BYTES));
      }
    });
  }

  private StreamTransaction transaction = null;
  private final StoreProvider logStoreProvider = new StoreProvider() {
    @Override public SchedulerStore getSchedulerStore() {
      return LogStorage.this;
    }

    @Override public JobStore getJobStore() {
      return LogStorage.this;
    }

    @Override public TaskStore getTaskStore() {
      return LogStorage.this;
    }

    @Override public UpdateStore getUpdateStore() {
      return LogStorage.this;
    }

    @Override public QuotaStore getQuotaStore() {
      return LogStorage.this;
    }
  };

  @Override
  public synchronized <T, E extends Exception> T doInTransaction(final Work<T, E> work)
      throws StorageException, E {

    // The log stream transaction has already been setup or is not desired - nothing to do here
    if (!recovered || (transaction != null)) {
      return super.doInTransaction(work);
    }

    transaction = streamManager.startTransaction();
    try {
      return super.doInTransaction(new Work<T, E>() {
        @Override public T apply(StoreProvider unused) throws E {
          T result = work.apply(logStoreProvider);
          try {
            checkpoint(transaction.commit());
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
  public void saveFrameworkId(final String frameworkId) {
    doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(StoreProvider unused) {
        log(Op.saveFrameworkId(new SaveFrameworkId(frameworkId)));
        LogStorage.super.saveFrameworkId(frameworkId);
      }
    });
  }

  @Override
  public void saveAcceptedJob(final String managerId, final JobConfiguration jobConfig) {
    doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(StoreProvider unused) {
        log(Op.saveAcceptedJob(new SaveAcceptedJob(managerId, jobConfig)));
        LogStorage.super.saveAcceptedJob(managerId, jobConfig);
      }
    });
  }

  @Override
  public void removeJob(final String jobKey) {
    doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(StoreProvider unused) {
        log(Op.removeJob(new RemoveJob(jobKey)));
        LogStorage.super.removeJob(jobKey);
      }
    });
  }

  @Override
  public void saveTasks(final Set<ScheduledTask> newTasks) throws IllegalStateException {
    doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(StoreProvider unused) {
        log(Op.saveTasks(new SaveTasks(newTasks)));
        LogStorage.super.saveTasks(newTasks);
      }
    });
  }

  @Override
  public void removeTasks(final Query query) {
    doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(StoreProvider storeProvider) {
        // TODO(John Sirois): this forces an id fetch whereas DbStorage skips a fetch when it can
        // doing DELETE FROM WHERE ... perhaps this is the best we can do.
        removeTasks(storeProvider.getTaskStore().fetchTaskIds(query));
      }
    });
  }

  @Override
  public void removeTasks(final Set<String> taskIds) {
    doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(StoreProvider unused) {
        log(Op.removeTasks(new RemoveTasks(taskIds)));
        LogStorage.super.removeTasks(taskIds);
      }
    });
  }

  @Override
  public ImmutableSet<ScheduledTask> mutateTasks(final Query query,
      final Closure<ScheduledTask> mutator) {
    return doInTransaction(new Work.Quiet<ImmutableSet<ScheduledTask>>() {
      @Override public ImmutableSet<ScheduledTask> apply(StoreProvider unused) {
        ImmutableSet<ScheduledTask> mutated = LogStorage.super.mutateTasks(query, mutator);
        log(Op.saveTasks(new SaveTasks(mutated)));
        return mutated;
      }
    });
  }

  @Override
  public void saveShardUpdateConfigs(final String role, final String job, final String updateToken,
      final Set<TaskUpdateConfiguration> delta) {
    doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(StoreProvider unused) {
        log(Op.saveJobUpdate(new SaveJobUpdate(role, job, updateToken, delta)));
        LogStorage.super.saveShardUpdateConfigs(role, job, updateToken, delta);
      }
    });
  }

  @Override
  public void removeShardUpdateConfigs(final String role, final String job) {
    doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(StoreProvider unused) {
        log(Op.removeJobUpdate(new RemoveJobUpdate(role, job)));
        LogStorage.super.removeShardUpdateConfigs(role, job);
      }
    });
  }

  @Override
  public void removeQuota(final String role) {
    doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(StoreProvider unused) {
        log(Op.removeQuota(new RemoveQuota(role)));
        LogStorage.super.removeQuota(role);
      }
    });
  }

  @Override
  public void saveQuota(final String role, final Quota quota) {
    doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(StoreProvider unused) {
        log(Op.saveQuota(new SaveQuota(role, quota)));
        LogStorage.super.saveQuota(role, quota);
      }
    });
  }

  private void log(Op op) {
    if (recovered) {
      transaction.add(op);
    }
  }
}

package com.twitter.mesos.scheduler.storage.log;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;

import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.base.Closure;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.concurrent.ExecutorServiceShutdown;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.gen.HostAttributes;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.JobKey;
import com.twitter.mesos.gen.JobUpdateConfiguration;
import com.twitter.mesos.gen.MaintenanceMode;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.storage.LogEntry;
import com.twitter.mesos.gen.storage.Op;
import com.twitter.mesos.gen.storage.RemoveJob;
import com.twitter.mesos.gen.storage.RemoveJobUpdate;
import com.twitter.mesos.gen.storage.RemoveQuota;
import com.twitter.mesos.gen.storage.RemoveTasks;
import com.twitter.mesos.gen.storage.SaveAcceptedJob;
import com.twitter.mesos.gen.storage.SaveFrameworkId;
import com.twitter.mesos.gen.storage.SaveHostAttributes;
import com.twitter.mesos.gen.storage.SaveJobUpdate;
import com.twitter.mesos.gen.storage.SaveQuota;
import com.twitter.mesos.gen.storage.SaveTasks;
import com.twitter.mesos.gen.storage.Snapshot;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.SchedulerException;
import com.twitter.mesos.scheduler.log.Log.Stream.InvalidPositionException;
import com.twitter.mesos.scheduler.log.Log.Stream.StreamAccessException;
import com.twitter.mesos.scheduler.storage.AttributeStore;
import com.twitter.mesos.scheduler.storage.DistributedSnapshotStore;
import com.twitter.mesos.scheduler.storage.ForwardingStore;
import com.twitter.mesos.scheduler.storage.JobStore;
import com.twitter.mesos.scheduler.storage.QuotaStore;
import com.twitter.mesos.scheduler.storage.SchedulerStore;
import com.twitter.mesos.scheduler.storage.SnapshotStore;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.NonVolatileStorage;
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
 */
public class LogStorage extends ForwardingStore
    implements NonVolatileStorage, DistributedSnapshotStore {

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

    ScheduledExecutorSchedulingService(ShutdownRegistry shutdownRegistry,
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
  private final SchedulingService schedulingService;
  private final SnapshotStore<Snapshot> snapshotStore;
  private final Amount<Long, Time> snapshotInterval;

  private StreamManager streamManager;

  private boolean recovered = false;
  private StreamTransaction transaction = null;

  private final MutableStoreProvider logStoreProvider = new MutableStoreProvider() {
    @Override public SchedulerStore.Mutable getSchedulerStore() {
      return LogStorage.this;
    }

    @Override public JobStore.Mutable getJobStore() {
      return LogStorage.this;
    }

    @Override public TaskStore getTaskStore() {
      return LogStorage.this;
    }

    @Override public TaskStore.Mutable getUnsafeTaskStore() {
      return LogStorage.this;
    }

    @Override public UpdateStore.Mutable getUpdateStore() {
      return LogStorage.this;
    }

    @Override public QuotaStore.Mutable getQuotaStore() {
      return LogStorage.this;
    }

    @Override public AttributeStore.Mutable getAttributeStore() {
      return LogStorage.this;
    }
  };

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
             @WriteBehind UpdateStore.Mutable updateStore,
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
        updateStore,
        quotaStore,
        attributeStore);
  }

  @VisibleForTesting
  LogStorage(LogManager logManager,
             SchedulingService schedulingService,
             SnapshotStore<Snapshot> snapshotStore,
             Amount<Long, Time> snapshotInterval,
             Storage storage,
             SchedulerStore.Mutable schedulerStore,
             JobStore.Mutable jobStore,
             TaskStore.Mutable taskStore,
             UpdateStore.Mutable updateStore,
             QuotaStore.Mutable quotaStore,
             AttributeStore.Mutable attributeStore) {

    super(storage, schedulerStore, jobStore, taskStore, updateStore, quotaStore, attributeStore);
    this.logManager = checkNotNull(logManager);
    this.schedulingService = checkNotNull(schedulingService);
    this.snapshotStore = checkNotNull(snapshotStore);
    this.snapshotInterval = checkNotNull(snapshotInterval);
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
      @Override protected void execute(MutableStoreProvider unused) {
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
        for (Op op : logEntry.getTransaction().getOps()) {
          replayOp(op);
        }
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
        saveFrameworkId(op.getSaveFrameworkId().getId());
        break;

      case SAVE_ACCEPTED_JOB:
        SaveAcceptedJob acceptedJob = op.getSaveAcceptedJob();
        saveAcceptedJob(acceptedJob.getManagerId(), acceptedJob.getJobConfig());
        break;

      case SAVE_JOB_UPDATE:
        SaveJobUpdate jobUpdate = op.getSaveJobUpdate();
        saveJobUpdateConfig(new JobUpdateConfiguration(
            jobUpdate.getRole(),
            jobUpdate.getJob(),
            jobUpdate.getUpdateToken(),
            jobUpdate.getConfigs()
        ));
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
        deleteTasks(op.getRemoveTasks().getTaskIds());
        break;

      case SAVE_QUOTA:
        SaveQuota saveQuota = op.getSaveQuota();
        saveQuota(saveQuota.getRole(), saveQuota.getQuota());
        break;

      case REMOVE_QUOTA:
        removeQuota(op.getRemoveQuota().getRole());
        break;

      case SAVE_HOST_ATTRIBUTES:
        saveHostAttributes(op.getSaveHostAttributes().hostAttributes);
        break;

      default:
        throw new IllegalStateException("Unknown transaction op: " + op);
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

  /**
   * Forces a snapshot of the storage state.
   *
   * @throws CodingException If there is a problem encoding the snapshot.
   * @throws InvalidPositionException If the log stream cursor is invalid.
   * @throws StreamAccessException If there is a problem writing the snapshot to the log stream.
   */
  @Timed("scheduler_log_snapshot")
  public void snapshot() throws CodingException, InvalidPositionException, StreamAccessException {
    super.write(new MutateWork.NoResult<CodingException>() {
      @Override protected void execute(MutableStoreProvider unused)
          throws CodingException, InvalidPositionException, StreamAccessException {

        persist(snapshotStore.createSnapshot());
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
      return super.write(work);
    }

    // The log stream transaction has already been set up so we just need to delegate with our
    // store provider so any mutations performed by work get logged.
    if (transaction != null) {
      return super.write(new MutateWork<T, E>() {
        @Override public T apply(MutableStoreProvider unused) throws E {
          return work.apply(logStoreProvider);
        }
      });
    }

    transaction = streamManager.startTransaction();
    try {
      return super.write(new MutateWork<T, E>() {
        @Override public T apply(MutableStoreProvider unused) throws E {
          T result = work.apply(logStoreProvider);
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

  @Timed("scheduler_log_save_framework_id")
  @Override
  public void saveFrameworkId(final String frameworkId) {
    write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider unused) {
        log(Op.saveFrameworkId(new SaveFrameworkId(frameworkId)));
        LogStorage.super.saveFrameworkId(frameworkId);
      }
    });
  }

  @Timed("scheduler_log_job_save")
  @Override
  public void saveAcceptedJob(final String managerId, final JobConfiguration jobConfig) {
    write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider unused) {
        log(Op.saveAcceptedJob(new SaveAcceptedJob(managerId, jobConfig)));
        LogStorage.super.saveAcceptedJob(managerId, jobConfig);
      }
    });
  }

  @Timed("scheduler_log_job_remove")
  @Override
  public void removeJob(final String jobKey) {
    write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider unused) {
        log(Op.removeJob(new RemoveJob(jobKey)));
        LogStorage.super.removeJob(jobKey);
      }
    });
  }

  @Override
  public void removeJob(JobKey jobKey) {
    // TODO(ksweeney): Remove this delegation as part of MESOS-2403.
    removeJob(Tasks.jobKey(jobKey));
  }

  @Timed("scheduler_log_tasks_save")
  @Override
  public void saveTasks(final Set<ScheduledTask> newTasks) throws IllegalStateException {
    write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider unused) {
        log(Op.saveTasks(new SaveTasks(newTasks)));
        LogStorage.super.saveTasks(newTasks);
      }
    });
  }

  @Override
  public void deleteAllTasks() {
    write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        deleteTasks(storeProvider.getTaskStore().fetchTaskIds(Query.unscoped()));
      }
    });
  }

  @Timed("scheduler_log_tasks_remove")
  @Override
  public void deleteTasks(final Set<String> taskIds) {
    write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider unused) {
        log(Op.removeTasks(new RemoveTasks(taskIds)));
        LogStorage.super.deleteTasks(taskIds);
      }
    });
  }

  @Timed("scheduler_log_tasks_mutate")
  @Override
  public ImmutableSet<ScheduledTask> mutateTasks(final TaskQuery query,
      final Closure<ScheduledTask> mutator) {
    return write(new MutateWork.Quiet<ImmutableSet<ScheduledTask>>() {
      @Override public ImmutableSet<ScheduledTask> apply(MutableStoreProvider unused) {
        ImmutableSet<ScheduledTask> mutated = LogStorage.super.mutateTasks(query, mutator);

        Map<String, ScheduledTask> tasksById = Tasks.mapById(mutated);
        if (LOG.isLoggable(Level.FINE)) {
          LOG.fine("Storing updated tasks to log: "
              + Maps.transformValues(tasksById, Tasks.GET_STATUS));
        }

        // TODO(William Farner): Avoid writing an op when mutated is empty.
        log(Op.saveTasks(new SaveTasks(mutated)));
        return mutated;
      }
    });
  }

  @Timed("scheduler_log_jobupdate_save")
  @Override
  public void saveJobUpdateConfig(final JobUpdateConfiguration configs) {
    write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider unused) {
        log(Op.saveJobUpdate(new SaveJobUpdate(
            configs.getRole(),
            configs.getJob(),
            configs.getUpdateToken(),
            configs.getConfigs())));
        LogStorage.super.saveJobUpdateConfig(configs);
      }
    });
  }

  @Timed("scheduler_log_jobupdate_remove")
  @Override
  public void removeShardUpdateConfigs(final String role, final String job) {
    write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider unused) {
        log(Op.removeJobUpdate(new RemoveJobUpdate(role, job)));
        LogStorage.super.removeShardUpdateConfigs(role, job);
      }
    });
  }

  @Timed("scheduler_log_quota_remove")
  @Override
  public void removeQuota(final String role) {
    write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider unused) {
        log(Op.removeQuota(new RemoveQuota(role)));
        LogStorage.super.removeQuota(role);
      }
    });
  }

  @Timed("scheduler_log_quota_save")
  @Override
  public void saveQuota(final String role, final Quota quota) {
    write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider unused) {
        log(Op.saveQuota(new SaveQuota(role, quota)));
        LogStorage.super.saveQuota(role, quota);
      }
    });
  }

  @Timed("scheduler_save_host_attribute")
  @Override
  public void saveHostAttributes(final HostAttributes attrs) {
    write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider unused) {
        // Pass the updated attributes upstream, and then check if the stored value changes.
        // We do this since different parts of the system write partial HostAttributes objects
        // and they are merged together internally.
        // TODO(William Farner): Split out a separate method
        //                       saveAttributes(String host, Iterable<Attributes>) to simplify this.
        Optional<HostAttributes> saved = LogStorage.super.getHostAttributes(attrs.getHost());
        LogStorage.super.saveHostAttributes(attrs);
        Optional<HostAttributes> updated = LogStorage.super.getHostAttributes(attrs.getHost());
        if (!saved.equals(updated)) {
          log(Op.saveHostAttributes(new SaveHostAttributes(updated.get())));
        }
      }
    });
  }

  @Override
  public boolean setMaintenanceMode(final String host, final MaintenanceMode mode) {
    write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider unused) {
        Optional<HostAttributes> saved = LogStorage.super.getHostAttributes(host);
        if (saved.isPresent()) {
          HostAttributes attributes = saved.get().setMode(mode);
          log(Op.saveHostAttributes(new SaveHostAttributes(attributes)));
          LogStorage.super.saveHostAttributes(attributes);
        }
      }
    });
    return false;
  }

  private void log(Op op) {
    if (recovered) {
      transaction.add(op);
    }
  }
}

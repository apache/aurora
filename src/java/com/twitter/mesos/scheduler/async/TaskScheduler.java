package com.twitter.mesos.scheduler.async;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;

import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.TaskInfo;

import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.base.Command;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.BackoffStrategy;
import com.twitter.common.util.concurrent.ExecutorServiceShutdown;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.StateManager;
import com.twitter.mesos.scheduler.TaskAssigner;
import com.twitter.mesos.scheduler.events.PubsubEvent.EventSubscriber;
import com.twitter.mesos.scheduler.events.PubsubEvent.StorageStarted;
import com.twitter.mesos.scheduler.events.PubsubEvent.TaskStateChange;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;

/**
 * An asynchronous task scheduler.  Scheduling of tasks is performed on a delay, where each task
 * backs off after a failed scheduling attempt.
 */
public interface TaskScheduler extends EventSubscriber {

  /**
   * A task scheduler that learns of pending tasks via internal pubsub notifications.
   * <p>
   * Additionally, it will automatically return resource offers after a configurable amount of time.
   */
  class TaskSchedulerImpl implements TaskScheduler {

    private static final Logger LOG = Logger.getLogger(TaskSchedulerImpl.class.getName());

    private final Storage storage;
    private final StateManager stateManager;
    private final TaskAssigner assigner;
    private final BackoffStrategy pendingRetryStrategy;
    private final ScheduledExecutorService executor;
    private final OfferQueue offerQueue;

    private final AtomicLong scheduleAttemptsFired = Stats.exportLong("schedule_attempts_fired");
    private final AtomicLong scheduleAttemptsFailed = Stats.exportLong("schedule_attempts_failed");

    @VisibleForTesting
    TaskSchedulerImpl(
        Storage storage,
        StateManager stateManager,
        TaskAssigner assigner,
        BackoffStrategy pendingRetryStrategy,
        ScheduledExecutorService executor,
        OfferQueue offerQueue) {

      this.storage = checkNotNull(storage);
      this.stateManager = checkNotNull(stateManager);
      this.assigner = checkNotNull(assigner);
      this.pendingRetryStrategy = checkNotNull(pendingRetryStrategy);
      this.executor = checkNotNull(executor);
      this.offerQueue = checkNotNull(offerQueue);
    }

    @Inject
    TaskSchedulerImpl(
        Storage storage,
        StateManager stateManager,
        TaskAssigner assigner,
        BackoffStrategy pendingRetryStrategy,
        OfferQueue offerQueue,
        ShutdownRegistry shutdownRegistry) {

      this(
          storage,
          stateManager,
          assigner,
          pendingRetryStrategy,
          createThreadPool(shutdownRegistry),
          offerQueue);
    }

    private static ScheduledExecutorService createThreadPool(ShutdownRegistry shutdownRegistry) {
      // TODO(William Farner): Extract a utility function that does this and overrides
      // afterExecute, otherwise uncaught exceptions are swallowed.
      // However, be careful doing this with periodic tasks (e.g. scheduleAtFixedRate), as
      // Future.get() will block forever on them.
      final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(
          1,
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat("TaskScheduler-%d").build());
      Stats.exportSize("schedule_queue_size", executor.getQueue());
      shutdownRegistry.addAction(new Command() {
        @Override public void execute() {
          new ExecutorServiceShutdown(executor, Amount.of(1L, Time.SECONDS)).execute();
        }
      });
      return executor;
    }

    private void addTask(String taskId, long delayMillis) {
      LOG.fine("Evaluating task " + taskId + " in " + delayMillis + " ms");
      executor.schedule(new PendingTask(taskId, delayMillis), delayMillis, TimeUnit.MILLISECONDS);
    }

    @Subscribe
    public void taskChangedState(TaskStateChange stateChange) {
      // TODO(William Farner): Use the ancestry of a task to check if the task is flapping, use
      // this to implement scheduling backoff (inducing a longer delay).
      String taskId = stateChange.getTaskId();
      if (stateChange.getNewState() == PENDING) {
        addTask(taskId, pendingRetryStrategy.calculateBackoffMs(0));
      }
    }

    @Subscribe
    public void storageStarted(StorageStarted event) {
      // TODO(William Farner): This being separate from TaskStateChange events is asking for a bug
      // where a subscriber forgets to handle storage start.
      // Wrap this concept into TaskStateChange.
      storage.doInTransaction(new Work.Quiet<Void>() {
        @Override public Void apply(StoreProvider store) {
          for (ScheduledTask task : store.getTaskStore().fetchTasks(Query.byStatus(PENDING))) {
            addTask(Tasks.id(task), pendingRetryStrategy.calculateBackoffMs(0));
          }
          return null;
        }
      });
    }

    @VisibleForTesting
    static final Optional<String> LAUNCH_FAILED_MSG =
        Optional.of("Unknown exception attempting to schedule task.");

    /**
     * A helper method that attempts to schedule a PENDING task. This method is package private
     * to satisfy @Timed annotation's requirement.
     *
     * @param taskId The task to schedule.
     * @param store Scheduler storage interface.
     * @param retryLater A command to execute when a task is not scheduled.
     */
    @Timed("task_schedule_attempt")
    void tryScheduling(String taskId, StoreProvider store, Command retryLater) {
      LOG.fine("Attempting to schedule task " + taskId);
      TaskQuery pendingTaskQuery = Query.byId(taskId).setStatuses(ImmutableSet.of(PENDING));
      final ScheduledTask task =
          Iterables.getOnlyElement(store.getTaskStore().fetchTasks(pendingTaskQuery), null);
      if (task == null) {
        LOG.warning("Failed to look up task " + taskId + ", it may have been deleted.");
      } else {
        Function<Offer, Optional<TaskInfo>> assignment =
            new Function<Offer, Optional<TaskInfo>>() {
              @Override public Optional<TaskInfo> apply(Offer offer) {
                return assigner.maybeAssign(offer, task);
              }
            };
        try {
          if (!offerQueue.launchFirst(assignment)) {
            // Task could not be scheduled.
            retryLater.execute();
          }
        } catch (OfferQueue.LaunchException e) {
          LOG.log(Level.WARNING, "Failed to launch task.", e);
          scheduleAttemptsFailed.incrementAndGet();

          // The attempt to schedule the task failed, so we need to backpedal on the assignment.
          // It is in the LOST state and a new task will move to PENDING to replace it.
          // Should the state change fail due to storage issues, that's okay.  The task will
          // time out in the ASSIGNED state and be moved to LOST.
          stateManager.changeState(pendingTaskQuery, LOST, LAUNCH_FAILED_MSG);
        }
      }
    }

    private class PendingTask implements Runnable {
      final String taskId;
      volatile long penaltyMs;

      PendingTask(String taskId, long penaltyMs) {
        this.taskId = taskId;
        this.penaltyMs = penaltyMs;
      }

      @Override public void run() {
        scheduleAttemptsFired.incrementAndGet();
        try {
          storage.doInWriteTransaction(new MutateWork.NoResult.Quiet() {
            @Override public void execute(MutableStoreProvider store) {
              tryScheduling(taskId, store, new Command() {
                @Override public void execute() {
                  retryLater();
                }
              });
            }
          });
        } catch (RuntimeException e) {
          // We catch the generic unchecked exception here to ensure tasks are not abandoned
          // if there is a transient issue resulting in an unchecked exception.
          LOG.log(Level.WARNING, "Task scheduling unexpectedly failed, will be retried", e);
          scheduleAttemptsFailed.incrementAndGet();
          retryLater();
        }
      }

      private void retryLater() {
        penaltyMs = pendingRetryStrategy.calculateBackoffMs(penaltyMs);
        LOG.fine("Re-evaluating " + taskId + " in " + penaltyMs + " ms.");
        executor.schedule(this, penaltyMs, TimeUnit.MILLISECONDS);
      }
    }
  }
}

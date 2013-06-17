package com.twitter.mesos.scheduler.async;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.TaskInfo;

import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.stats.Stats;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.StateManager;
import com.twitter.mesos.scheduler.TaskAssigner;
import com.twitter.mesos.scheduler.async.TaskGroups.SchedulingAction;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;

/**
 * An asynchronous task scheduler.  Scheduling of tasks is performed on a delay, where each task
 * backs off after a failed scheduling attempt.
 * <p>
 * Pending tasks are advertised to the scheduler via internal pubsub notifications.
 */
class TaskScheduler implements SchedulingAction {

  private static final Logger LOG = Logger.getLogger(TaskScheduler.class.getName());

  private final Storage storage;
  private final StateManager stateManager;
  private final TaskAssigner assigner;
  private final OfferQueue offerQueue;

  private final AtomicLong scheduleAttemptsFired = Stats.exportLong("schedule_attempts_fired");
  private final AtomicLong scheduleAttemptsFailed = Stats.exportLong("schedule_attempts_failed");

  @Inject
  TaskScheduler(
      Storage storage,
      StateManager stateManager,
      TaskAssigner assigner,
      OfferQueue offerQueue) {

    this.storage = checkNotNull(storage);
    this.stateManager = checkNotNull(stateManager);
    this.assigner = checkNotNull(assigner);
    this.offerQueue = checkNotNull(offerQueue);
  }

  @VisibleForTesting
  static final Optional<String> LAUNCH_FAILED_MSG =
      Optional.of("Unknown exception attempting to schedule task.");

  @Timed("task_schedule_attempt")
  @Override
  public boolean schedule(final String taskId) {
    scheduleAttemptsFired.incrementAndGet();
    try {
      return storage.write(new MutateWork.Quiet<Boolean>() {
        @Override public Boolean apply(MutableStoreProvider store) {
          LOG.fine("Attempting to schedule task " + taskId);
          Query.Builder pendingTaskQuery = Query.taskScoped(taskId).byStatus(PENDING);
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
                return false;
              }
            } catch (OfferQueue.LaunchException e) {
              LOG.log(Level.WARNING, "Failed to launch task.", e);
              scheduleAttemptsFailed.incrementAndGet();

              // The attempt to schedule the task failed, so we need to backpedal on the assignment.
              // It is in the LOST state and a new task will move to PENDING to replace it.
              // Should the state change fail due to storage issues, that's okay.  The task will
              // time out in the ASSIGNED state and be moved to LOST.
              stateManager.changeState(pendingTaskQuery.get(), LOST, LAUNCH_FAILED_MSG);
            }
          }

          return true;
        }
      });
    } catch (RuntimeException e) {
      // We catch the generic unchecked exception here to ensure tasks are not abandoned
      // if there is a transient issue resulting in an unchecked exception.
      LOG.log(Level.WARNING, "Task scheduling unexpectedly failed, will be retried", e);
      scheduleAttemptsFailed.incrementAndGet();
      return false;
    }
  }
}

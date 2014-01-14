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
package org.apache.aurora.scheduler.async;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.Subscribe;
import com.google.inject.BindingAnnotation;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.StatImpl;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.Clock;

import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.state.TaskAssigner;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskInfo;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import static com.google.common.base.Preconditions.checkNotNull;

import static org.apache.aurora.gen.ScheduleStatus.LOST;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;

/**
 * Enables scheduling and preemption of tasks.
 */
interface TaskScheduler extends EventSubscriber {

  /**
   * Attempts to schedule a task, possibly performing irreversible actions.
   *
   * @param taskId The task to attempt to schedule.
   * @return SUCCESS if the task was scheduled, TRY_AGAIN otherwise. The caller should call schedule
   * again if TRY_AGAIN is returned.
   */
  TaskSchedulerResult schedule(String taskId);

  enum TaskSchedulerResult {
    SUCCESS,
    TRY_AGAIN
  }

  /**
   * An asynchronous task scheduler.  Scheduling of tasks is performed on a delay, where each task
   * backs off after a failed scheduling attempt.
   * <p>
   * Pending tasks are advertised to the scheduler via internal pubsub notifications.
   */
  class TaskSchedulerImpl implements TaskScheduler {
    /**
     * Binding annotation for the time duration of reservations
     */
    @BindingAnnotation
    @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
    @interface ReservationDuration { }

    private static final Logger LOG = Logger.getLogger(TaskSchedulerImpl.class.getName());

    private final Storage storage;
    private final StateManager stateManager;
    private final TaskAssigner assigner;
    private final OfferQueue offerQueue;
    private final Preemptor preemptor;
    private final Reservations reservations;

    private final AtomicLong scheduleAttemptsFired = Stats.exportLong("schedule_attempts_fired");
    private final AtomicLong scheduleAttemptsFailed = Stats.exportLong("schedule_attempts_failed");

    @Inject
    TaskSchedulerImpl(
        Storage storage,
        StateManager stateManager,
        TaskAssigner assigner,
        OfferQueue offerQueue,
        Preemptor preemptor,
        @ReservationDuration Amount<Long, Time> reservationDuration,
        final Clock clock) {

      this.storage = checkNotNull(storage);
      this.stateManager = checkNotNull(stateManager);
      this.assigner = checkNotNull(assigner);
      this.offerQueue = checkNotNull(offerQueue);
      this.preemptor = checkNotNull(preemptor);
      this.reservations = new Reservations(reservationDuration, clock);
    }

    private Function<Offer, Optional<TaskInfo>> getAssignerFunction(
        final String taskId,
        final IScheduledTask task) {

      return new Function<Offer, Optional<TaskInfo>>() {
        @Override public Optional<TaskInfo> apply(Offer offer) {
          Optional<String> reservedTaskId = reservations.getSlaveReservation(offer.getSlaveId());
          if (reservedTaskId.isPresent()) {
            if (taskId.equals(reservedTaskId.get())) {
              // Slave is reserved to satisfy this task.
              return assigner.maybeAssign(offer, task);
            } else {
              // Slave is reserved for another task.
              return Optional.absent();
            }
          } else {
            // Slave is not reserved.
            return assigner.maybeAssign(offer, task);
          }
        }
      };
    }

    @VisibleForTesting
    static final Optional<String> LAUNCH_FAILED_MSG =
        Optional.of("Unknown exception attempting to schedule task.");

    @Timed("task_schedule_attempt")
    @Override
    public TaskSchedulerResult schedule(final String taskId) {
      scheduleAttemptsFired.incrementAndGet();
      try {
        return storage.write(new MutateWork.Quiet<TaskSchedulerResult>() {
          @Override public TaskSchedulerResult apply(MutableStoreProvider store) {
            LOG.fine("Attempting to schedule task " + taskId);
            final IScheduledTask task = Iterables.getOnlyElement(
                store.getTaskStore().fetchTasks(Query.taskScoped(taskId).byStatus(PENDING)),
                null);
            if (task == null) {
              LOG.warning("Failed to look up task " + taskId + ", it may have been deleted.");
            } else {
              try {
                if (!offerQueue.launchFirst(getAssignerFunction(taskId, task))) {
                  // Task could not be scheduled.
                  maybePreemptFor(taskId);
                  return TaskSchedulerResult.TRY_AGAIN;
                }
              } catch (OfferQueue.LaunchException e) {
                LOG.log(Level.WARNING, "Failed to launch task.", e);
                scheduleAttemptsFailed.incrementAndGet();

                // The attempt to schedule the task failed, so we need to backpedal on the
                // assignment.
                // It is in the LOST state and a new task will move to PENDING to replace it.
                // Should the state change fail due to storage issues, that's okay.  The task will
                // time out in the ASSIGNED state and be moved to LOST.
                stateManager.changeState(taskId, Optional.of(PENDING), LOST, LAUNCH_FAILED_MSG);
              }
            }

            return TaskSchedulerResult.SUCCESS;
          }
        });
      } catch (RuntimeException e) {
        // We catch the generic unchecked exception here to ensure tasks are not abandoned
        // if there is a transient issue resulting in an unchecked exception.
        LOG.log(Level.WARNING, "Task scheduling unexpectedly failed, will be retried", e);
        scheduleAttemptsFailed.incrementAndGet();
        return TaskSchedulerResult.TRY_AGAIN;
      }
    }

    private void maybePreemptFor(String taskId) {
      if (reservations.hasReservationForTask(taskId)) {
        return;
      }
      Optional<String> slaveId = preemptor.findPreemptionSlotFor(taskId);
      if (slaveId.isPresent()) {
        this.reservations.add(SlaveID.newBuilder().setValue(slaveId.get()).build(), taskId);
      }
    }

    @Subscribe
    public void taskChanged(final TaskStateChange stateChangeEvent) {
      if (Optional.of(PENDING).equals(stateChangeEvent.getOldState())) {
        reservations.invalidateTask(stateChangeEvent.getTaskId());
      }
    }

    private static class Reservations {
      private final Cache<SlaveID, String> reservations;

      Reservations(final Amount<Long, Time> duration, final Clock clock) {
        checkNotNull(duration);
        checkNotNull(clock);
        this.reservations = CacheBuilder.newBuilder()
            .expireAfterWrite(duration.as(Time.MINUTES), TimeUnit.MINUTES)
            .ticker(new Ticker() {
              @Override public long read() {
                return clock.nowNanos();
              }
            })
            .build();
        Stats.export(new StatImpl<Long>("reservation_cache_size") {
          @Override public Long read() {
            return reservations.size();
          }
        });
      }

      private synchronized void add(SlaveID slaveId, String taskId) {
        reservations.put(slaveId, taskId);
      }

      private synchronized boolean hasReservationForTask(String taskId) {
        return reservations.asMap().containsValue(taskId);
      }

      private synchronized Optional<String> getSlaveReservation(SlaveID slaveID) {
        return Optional.fromNullable(reservations.getIfPresent(slaveID));
      }

      private synchronized void invalidateTask(String taskId) {
        reservations.asMap().values().remove(taskId);
      }
    }
  }
}

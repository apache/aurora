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
package org.apache.aurora.scheduler.async;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Qualifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.Subscribe;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.stats.StatsProvider;
import com.twitter.common.util.Clock;

import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.state.TaskAssigner;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskInfo;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

import static org.apache.aurora.gen.ScheduleStatus.LOST;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;

/**
 * Enables scheduling and preemption of tasks.
 */
public interface TaskScheduler extends EventSubscriber {

  /**
   * Attempts to schedule a task, possibly performing irreversible actions.
   *
   * @param taskId The task to attempt to schedule.
   * @return {@code true} if the task was scheduled, {@code false} otherwise. The caller should
   *         call schedule again if {@code false} is returned.
   */
  boolean schedule(String taskId);

  /**
   * An asynchronous task scheduler.  Scheduling of tasks is performed on a delay, where each task
   * backs off after a failed scheduling attempt.
   * <p>
   * Pending tasks are advertised to the scheduler via internal pubsub notifications.
   */
  class TaskSchedulerImpl implements TaskScheduler {
    /**
     * Binding annotation for the time duration of reservations.
     */
    @Qualifier
    @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
    @interface ReservationDuration { }

    private static final Logger LOG = Logger.getLogger(TaskSchedulerImpl.class.getName());

    private final Storage storage;
    private final StateManager stateManager;
    private final TaskAssigner assigner;
    private final OfferQueue offerQueue;
    private final Preemptor preemptor;
    private final Reservations reservations;

    private final AtomicLong attemptsFired = Stats.exportLong("schedule_attempts_fired");
    private final AtomicLong attemptsFailed = Stats.exportLong("schedule_attempts_failed");
    private final AtomicLong attemptsNoMatch = Stats.exportLong("schedule_attempts_no_match");

    @Inject
    TaskSchedulerImpl(
        Storage storage,
        StateManager stateManager,
        TaskAssigner assigner,
        OfferQueue offerQueue,
        Preemptor preemptor,
        @ReservationDuration Amount<Long, Time> reservationDuration,
        final Clock clock,
        StatsProvider statsProvider) {

      this.storage = requireNonNull(storage);
      this.stateManager = requireNonNull(stateManager);
      this.assigner = requireNonNull(assigner);
      this.offerQueue = requireNonNull(offerQueue);
      this.preemptor = requireNonNull(preemptor);
      this.reservations = new Reservations(statsProvider, reservationDuration, clock);
    }

    private Function<HostOffer, Optional<TaskInfo>> getAssignerFunction(
        final MutableStoreProvider storeProvider,
        final ResourceRequest resourceRequest) {

      // TODO(wfarner): Turn this into Predicate<Offer>, and in the caller, find the first match
      // and perform the assignment at the very end.  This will allow us to use optimistic locking
      // at the top of the stack and avoid holding the write lock for too long.
      return new Function<HostOffer, Optional<TaskInfo>>() {
        @Override
        public Optional<TaskInfo> apply(HostOffer offer) {
          Optional<String> reservedTaskId =
              reservations.getSlaveReservation(offer.getOffer().getSlaveId());
          if (reservedTaskId.isPresent()) {
            if (resourceRequest.getTaskId().equals(reservedTaskId.get())) {
              // Slave is reserved to satisfy this task.
              return assigner.maybeAssign(storeProvider, offer, resourceRequest);
            } else {
              // Slave is reserved for another task.
              return Optional.absent();
            }
          } else {
            // Slave is not reserved.
            return assigner.maybeAssign(storeProvider, offer, resourceRequest);
          }
        }
      };
    }

    @VisibleForTesting
    static final Optional<String> LAUNCH_FAILED_MSG =
        Optional.of("Unknown exception attempting to schedule task.");

    @VisibleForTesting
    static Query.Builder activeJobStateQuery(IJobKey jobKey) {
      return Query.jobScoped(jobKey).byStatus(Tasks.SLAVE_ASSIGNED_STATES);
    }

    private AttributeAggregate getJobState(
        final StoreProvider storeProvider,
        final IJobKey jobKey) {

      Supplier<ImmutableSet<IScheduledTask>> taskSupplier = Suppliers.memoize(
          new Supplier<ImmutableSet<IScheduledTask>>() {
            @Override
            public ImmutableSet<IScheduledTask> get() {
              return storeProvider.getTaskStore().fetchTasks(activeJobStateQuery(jobKey));
            }
          });
      return new AttributeAggregate(taskSupplier, storeProvider.getAttributeStore());
    }

    @Timed("task_schedule_attempt")
    @Override
    public boolean schedule(final String taskId) {
      attemptsFired.incrementAndGet();
      try {
        return storage.write(new MutateWork.Quiet<Boolean>() {
          @Override
          public Boolean apply(MutableStoreProvider store) {
            LOG.fine("Attempting to schedule task " + taskId);
            final ITaskConfig task = Iterables.getOnlyElement(
                Iterables.transform(
                    store.getTaskStore().fetchTasks(Query.taskScoped(taskId).byStatus(PENDING)),
                    Tasks.SCHEDULED_TO_INFO),
                null);
            if (task == null) {
              LOG.warning("Failed to look up task " + taskId + ", it may have been deleted.");
            } else {
              AttributeAggregate aggregate = getJobState(store, task.getJob());
              try {
                ResourceRequest resourceRequest = new ResourceRequest(task, taskId, aggregate);
                if (!offerQueue.launchFirst(getAssignerFunction(store, resourceRequest))) {
                  // Task could not be scheduled.
                  maybePreemptFor(taskId, aggregate);
                  attemptsNoMatch.incrementAndGet();
                  return false;
                }
              } catch (OfferQueue.LaunchException e) {
                LOG.log(Level.WARNING, "Failed to launch task.", e);
                attemptsFailed.incrementAndGet();

                // The attempt to schedule the task failed, so we need to backpedal on the
                // assignment.
                // It is in the LOST state and a new task will move to PENDING to replace it.
                // Should the state change fail due to storage issues, that's okay.  The task will
                // time out in the ASSIGNED state and be moved to LOST.
                stateManager.changeState(
                    store,
                    taskId,
                    Optional.of(PENDING),
                    LOST,
                    LAUNCH_FAILED_MSG);
              }
            }

            return true;
          }
        });
      } catch (RuntimeException e) {
        // We catch the generic unchecked exception here to ensure tasks are not abandoned
        // if there is a transient issue resulting in an unchecked exception.
        LOG.log(Level.WARNING, "Task scheduling unexpectedly failed, will be retried", e);
        attemptsFailed.incrementAndGet();
        return false;
      }
    }

    private void maybePreemptFor(String taskId, AttributeAggregate attributeAggregate) {
      if (reservations.hasReservationForTask(taskId)) {
        return;
      }
      Optional<String> slaveId = preemptor.findPreemptionSlotFor(taskId, attributeAggregate);
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

    @VisibleForTesting
    static final String RESERVATIONS_CACHE_SIZE_STAT = "reservation_cache_size";

    private static class Reservations {
      private final Cache<SlaveID, String> reservations;

      Reservations(
          StatsProvider statsProvider,
          Amount<Long, Time> duration,
          final Clock clock) {
        requireNonNull(duration);
        requireNonNull(clock);
        this.reservations = CacheBuilder.newBuilder()
            .expireAfterWrite(duration.as(Time.MINUTES), TimeUnit.MINUTES)
            .ticker(new Ticker() {
              @Override
              public long read() {
                return clock.nowNanos();
              }
            })
            .build();
        statsProvider.makeGauge(
            RESERVATIONS_CACHE_SIZE_STAT,
            new Supplier<Long>() {
              @Override
              public Long get() {
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

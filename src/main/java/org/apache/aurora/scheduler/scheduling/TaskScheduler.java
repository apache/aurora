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
package org.apache.aurora.scheduler.scheduling;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Qualifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.Subscribe;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.stats.Stats;

import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.preemptor.BiCache;
import org.apache.aurora.scheduler.preemptor.Preemptor;
import org.apache.aurora.scheduler.state.TaskAssigner;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

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
    @VisibleForTesting
    @Qualifier
    @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
    public @interface ReservationDuration { }

    private static final Logger LOG = Logger.getLogger(TaskSchedulerImpl.class.getName());

    private final Storage storage;
    private final TaskAssigner assigner;
    private final Preemptor preemptor;
    private final BiCache<String, TaskGroupKey> reservations;

    private final AtomicLong attemptsFired = Stats.exportLong("schedule_attempts_fired");
    private final AtomicLong attemptsFailed = Stats.exportLong("schedule_attempts_failed");
    private final AtomicLong attemptsNoMatch = Stats.exportLong("schedule_attempts_no_match");

    @Inject
    TaskSchedulerImpl(
        Storage storage,
        TaskAssigner assigner,
        Preemptor preemptor,
        BiCache<String, TaskGroupKey> reservations) {

      this.storage = requireNonNull(storage);
      this.assigner = requireNonNull(assigner);
      this.preemptor = requireNonNull(preemptor);
      this.reservations = requireNonNull(reservations);
    }

    @Timed("task_schedule_attempt")
    @Override
    public boolean schedule(final String taskId) {
      attemptsFired.incrementAndGet();
      try {
        return storage.write(new MutateWork.Quiet<Boolean>() {
          @Override
          public Boolean apply(MutableStoreProvider store) {
            return scheduleTask(store, taskId);
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

    @Timed("task_schedule_attempt_locked")
    protected boolean scheduleTask(MutableStoreProvider store, String taskId) {
      LOG.fine("Attempting to schedule task " + taskId);
      IAssignedTask assignedTask = Iterables.getOnlyElement(
          Iterables.transform(
              store.getTaskStore().fetchTasks(Query.taskScoped(taskId).byStatus(PENDING)),
              IScheduledTask::getAssignedTask),
          null);

      if (assignedTask == null) {
        LOG.warning("Failed to look up task " + taskId + ", it may have been deleted.");
      } else {
        ITaskConfig task = assignedTask.getTask();
        AttributeAggregate aggregate = AttributeAggregate.getJobActiveState(store, task.getJob());

        boolean launched = assigner.maybeAssign(
            store,
            new ResourceRequest(task, aggregate),
            TaskGroupKey.from(task),
            taskId,
            reservations.asMap());

        if (!launched) {
          // Task could not be scheduled.
          // TODO(maxim): Now that preemption slots are searched asynchronously, consider
          // retrying a launch attempt within the current scheduling round IFF a reservation is
          // available.
          maybePreemptFor(assignedTask, aggregate, store);
          attemptsNoMatch.incrementAndGet();
          return false;
        }
      }

      return true;
    }

    private void maybePreemptFor(
        IAssignedTask task,
        AttributeAggregate jobState,
        MutableStoreProvider storeProvider) {

      if (!reservations.getByValue(TaskGroupKey.from(task.getTask())).isEmpty()) {
        return;
      }
      Optional<String> slaveId = preemptor.attemptPreemptionFor(task, jobState, storeProvider);
      if (slaveId.isPresent()) {
        reservations.put(slaveId.get(), TaskGroupKey.from(task.getTask()));
      }
    }

    @Subscribe
    public void taskChanged(final TaskStateChange stateChangeEvent) {
      if (Optional.of(PENDING).equals(stateChangeEvent.getOldState())) {
        IAssignedTask assigned = stateChangeEvent.getTask().getAssignedTask();
        if (assigned.getSlaveId() != null) {
          reservations.remove(assigned.getSlaveId(), TaskGroupKey.from(assigned.getTask()));
        }
      }
    }
  }
}

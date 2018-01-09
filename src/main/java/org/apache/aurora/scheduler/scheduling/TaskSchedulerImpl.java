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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Qualifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.eventbus.Subscribe;

import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.stats.Stats;
import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.configuration.executor.ExecutorSettings;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.preemptor.BiCache;
import org.apache.aurora.scheduler.preemptor.Preemptor;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

import static org.apache.aurora.gen.ScheduleStatus.PENDING;

/**
 * An asynchronous task scheduler.  Scheduling of tasks is performed on a delay, where each task
 * backs off after a failed scheduling attempt.
 * <p>
 * Pending tasks are advertised to the scheduler via internal pubsub notifications.
 */
public class TaskSchedulerImpl implements TaskScheduler {
  /**
   * Binding annotation for the time duration of reservations.
   */
  @VisibleForTesting
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  public @interface ReservationDuration { }

  private static final Logger LOG =
      LoggerFactory.getLogger(org.apache.aurora.scheduler.scheduling.TaskSchedulerImpl.class);

  private final TaskAssigner assigner;
  private final Preemptor preemptor;
  private final ExecutorSettings executorSettings;
  private final TierManager tierManager;
  private final BiCache<String, TaskGroupKey> reservations;

  private final AtomicLong attemptsFired = Stats.exportLong("schedule_attempts_fired");
  private final AtomicLong attemptsFailed = Stats.exportLong("schedule_attempts_failed");
  private final AtomicLong attemptsNoMatch = Stats.exportLong("schedule_attempts_no_match");

  @Inject
  TaskSchedulerImpl(
      TaskAssigner assigner,
      Preemptor preemptor,
      ExecutorSettings executorSettings,
      TierManager tierManager,
      BiCache<String, TaskGroupKey> reservations) {

    this.assigner = requireNonNull(assigner);
    this.preemptor = requireNonNull(preemptor);
    this.executorSettings = requireNonNull(executorSettings);
    this.tierManager = requireNonNull(tierManager);
    this.reservations = requireNonNull(reservations);
  }

  @Timed("task_schedule_attempt")
  public Set<String> schedule(MutableStoreProvider store, Set<String> taskIds) {
    try {
      return scheduleTasks(store, taskIds);
    } catch (RuntimeException e) {
      // We catch the generic unchecked exception here to ensure tasks are not abandoned
      // if there is a transient issue resulting in an unchecked exception.
      LOG.warn("Task scheduling unexpectedly failed, will be retried", e);
      attemptsFailed.incrementAndGet();
      // Return empty set for all task IDs to be retried later.
      // It's ok if some tasks were already assigned, those will be ignored in the next round.
      return ImmutableSet.of();
    }
  }

  private Map<String, IAssignedTask> fetchTasks(StoreProvider store, Set<String> ids) {
    Map<String, IAssignedTask> tasks = store.getTaskStore()
        .fetchTasks(Query.taskScoped(ids).byStatus(PENDING))
        .stream()
        .map(IScheduledTask::getAssignedTask)
        .collect(Collectors.toMap(
            IAssignedTask::getTaskId,
            Function.identity()
        ));

    if (ids.size() != tasks.size()) {
      LOG.warn("Failed to look up tasks "
          + Joiner.on(", ").join(Sets.difference(ids, tasks.keySet())));
    }
    return tasks;
  }

  private Set<String> scheduleTasks(MutableStoreProvider store, Set<String> ids) {
    LOG.debug("Attempting to schedule tasks {}", ids);
    Map<String, IAssignedTask> tasksById = fetchTasks(store, ids);

    if (tasksById.isEmpty()) {
      // None of the tasks were found in storage.  This could be caused by a task group that was
      // killed by the user, for example.
      return ids;
    }

    // Prepare scheduling context for the tasks
    ITaskConfig task = Iterables.getOnlyElement(tasksById.values().stream()
        .map(IAssignedTask::getTask)
        .collect(Collectors.toSet()));
    AttributeAggregate aggregate = AttributeAggregate.getJobActiveState(store, task.getJob());

    // Attempt to schedule using available resources.
    Set<String> launched = assigner.maybeAssign(
        store,
        ResourceRequest.fromTask(task, executorSettings, aggregate, tierManager),
        TaskGroupKey.from(task),
        ImmutableSet.copyOf(tasksById.values()),
        reservations.asMap());

    attemptsFired.addAndGet(tasksById.size());

    // Fall back to preemption for tasks not scheduled above.
    Set<String> unassigned = Sets.difference(tasksById.keySet(), launched);
    unassigned.forEach(taskId -> {
      // TODO(maxim): Now that preemption slots are searched asynchronously, consider
      // retrying a launch attempt within the current scheduling round IFF a reservation is
      // available.
      maybePreemptFor(tasksById.get(taskId), aggregate, store);
    });
    attemptsNoMatch.addAndGet(unassigned.size());

    // Return all successfully launched tasks as well as those weren't tried (not in PENDING).
    return Sets.union(launched, Sets.difference(ids, tasksById.keySet()));
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
  public void taskChanged(final PubsubEvent.TaskStateChange stateChangeEvent) {
    if (Optional.of(PENDING).equals(stateChangeEvent.getOldState())) {
      IAssignedTask assigned = stateChangeEvent.getTask().getAssignedTask();
      if (assigned.getSlaveId() != null) {
        reservations.remove(assigned.getSlaveId(), TaskGroupKey.from(assigned.getTask()));
      }
    }
  }
}

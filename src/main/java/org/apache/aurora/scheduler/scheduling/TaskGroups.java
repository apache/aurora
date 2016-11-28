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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;
import javax.inject.Qualifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.SlidingStats;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.util.BackoffStrategy;
import org.apache.aurora.scheduler.BatchWorker;
import org.apache.aurora.scheduler.async.AsyncModule.AsyncExecutor;
import org.apache.aurora.scheduler.async.DelayExecutor;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.events.PubsubEvent.TasksDeleted;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

import static org.apache.aurora.gen.ScheduleStatus.PENDING;

/**
 * A collection of task groups, where a task group is a collection of tasks that are known to be
 * equal in the way they schedule. This is expected to be tasks associated with the same job key,
 * who also have {@code equal()} {@link org.apache.aurora.scheduler.storage.entities.ITaskConfig}
 * values.
 * <p>
 * This is used to prevent redundant work in trying to schedule tasks as well as to provide
 * nearly-equal responsiveness when scheduling across jobs.  In other words, a 1000 instance job
 * cannot starve a 1 instance job.
 */
public class TaskGroups implements EventSubscriber {

  private final ConcurrentMap<TaskGroupKey, TaskGroup> groups = Maps.newConcurrentMap();
  private final DelayExecutor executor;
  private final TaskGroupsSettings settings;
  private final TaskScheduler taskScheduler;
  private final RescheduleCalculator rescheduleCalculator;
  private final BatchWorker<Set<String>> batchWorker;

  // Track the penalties of tasks at the time they were scheduled. This is to provide data that
  // may influence the selection of a different backoff strategy.
  private final SlidingStats scheduledTaskPenalties =
      new SlidingStats("scheduled_task_penalty", "ms");

  /**
   * Annotation for the max scheduling batch size.
   */
  @VisibleForTesting
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  public @interface SchedulingMaxBatchSize { }

  @VisibleForTesting
  public static class TaskGroupBatchWorker extends BatchWorker<Set<String>> {
    @Inject
    TaskGroupBatchWorker(
        Storage storage,
        StatsProvider statsProvider,
        @SchedulingMaxBatchSize int maxBatchSize) {

      super(storage, statsProvider, maxBatchSize);
    }

    @Override
    protected String serviceName() {
      return "TaskGroupBatchWorker";
    }
  }

  public static class TaskGroupsSettings {
    private final Amount<Long, Time> firstScheduleDelay;
    private final BackoffStrategy taskGroupBackoff;
    private final RateLimiter rateLimiter;
    private final int maxTasksPerSchedule;

    public TaskGroupsSettings(
        Amount<Long, Time> firstScheduleDelay,
        BackoffStrategy taskGroupBackoff,
        RateLimiter rateLimiter,
        int maxTasksPerSchedule) {

      this.firstScheduleDelay = requireNonNull(firstScheduleDelay);
      Preconditions.checkArgument(firstScheduleDelay.getValue() > 0);
      this.taskGroupBackoff = requireNonNull(taskGroupBackoff);
      this.rateLimiter = requireNonNull(rateLimiter);
      this.maxTasksPerSchedule = maxTasksPerSchedule;
      Preconditions.checkArgument(maxTasksPerSchedule > 0);
    }
  }

  @VisibleForTesting
  @Inject
  public TaskGroups(
      @AsyncExecutor DelayExecutor executor,
      TaskGroupsSettings settings,
      TaskScheduler taskScheduler,
      RescheduleCalculator rescheduleCalculator,
      TaskGroupBatchWorker batchWorker) {

    this.executor = requireNonNull(executor);
    this.settings = requireNonNull(settings);
    this.taskScheduler = requireNonNull(taskScheduler);
    this.rescheduleCalculator = requireNonNull(rescheduleCalculator);
    this.batchWorker = requireNonNull(batchWorker);
  }

  private synchronized void evaluateGroupLater(Runnable evaluate, TaskGroup group) {
    // Avoid check-then-act by holding the intrinsic lock.  If not done atomically, we could
    // remove a group while a task is being added to it.
    if (group.hasMore()) {
      executor.execute(evaluate, Amount.of(group.getPenaltyMs(), Time.MILLISECONDS));
    } else {
      groups.remove(group.getKey());
    }
  }

  private void startGroup(final TaskGroup group) {
    Runnable monitor = new Runnable() {
      @Override
      public void run() {
        final Set<String> taskIds = group.peek(settings.maxTasksPerSchedule);
        long penaltyMs = 0;
        if (!taskIds.isEmpty()) {
          settings.rateLimiter.acquire();
          CompletableFuture<Set<String>> result = batchWorker.execute(storeProvider ->
              taskScheduler.schedule(storeProvider, taskIds));

          Set<String> scheduled = null;
          try {
            scheduled = result.get();
          } catch (ExecutionException | InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }

          if (scheduled.isEmpty()) {
            penaltyMs = settings.taskGroupBackoff.calculateBackoffMs(group.getPenaltyMs());
          } else {
            scheduledTaskPenalties.accumulate(group.getPenaltyMs());
            group.remove(scheduled);
            if (group.hasMore()) {
              penaltyMs = settings.firstScheduleDelay.as(Time.MILLISECONDS);
            }
          }
        }

        group.setPenaltyMs(penaltyMs);
        evaluateGroupLater(this, group);
      }
    };
    evaluateGroupLater(monitor, group);
  }

  /**
   * Informs the task groups of a task state change.
   * <p>
   * This is used to observe {@link org.apache.aurora.gen.ScheduleStatus#PENDING} tasks and begin
   * attempting to schedule them.
   *
   * @param stateChange State change notification.
   */
  @Subscribe
  public synchronized void taskChangedState(TaskStateChange stateChange) {
    if (stateChange.getNewState() == PENDING) {
      IScheduledTask task = stateChange.getTask();
      TaskGroupKey key = TaskGroupKey.from(task.getAssignedTask().getTask());
      TaskGroup newGroup = new TaskGroup(key, Tasks.id(task));
      TaskGroup existing = groups.putIfAbsent(key, newGroup);
      if (existing == null) {
        long penaltyMs;
        if (stateChange.isTransition()) {
          penaltyMs = settings.firstScheduleDelay.as(Time.MILLISECONDS);
        } else {
          penaltyMs = rescheduleCalculator.getStartupScheduleDelayMs(task);
        }
        newGroup.setPenaltyMs(penaltyMs);
        startGroup(newGroup);
      } else {
        existing.offer(Tasks.id(task));
      }
    }
  }

  /**
   * Signals the scheduler that tasks have been deleted.
   *
   * @param deleted Tasks deleted event.
   */
  @Subscribe
  public synchronized void tasksDeleted(TasksDeleted deleted) {
    for (IAssignedTask task
        : Iterables.transform(deleted.getTasks(), IScheduledTask::getAssignedTask)) {
      TaskGroup group = groups.get(TaskGroupKey.from(task.getTask()));
      if (group != null) {
        group.remove(ImmutableSet.of(task.getTaskId()));
      }
    }
  }

  public Iterable<TaskGroup> getGroups() {
    return ImmutableSet.copyOf(groups.values());
  }

}

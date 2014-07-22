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

import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.RateLimiter;
import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.BackoffStrategy;
import com.twitter.common.util.concurrent.ExecutorServiceShutdown;

import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.events.PubsubEvent.TasksDeleted;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.apache.aurora.gen.ScheduleStatus.PENDING;

/**
 * A collection of task groups, where a task group is a collection of tasks that are known to be
 * equal in the way they schedule. This is expected to be tasks associated with the same job key,
 * who also have {@code equal()} {@link ITaskConfig} values.
 * <p>
 * This is used to prevent redundant work in trying to schedule tasks as well as to provide
 * nearly-equal responsiveness when scheduling across jobs.  In other words, a 1000 instance job
 * cannot starve a 1 instance job.
 */
public class TaskGroups implements EventSubscriber {

  private static final Logger LOG = Logger.getLogger(TaskGroups.class.getName());

  private final ConcurrentMap<GroupKey, TaskGroup> groups = Maps.newConcurrentMap();
  private final ScheduledExecutorService executor;
  private final TaskScheduler taskScheduler;
  private final BackoffStrategy backoff;
  private final RescheduleCalculator rescheduleCalculator;

  public static class TaskGroupsSettings {
    private final BackoffStrategy taskGroupBackoff;
    private final RateLimiter rateLimiter;

    public TaskGroupsSettings(BackoffStrategy taskGroupBackoff, RateLimiter rateLimiter) {
      this.taskGroupBackoff = requireNonNull(taskGroupBackoff);
      this.rateLimiter = requireNonNull(rateLimiter);
    }
  }

  @Inject
  TaskGroups(
      ShutdownRegistry shutdownRegistry,
      TaskGroupsSettings settings,
      TaskScheduler taskScheduler,
      RescheduleCalculator rescheduleCalculator) {

    this(
        createThreadPool(shutdownRegistry),
        settings.taskGroupBackoff,
        settings.rateLimiter,
        taskScheduler,
        rescheduleCalculator);
  }

  @VisibleForTesting
  TaskGroups(
      final ScheduledExecutorService executor,
      final BackoffStrategy backoff,
      final RateLimiter rateLimiter,
      final TaskScheduler taskScheduler,
      final RescheduleCalculator rescheduleCalculator) {

    this.executor = requireNonNull(executor);
    requireNonNull(rateLimiter);
    requireNonNull(taskScheduler);
    this.backoff = requireNonNull(backoff);
    this.rescheduleCalculator = requireNonNull(rescheduleCalculator);

    this.taskScheduler = new TaskScheduler() {
      @Override
      public boolean schedule(String taskId) {
        rateLimiter.acquire();
        return taskScheduler.schedule(taskId);
      }
    };
  }

  private synchronized void evaluateGroupLater(Runnable evaluate, TaskGroup group) {
    // Avoid check-then-act by holding the intrinsic lock.  If not done atomically, we could
    // remove a group while a task is being added to it.
    if (group.hasMore()) {
      executor.schedule(evaluate, group.getPenaltyMs(), MILLISECONDS);
    } else {
      groups.remove(group.getKey());
    }
  }

  private void startGroup(final TaskGroup group) {
    Runnable monitor = new Runnable() {
      @Override
      public void run() {
        Optional<String> taskId = group.peek();
        long penaltyMs = 0;
        if (taskId.isPresent()) {
          if (taskScheduler.schedule(taskId.get())) {
            group.remove(taskId.get());
            if (group.hasMore()) {
              penaltyMs = backoff.calculateBackoffMs(0);
            }
          } else {
            penaltyMs = backoff.calculateBackoffMs(group.getPenaltyMs());
          }
        }

        group.setPenaltyMs(penaltyMs);
        evaluateGroupLater(this, group);
      }
    };
    evaluateGroupLater(monitor, group);
  }

  private static ScheduledExecutorService createThreadPool(ShutdownRegistry shutdownRegistry) {
    final ScheduledThreadPoolExecutor executor =
        AsyncUtil.singleThreadLoggingScheduledExecutor("TaskScheduler-%d", LOG);

    Stats.exportSize("schedule_queue_size", executor.getQueue());
    shutdownRegistry.addAction(new Command() {
      @Override
      public void execute() {
        new ExecutorServiceShutdown(executor, Amount.of(1L, Time.SECONDS)).execute();
      }
    });
    return executor;
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
      GroupKey key = new GroupKey(task.getAssignedTask().getTask());
      TaskGroup newGroup = new TaskGroup(key, Tasks.id(task));
      TaskGroup existing = groups.putIfAbsent(key, newGroup);
      if (existing == null) {
        long penaltyMs;
        if (stateChange.isTransition()) {
          penaltyMs = backoff.calculateBackoffMs(0);
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
        : Iterables.transform(deleted.getTasks(), Tasks.SCHEDULED_TO_ASSIGNED)) {
      TaskGroup group = groups.get(new GroupKey(task.getTask()));
      if (group != null) {
        group.remove(task.getTaskId());
      }
    }
  }

  public Iterable<TaskGroup> getGroups() {
    return ImmutableSet.copyOf(groups.values());
  }

  static class GroupKey {
    private final ITaskConfig canonicalTask;

    GroupKey(ITaskConfig task) {
      this.canonicalTask = task;
    }

    @Override
    public int hashCode() {
      return Objects.hash(canonicalTask);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof GroupKey)) {
        return false;
      }
      GroupKey other = (GroupKey) o;
      return Objects.equals(canonicalTask, other.canonicalTask);
    }

    @Override
    public String toString() {
      return JobKeys.canonicalString(Tasks.INFO_TO_JOB_KEY.apply(canonicalTask));
    }
  }
}

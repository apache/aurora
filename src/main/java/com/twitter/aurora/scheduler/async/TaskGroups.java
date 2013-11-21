/*
 * Copyright 2013 Twitter, Inc.
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
package com.twitter.aurora.scheduler.async;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.scheduler.async.PreemptorIface;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.configuration.ConfigurationManager;
import com.twitter.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import com.twitter.aurora.scheduler.events.PubsubEvent.StorageStarted;
import com.twitter.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import com.twitter.aurora.scheduler.events.PubsubEvent.TasksDeleted;
import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.aurora.scheduler.storage.entities.IAssignedTask;
import com.twitter.aurora.scheduler.storage.entities.IScheduledTask;
import com.twitter.aurora.scheduler.storage.entities.ITaskConfig;
import com.twitter.aurora.scheduler.storage.entities.ITaskEvent;
import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.BackoffStrategy;
import com.twitter.common.util.Clock;
import com.twitter.common.util.concurrent.ExecutorServiceShutdown;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.aurora.gen.ScheduleStatus.KILLING;
import static com.twitter.aurora.gen.ScheduleStatus.PENDING;
import static com.twitter.aurora.gen.ScheduleStatus.RESTARTING;
import static com.twitter.aurora.scheduler.async.TaskGroup.GroupState;

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

  private final Storage storage;
  private final LoadingCache<GroupKey, TaskGroup> groups;
  private final Amount<Long, Time> flappingThreshold;
  private final BackoffStrategy flappingBackoffStrategy;
  private final Clock clock;
  private final PreemptorIface preemptor;

  @Inject
  TaskGroups(
      ShutdownRegistry shutdownRegistry,
      Storage storage,
      SchedulingSettings schedulingSettings,
      SchedulingAction schedulingAction,
      FlappingTaskSettings flappingTaskSettings,
      Clock clock,
      PreemptorIface preemptor) {

    this(
        createThreadPool(shutdownRegistry),
        storage,
        schedulingSettings.getBackoff(),
        schedulingSettings.getRateLimit(),
        schedulingAction,
        flappingTaskSettings.getFlappingThreashold(),
        clock,
        flappingTaskSettings.getBackoff(),
        preemptor);
  }

  TaskGroups(
      final ScheduledExecutorService executor,
      final Storage storage,
      final BackoffStrategy backoffStrategy,
      final RateLimiter rateLimiter,
      final SchedulingAction schedulingAction,
      final Amount<Long, Time> flappingThreshold,
      final Clock clock,
      final BackoffStrategy flappingBackoffStrategy,
      final PreemptorIface preemptor) {

    this.storage = checkNotNull(storage);
    checkNotNull(executor);
    checkNotNull(backoffStrategy);
    checkNotNull(schedulingAction);
    this.flappingThreshold = checkNotNull(flappingThreshold);
    this.clock = checkNotNull(clock);
    this.flappingBackoffStrategy = checkNotNull(flappingBackoffStrategy);
    this.preemptor = checkNotNull(preemptor);

    final SchedulingAction rateLimitedAction = new SchedulingAction() {
      @Override public boolean schedule(String taskId) {
        rateLimiter.acquire();
        return schedulingAction.schedule(taskId);
      }
    };

    groups = CacheBuilder.newBuilder().build(new CacheLoader<GroupKey, TaskGroup>() {
      @Override public TaskGroup load(GroupKey key) {
        TaskGroup group = new TaskGroup(key, backoffStrategy);
        LOG.info("Evaluating group " + key + " in " + group.getPenaltyMs() + " ms");
        startGroup(group, executor, rateLimitedAction);
        return group;
      }
    });
  }

  private synchronized boolean maybeInvalidate(TaskGroup group) {
    if (group.getTaskIds().isEmpty()) {
      groups.invalidate(group.getKey());
      return true;
    }
    return false;
  }

  private void startGroup(
      final TaskGroup group,
      final ScheduledExecutorService executor,
      final SchedulingAction action) {

    Runnable monitor = new Runnable() {
      @Override public void run() {
        GroupState state = group.isReady(clock.nowMillis());

        switch (state) {
          case EMPTY:
            maybeInvalidate(group);
            break;

          case READY:
            String id = group.pop();
            if (action.schedule(id)) {
              if (!maybeInvalidate(group)) {
                executor.schedule(this, group.resetPenaltyAndGet(), TimeUnit.MILLISECONDS);
              }
            } else {
              group.push(id, clock.nowMillis());
              executor.schedule(this, group.penalizeAndGet(), TimeUnit.MILLISECONDS);
              // TODO(zmanji): Use the return value in a slave <-> task matching manner
              preemptor.findPreemptionSlotFor(id);
            }
            break;

          case NOT_READY:
            executor.schedule(this, group.getPenaltyMs(), TimeUnit.MILLISECONDS);
            break;

          default:
            throw new IllegalStateException("Unknown GroupState " + state);
        }
      }
    };
    executor.schedule(monitor, group.getPenaltyMs(), TimeUnit.MILLISECONDS);
  }

  private static ScheduledExecutorService createThreadPool(ShutdownRegistry shutdownRegistry) {
    // TODO(William Farner): Leverage ExceptionHandlingScheduledExecutorService:
    // com.twitter.common.util.concurrent.ExceptionHandlingScheduledExecutorService
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

  private synchronized void add(IAssignedTask task, long readyTimestamp) {
    groups.getUnchecked(new GroupKey(task.getTask())).push(task.getTaskId(), readyTimestamp);
  }

  private Optional<IScheduledTask> getTaskAncestor(IScheduledTask task) {
    if (!task.isSetAncestorId()) {
      return Optional.absent();
    }

    ImmutableSet<IScheduledTask> res =
        Storage.Util.weaklyConsistentFetchTasks(storage, Query.taskScoped(task.getAncestorId()));

    return Optional.fromNullable(Iterables.getOnlyElement(res, null));
  }

  private static final Predicate<ScheduleStatus> IS_ACTIVE_STATUS =
      Predicates.in(Tasks.ACTIVE_STATES);

  private static final Function<ITaskEvent, ScheduleStatus> TO_STATUS =
      new Function<ITaskEvent, ScheduleStatus>() {
    @Override public ScheduleStatus apply(ITaskEvent input) {
      return input.getStatus();
    }
  };

  private static final Set<ScheduleStatus> INTERRUPTED_TASK_STATES =
      EnumSet.of(RESTARTING, KILLING);

  private final Predicate<IScheduledTask> flapped = new Predicate<IScheduledTask>() {
    @Override public boolean apply(IScheduledTask task) {
      if (!task.isSetTaskEvents()) {
        return false;
      }

      List<ITaskEvent> events = Lists.reverse(task.getTaskEvents());

      // Avoid penalizing tasks that were interrupted by outside action, such as a user
      // restarting them.
      if (Iterables.any(Iterables.transform(events, TO_STATUS),
          Predicates.in(INTERRUPTED_TASK_STATES))) {
        return false;
      }

      ITaskEvent terminalEvent = Iterables.get(events, 0);
      ScheduleStatus terminalState = terminalEvent.getStatus();
      Preconditions.checkState(Tasks.isTerminated(terminalState));

      ITaskEvent activeEvent =
          Iterables.find(events, Predicates.compose(IS_ACTIVE_STATUS, TO_STATUS));

      long thresholdMs = flappingThreshold.as(Time.MILLISECONDS);

      return (terminalEvent.getTimestamp() - activeEvent.getTimestamp()) < thresholdMs;
    }
  };

  private long getTaskReadyTimestamp(IScheduledTask task) {
    Optional<IScheduledTask> curTask = getTaskAncestor(task);
    long penaltyMs = 0;
    while (curTask.isPresent() && flapped.apply(curTask.get())) {
      LOG.info(
          String.format("Ancestor of %s flapped: %s", Tasks.id(task), Tasks.id(curTask.get())));
      long newPenalty = flappingBackoffStrategy.calculateBackoffMs(penaltyMs);
      // If the backoff strategy is truncated then there is no need for us to continue.
      if (newPenalty == penaltyMs) {
        break;
      }
      penaltyMs = newPenalty;
      curTask = getTaskAncestor(curTask.get());
    }

    return penaltyMs + clock.nowMillis();
  }

  /**
   * Informs the task groups of a task state change.
   * <p>
   * This is used to observe {@link com.twitter.aurora.gen.ScheduleStatus#PENDING} tasks and begin
   * attempting to schedule them.
   *
   * @param stateChange State change notification.
   */
  @Subscribe
  public synchronized void taskChangedState(TaskStateChange stateChange) {
    if (stateChange.getNewState() == PENDING) {
      add(stateChange.getTask().getAssignedTask(), getTaskReadyTimestamp(stateChange.getTask()));
    }
  }

  /**
   * Signals that storage has started and is consistent.
   * <p>
   * Upon this signal, all {@link com.twitter.aurora.gen.ScheduleStatus#PENDING} tasks in the stoage
   * will become eligible for scheduling.
   *
   * @param event Storage started notification.
   */
  @Subscribe
  public void storageStarted(StorageStarted event) {
    for (IScheduledTask task
        : Storage.Util.consistentFetchTasks(storage, Query.unscoped().byStatus(PENDING))) {

      add(task.getAssignedTask(), getTaskReadyTimestamp(task));
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
      TaskGroup group = groups.getIfPresent(new GroupKey(task.getTask()));
      if (group != null) {
        group.remove(task.getTaskId());
      }
    }
  }

  public Iterable<TaskGroup> getGroups() {
    return ImmutableSet.copyOf(groups.asMap().values());
  }

  static class GroupKey {
    private final ITaskConfig scrubbedCanonicalTask;

    GroupKey(ITaskConfig task) {
      this.scrubbedCanonicalTask = ConfigurationManager.scrubNonUniqueTaskFields(task);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(scrubbedCanonicalTask);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof GroupKey)) {
        return false;
      }
      GroupKey other = (GroupKey) o;
      return Objects.equal(scrubbedCanonicalTask, other.scrubbedCanonicalTask);
    }

    @Override
    public String toString() {
      return JobKeys.toPath(Tasks.INFO_TO_JOB_KEY.apply(scrubbedCanonicalTask));
    }
  }

  interface SchedulingAction {
    /**
     * Attempts to schedule a task, possibly performing irreversible actions.
     *
     * @param taskId The task to attempt to schedule.
     * @return {@code true} if the task was scheduled, {@code false} otherwise.
     */
    boolean schedule(String taskId);
  }

  static class SchedulingSettings {
    private final BackoffStrategy backoff;
    private final RateLimiter rateLimit;

    SchedulingSettings(BackoffStrategy backoff, RateLimiter rateLimit) {
      this.backoff = checkNotNull(backoff);
      this.rateLimit = checkNotNull(rateLimit);
    }

    BackoffStrategy getBackoff() {
      return backoff;
    }

    RateLimiter getRateLimit() {
      return rateLimit;
    }
  }

  static class FlappingTaskSettings {
    private final BackoffStrategy backoff;
    private final Amount<Long, Time> flappingThreashold;

    FlappingTaskSettings(BackoffStrategy backoff, Amount<Long, Time> flappingThreashold) {
      this.backoff = checkNotNull(backoff);
      this.flappingThreashold = checkNotNull(flappingThreashold);
    }

    BackoffStrategy getBackoff() {
      return backoff;
    }

    Amount<Long, Time> getFlappingThreashold() {
      return flappingThreashold;
    }
  }
}

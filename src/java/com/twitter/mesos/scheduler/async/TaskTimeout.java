package com.twitter.mesos.scheduler.async;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.StatImpl;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.Clock;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.StateManager;
import com.twitter.mesos.scheduler.events.PubsubEvent.EventSubscriber;
import com.twitter.mesos.scheduler.events.PubsubEvent.StorageStarted;
import com.twitter.mesos.scheduler.events.PubsubEvent.TaskStateChange;
import com.twitter.mesos.scheduler.storage.Storage;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Observes task transitions and identifies tasks that are 'stuck' in a transient state.  Stuck
 * tasks will be transitioned to the LOST state.
 */
class TaskTimeout implements EventSubscriber {
  private static final Logger LOG = Logger.getLogger(TaskTimeout.class.getName());

  @VisibleForTesting
  static final Optional<String> TIMEOUT_MESSAGE = Optional.of("Task timed out");

  private static final Set<ScheduleStatus> TRANSIENT_STATES = EnumSet.of(
      ScheduleStatus.ASSIGNED,
      ScheduleStatus.PREEMPTING,
      ScheduleStatus.RESTARTING,
      ScheduleStatus.KILLING,
      ScheduleStatus.UPDATING,
      ScheduleStatus.ROLLBACK);

  @VisibleForTesting
  static final TaskQuery TRANSIENT_QUERY = Query.byStatus(TRANSIENT_STATES);

  private final Map<String, Context> futures = Maps.newConcurrentMap();

  private final Storage storage;
  private final ScheduledExecutorService executor;
  private final StateManager stateManager;
  private final long timeoutMillis;
  private final Clock clock;

  @Inject
  TaskTimeout(
      Storage storage,
      ScheduledExecutorService executor,
      StateManager stateManager,
      final Clock clock,
      Amount<Long, Time> timeout) {

    this.storage = checkNotNull(storage);
    this.executor = checkNotNull(executor);
    this.stateManager = checkNotNull(stateManager);
    this.timeoutMillis = timeout.as(Time.MILLISECONDS);
    this.clock = checkNotNull(clock);

    exportStats();
  }

  private void maybeCancel(String taskId) {
    Context context = futures.remove(taskId);
    if (context != null) {
      LOG.fine("Canceling state timeout for task " + taskId);
      context.future.cancel(false);
    }
  }

  private void registerTimeout(String taskId, ScheduleStatus presentStatus, long timestampMillis) {
    maybeCancel(taskId);

    long timeElapsed = Math.max(0, clock.nowMillis() - timestampMillis);
    long timeoutRemaining = Math.max(0, timeoutMillis - timeElapsed);

    LOG.fine("Timing out task " + taskId + " in " + timeoutRemaining + " ms.");
    Future<?> timeoutHandler = executor.schedule(
        new TimedOutTaskHandler(taskId),
        timeoutRemaining,
        TimeUnit.MILLISECONDS);
    futures.put(taskId, new Context(presentStatus, timestampMillis, timeoutHandler));
  }

  private static boolean isTransient(ScheduleStatus status) {
    return TRANSIENT_STATES.contains(status);
  }

  @Subscribe
  public void recordStateChange(TaskStateChange change) {
    String taskId = change.getTaskId();
    ScheduleStatus newState = change.getNewState();
    if (isTransient(change.getOldState())) {
      maybeCancel(taskId);
    }

    if (isTransient(newState)) {
      registerTimeout(taskId, newState, clock.nowMillis());
    }
  }

  @Subscribe
  public void storageStarted(StorageStarted event) {
    for (ScheduledTask task : Storage.Util.fetchTasks(storage, TRANSIENT_QUERY)) {
      registerTimeout(
          Tasks.id(task),
          task.getStatus(),
          Iterables.getLast(task.getTaskEvents()).getTimestamp());
    }
  }

  private class TimedOutTaskHandler implements Runnable {
    final String taskId;

    TimedOutTaskHandler(String taskId) {
      this.taskId = taskId;
    }

    @Override public void run() {
      Context context = futures.get(taskId);
      try {
        if (context == null) {
          LOG.warning("Timeout context not found for " + taskId);
          return;
        }

        LOG.info("Timeout reached for task " + taskId);
        // This query acts as a CAS by including the state that we expect the task to be in if the
        // timeout is still valid.  Ideally, the future would have already been canceled, but in the
        // event of a state transition race, including transientState prevents an unintended
        // task timeout.
        TaskQuery query = Query.taskScoped(taskId).byStatus(context.transientState).get();
        // Note: This requires LOST transitions trigger Driver.killTask.
        stateManager.changeState(query, ScheduleStatus.LOST, TIMEOUT_MESSAGE);
      } finally {
        futures.remove(taskId);
      }
    }
  }

  private class Context {
    final ScheduleStatus transientState;
    final long timestampMillis;
    final Future<?> future;

    Context(ScheduleStatus transientState, long timestampMillis, Future<?> future) {
      this.transientState = transientState;
      this.timestampMillis = timestampMillis;
      this.future = future;
    }
  }

  private static final Function<Context, ScheduleStatus> CONTEXT_STATUS =
      new Function<Context, ScheduleStatus>() {
        @Override public ScheduleStatus apply(Context context) {
          return context.transientState;
        }
      };

  private static final Function<Context, Long> CONTEXT_TIMESTAMP = new Function<Context, Long>() {
    @Override public Long apply(Context context) {
      return context.timestampMillis;
    }
  };

  private static final Ordering<Context> TIMESTAMP_ORDER =
      Ordering.natural().onResultOf(CONTEXT_TIMESTAMP);

  @VisibleForTesting
  static final String TRANSIENT_COUNT_STAT_NAME = "transient_states";

  @VisibleForTesting
  static String waitingTimeStatName(ScheduleStatus status) {
    return "scheduler_max_" + status + "_waiting_ms";
  }

  private void exportStats() {
    Stats.exportSize(TRANSIENT_COUNT_STAT_NAME, futures);
    for (final ScheduleStatus status : TRANSIENT_STATES) {
      Stats.export(new StatImpl<Long>(waitingTimeStatName(status)) {
        final Predicate<Context> statusMatcher =
            Predicates.compose(Predicates.equalTo(status), CONTEXT_STATUS);

        @Override public Long read() {
          Iterable<Context> matches = Iterables.filter(futures.values(), statusMatcher);
          if (Iterables.isEmpty(matches)) {
            return 0L;
          } else {
            return clock.nowMillis() - TIMESTAMP_ORDER.min(matches).timestampMillis;
          }
        }
      });
    }
  }
}

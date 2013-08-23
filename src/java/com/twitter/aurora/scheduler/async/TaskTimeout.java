package com.twitter.aurora.scheduler.async;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;

import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import com.twitter.aurora.scheduler.events.PubsubEvent.StorageStarted;
import com.twitter.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import com.twitter.aurora.scheduler.state.StateManager;
import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.StatImpl;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.Clock;

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
  static final Query.Builder TRANSIENT_QUERY = Query.unscoped().byStatus(TRANSIENT_STATES);

  private final Map<TimeoutKey, Context> futures = Maps.newConcurrentMap();

  private static final class TimeoutKey {
    private final String taskId;
    private final ScheduleStatus status;

    private TimeoutKey(String taskId, ScheduleStatus status) {
      this.taskId = taskId;
      this.status = status;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(taskId, status);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof TimeoutKey)) {
        return false;
      }
      TimeoutKey key = (TimeoutKey) o;
      return Objects.equal(taskId, key.taskId)
          && (status == key.status);
    }

    @Override
    public String toString() {
      return taskId + ":" + status;
    }
  }

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

  private void registerTimeout(TimeoutKey key, long timestampMillis) {
    long timeElapsed = Math.max(0, clock.nowMillis() - timestampMillis);
    long timeoutRemaining = Math.max(0, timeoutMillis - timeElapsed);

    LOG.fine("Timing out task " + key.taskId + " in " + timeoutRemaining + " ms.");
    // This is an obvious check-then-act, but:
    //   - there isn't much of a better option, given that we have to get the Future before
    //     inserting into the map
    //   - a key collision only happens in practice if something is wrong externally to this class
    //     (double event for the same state)
    //   - the outcome is low-risk, we would wind up with a redundant Future that will eventually
    //     no-op
    if (!futures.containsKey(key)) {
      Future<?> timeoutHandler = executor.schedule(
          new TimedOutTaskHandler(key),
          timeoutRemaining,
          TimeUnit.MILLISECONDS);
      futures.put(key, new Context(timestampMillis, timeoutHandler));
    }
  }

  private static boolean isTransient(ScheduleStatus status) {
    return TRANSIENT_STATES.contains(status);
  }

  @Subscribe
  public void recordStateChange(TaskStateChange change) {
    String taskId = change.getTaskId();
    ScheduleStatus newState = change.getNewState();
    if (isTransient(change.getOldState())) {
      TimeoutKey oldKey = new TimeoutKey(taskId, change.getOldState());
      Context context = futures.remove(oldKey);
      if (context != null) {
        LOG.fine("Canceling state timeout for task " + oldKey);
        context.future.cancel(false);
      }
    }

    if (isTransient(newState)) {
      registerTimeout(new TimeoutKey(taskId, change.getNewState()), clock.nowMillis());
    }
  }

  @Subscribe
  public void storageStarted(StorageStarted event) {
    for (ScheduledTask task : Storage.Util.consistentFetchTasks(storage, TRANSIENT_QUERY)) {
      registerTimeout(
          new TimeoutKey(Tasks.id(task), task.getStatus()),
          Iterables.getLast(task.getTaskEvents()).getTimestamp());
    }
  }

  private class TimedOutTaskHandler implements Runnable {
    private final TimeoutKey key;

    TimedOutTaskHandler(TimeoutKey key) {
      this.key = key;
    }

    @Override public void run() {
      Context context = futures.get(key);
      try {
        if (context == null) {
          LOG.warning("Timeout context not found for " + key);
          return;
        }

        LOG.info("Timeout reached for task " + key);
        // This query acts as a CAS by including the state that we expect the task to be in if the
        // timeout is still valid.  Ideally, the future would have already been canceled, but in the
        // event of a state transition race, including transientState prevents an unintended
        // task timeout.
        Query.Builder query = Query.taskScoped(key.taskId).byStatus(key.status);
        // Note: This requires LOST transitions trigger Driver.killTask.
        stateManager.changeState(query, ScheduleStatus.LOST, TIMEOUT_MESSAGE);
      } finally {
        futures.remove(key);
      }
    }
  }

  private class Context {
    private final long timestampMillis;
    private final Future<?> future;

    Context(long timestampMillis, Future<?> future) {
      this.timestampMillis = timestampMillis;
      this.future = future;
    }
  }

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
        private final Predicate<TimeoutKey> statusMatcher = new Predicate<TimeoutKey>() {
          @Override public boolean apply(TimeoutKey key) {
            return key.status == status;
          }
        };

        @Override public Long read() {
          Iterable<Context> matches = Maps.filterKeys(futures, statusMatcher).values();
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

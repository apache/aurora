package com.twitter.mesos.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.BackoffStrategy;
import com.twitter.mesos.scheduler.events.TaskPubsubEvent.Deleted;
import com.twitter.mesos.scheduler.events.TaskPubsubEvent.EventSubscriber;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Backoff information used to inject a delay when rescheduling tasks that are restarting
 * frequently.
 * <p>
 * To accompish this, an internal cache of penalties is maintained.  A task obtains a penalty when
 * it needs to be rescheduled.  If the task needs to be rescheduled again within the penalty period,
 * it will not be eligible for rescheduling until the penalty period expires.  Additionally, the
 * penalty period is increased.  This process repeats, with an upper bound on the penalty period.
 *
 * TODO(William Farner): Surface a task's penalty on the job page when integrating this.
 */
public class ScheduleBackoff implements EventSubscriber {

  static final Amount<Long, Time> MAX_PENALTY = Amount.of(1L, Time.MINUTES);

  private final Ticker ticker;
  private final BackoffStrategy strategy;
  private final Cache<String, Penalty> taskPenalties;

  @VisibleForTesting
  ScheduleBackoff(Ticker ticker, BackoffStrategy strategy) {
    this.ticker = checkNotNull(ticker);
    this.strategy = checkNotNull(strategy);
    taskPenalties = CacheBuilder.newBuilder()
        .ticker(ticker)
        .expireAfterWrite(MAX_PENALTY.getValue(), MAX_PENALTY.getUnit().getTimeUnit())
        .build();
    Stats.exportSize("schedule_backoff_penalties", taskPenalties);
  }

  @Inject
  ScheduleBackoff(BackoffStrategy strategy) {
    this(Ticker.systemTicker(), strategy);
  }

  /**
   * Tests whether a task is schedulable.
   * A task is considered schedulable if it has no active penalties against it, or it has waited
   * beyond its current penalty.
   *
   * @param taskId Task to test.
   * @return {@code true} if the task may be scheduled, {@code false} otherwise.
   */
  public boolean isSchedulable(String taskId) {
    Penalty penalty = taskPenalties.getIfPresent(taskId);
    if (penalty != null) {
      if (ticker.read() >= penalty.endTime) {
        taskPenalties.invalidate(taskId);
        return true;
      } else {
        return false;
      }
    }

    return true;
  }

  /**
   * Removes penalties associated with tasks that have been deleted.
   *
   * @param deleted Task deleted notification.
   */
  @Subscribe
  public void onDeleted(Deleted deleted) {
    taskPenalties.invalidateAll(deleted.getTaskIds());
  }

  /**
   * Marks tasks as having been rescheduled, creating a penalty for the task if none exits, or
   * updating the existing penalty.
   *
   * TODO(William Farner): This event is not yet published, publish it.
   *
   * @param taskId Task that was rescheduled.
   */
  @Subscribe
  public void onRescheduled(String taskId) {
    Penalty penalty = taskPenalties.getIfPresent(taskId);
    long waitTimeNs;
    if (penalty != null) {
      waitTimeNs = strategy.calculateBackoffMs(penalty.endTime - penalty.startTime);
    } else {
      waitTimeNs = strategy.calculateBackoffMs(0);
    }

    // To be internally consistent with the cache expiration period, ignore any wait times provided
    // by the strategy that exceeds the internally-used maximum.
    waitTimeNs = Math.min(waitTimeNs, MAX_PENALTY.as(Time.NANOSECONDS));
    taskPenalties.put(taskId, new Penalty(ticker.read(), ticker.read() + waitTimeNs));
  }

  private static class Penalty {
    final long startTime;
    final long endTime;

    Penalty(long startTime, long endTime) {
      this.startTime = startTime;
      this.endTime = endTime;
    }
  }
}

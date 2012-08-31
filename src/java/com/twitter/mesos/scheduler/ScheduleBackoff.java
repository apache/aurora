package com.twitter.mesos.scheduler;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.eventbus.Subscribe;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.BackoffStrategy;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.events.TaskPubsubEvent.EventSubscriber;
import com.twitter.mesos.scheduler.events.TaskPubsubEvent.Rescheduled;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Backoff information used to inject a delay when rescheduling tasks that are restarting
 * frequently.
 * <p>
 * To accompish this, an internal cache of penalties is maintained.  A task obtains a penalty when
 * it needs to be rescheduled.  If the task needs to be rescheduled again within the penalty period,
 * it will not be eligible for rescheduling until the penalty period expires.  Additionally, the
 * penalty period is increased.  This process repeats, with an upper bound on the penalty period.
 */
public interface ScheduleBackoff extends EventSubscriber {

  /**
   * Tests whether a task is schedulable based on any outstanding penalties against it.
   * A task is considered schedulable if it has no active penalties against it, or it has waited
   * beyond its current penalty.
   *
   * @param task Task to test.
   * @return {@code true} if the task may be scheduled, {@code false} otherwise.
   */
  boolean isSchedulable(TwitterTaskInfo task);

  static class ScheduleBackoffImpl implements ScheduleBackoff {
    private final Ticker ticker;
    private final BackoffStrategy strategy;
    private final Cache<String, Penalty> taskPenalties;
    private final Amount<Long, Time> maxBackoff;

    @BindingAnnotation
    @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
    public @interface Backoff { }

    @VisibleForTesting
    ScheduleBackoffImpl(Ticker ticker, BackoffStrategy strategy, Amount<Long, Time> maxBackoff) {
      this.ticker = checkNotNull(ticker);
      this.strategy = checkNotNull(strategy);
      this.maxBackoff = checkNotNull(maxBackoff);
      taskPenalties = CacheBuilder.newBuilder()
          .ticker(ticker)
          .expireAfterWrite(maxBackoff.getValue() * 2, maxBackoff.getUnit().getTimeUnit())
          .build();
      Stats.exportSize("schedule_backoff_penalties", taskPenalties);
    }

    @Inject
    ScheduleBackoffImpl(BackoffStrategy strategy, @Backoff Amount<Long, Time> maxBackoff) {
      this(Ticker.systemTicker(), strategy, maxBackoff);
    }

    @Override
    public boolean isSchedulable(TwitterTaskInfo task) {
      String key = makeKey(task);
      Penalty penalty = taskPenalties.getIfPresent(key);
      if (penalty != null) {
        if (penalty.canSchedule()) {
          if (penalty.isExpired()) {
            taskPenalties.invalidate(key);
          }
          return true;
        } else {
          return false;
        }
      }

      return true;
    }

    /**
     * Marks tasks as having been rescheduled, creating a penalty for the task if none exits, or
     * updating the existing penalty.
     *
     * @param rescheduled Rescheduled notification.
     */
    @Subscribe
    public void onRescheduled(Rescheduled rescheduled) {
      String key = makeKey(rescheduled);
      Penalty penalty = taskPenalties.getIfPresent(key);
      long waitTimeNs;
      if ((penalty != null) && !penalty.isExpired()) {
        waitTimeNs = strategy.calculateBackoffMs(penalty.getDuration());
      } else {
        waitTimeNs = strategy.calculateBackoffMs(0);
      }

      // To be internally consistent with the cache expiration period, ignore any wait times
      // provided by the strategy that exceeds the internally-used maximum.
      waitTimeNs = Math.min(waitTimeNs, maxBackoff.as(Time.NANOSECONDS));
      taskPenalties.put(key, new Penalty(ticker.read(), waitTimeNs));
    }

    private static String makeKey(Rescheduled rescheduled) {
      return makeKey(rescheduled.getRole(), rescheduled.getJob(), rescheduled.getShard());
    }

    private static String makeKey(TwitterTaskInfo task) {
      return makeKey(task.getOwner().getRole(), task.getJobName(), task.getShardId());
    }

    private static String makeKey(String role, String job, int shard) {
      return String.format("%s/%s/%s", role, job, shard);
    }

    private class Penalty {
      final long startTime;
      final long scheduleReady;
      final long endTime;

      Penalty(long startTime, long duration) {
        this.startTime = startTime;
        this.scheduleReady = startTime + duration;
        this.endTime = scheduleReady + duration;
      }

      long getDuration() {
        return scheduleReady - startTime;
      }

      boolean canSchedule() {
        return ticker.read() > scheduleReady;
      }

      boolean isExpired() {
        return ticker.read() > endTime;
      }
    }
  }
}

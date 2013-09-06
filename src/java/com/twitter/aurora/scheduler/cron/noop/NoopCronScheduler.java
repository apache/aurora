package com.twitter.aurora.scheduler.cron.noop;

import java.util.Collections;
import java.util.Set;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import com.twitter.aurora.scheduler.cron.CronException;
import com.twitter.aurora.scheduler.cron.CronScheduler;

/**
 * A cron scheduler that accepts cron jobs but never runs them. Useful if you want to hook up an
 * external triggering mechanism (e.g. a system cron job that calls the startCronJob RPC manually
 * on an interval).
 *
 * This class exists as a short term hack to get around a license compatibility issue - Real
 * Implementation (TM) coming soon.
 */
class NoopCronScheduler implements CronScheduler {
  private static final Logger LOG = Logger.getLogger(NoopCronScheduler.class.getName());

  // Keep a list of schedules we've seen.
  private final Set<String> schedules = Collections.synchronizedSet(Sets.<String>newHashSet());

  @Override
  public String schedule(String schedule, Runnable task) {
    schedules.add(schedule);

    LOG.warning(String.format(
        "NO-OP cron scheduler is in use! %s with schedule %s WILL NOT be automatically triggered!",
        task,
        schedule));

    return schedule;
  }

  @Override
  public void deschedule(String key) throws IllegalStateException {
    schedules.remove(key);
  }

  @Override
  public Optional<String> getSchedule(String key) throws IllegalStateException {
    return schedules.contains(key)
        ? Optional.of(key)
        : Optional.<String>absent();
  }

  @Override
  public void start() throws IllegalStateException {
    LOG.warning("NO-OP cron scheduler is in use. Cron jobs submitted will not be triggered!");
  }

  @Override
  public void stop() throws CronException {
    // No-op.
  }

  @Override
  public boolean isValidSchedule(@Nullable String schedule) {
    // Accept everything.
    return schedule != null;
  }
}

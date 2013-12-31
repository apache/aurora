/**
 * Copyright 2013 Apache Software Foundation
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
package org.apache.aurora.scheduler.cron.noop;

import java.util.Collections;
import java.util.Set;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import org.apache.aurora.scheduler.cron.CronException;
import org.apache.aurora.scheduler.cron.CronScheduler;

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

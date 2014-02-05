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
package org.apache.aurora.scheduler.cron;

import javax.annotation.Nullable;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.Service;

/**
 * An execution manager that executes work on a cron schedule.
 */
public interface CronScheduler extends Service {
  /**
   * Schedules a task on a cron schedule.
   *
   * @param schedule Cron-style schedule.
   * @param task Work to run when on the cron schedule.
   * @return A unique ID to identify the scheduled cron task.
   * @throws CronException when there was a failure to schedule, for example if {@code schedule}
   *         is not a valid input.
   * @throws IllegalStateException If the cron scheduler is not currently running.
   */
  String schedule(String schedule, Runnable task) throws CronException, IllegalStateException;

  /**
   * Removes a scheduled cron item.
   *
   * @param key Key previously returned from {@link #schedule(String, Runnable)}.
   * @throws IllegalStateException If the cron scheduler is not currently running.
   */
  void deschedule(String key) throws IllegalStateException;

  /**
   * Gets the cron schedule associated with a scheduling key.
   *
   * @param key Key previously returned from {@link #schedule(String, Runnable)}.
   * @return The task's cron schedule, if a matching task was found.
   * @throws IllegalStateException If the cron scheduler is not currently running.
   */
  Optional<String> getSchedule(String key) throws IllegalStateException;

  /**
   * Checks to see if the scheduler would be accepted by the underlying scheduler.
   *
   * @param schedule Cron scheduler to validate.
   * @return {@code true} if the schedule is valid.
   */
  boolean isValidSchedule(@Nullable String schedule);
}

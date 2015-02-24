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
package org.apache.aurora.scheduler.cron;

import java.util.Map;

import org.apache.aurora.scheduler.storage.entities.IJobKey;

/**
 * Manages the persistence and scheduling of jobs that should be run periodically on a cron
 * schedule.
 */
public interface CronJobManager {
  /**
   * Triggers execution of a job.
   *
   * @param jobKey Key of the job to start.
   * @throws CronException If the job could not be started with the cron system.
   */
  void startJobNow(IJobKey jobKey) throws CronException;

  /**
   * Persist a new cron job to storage and schedule it for future execution.
   *
   * @param config Cron job configuration to update to.
   * @throws CronException If a job with the same key does not exist or the job could not be
   * scheduled.
   */
  void updateJob(SanitizedCronJob config) throws CronException;

  /**
   * Persist a cron job to storage and schedule it for future execution.
   *
   * @param config New cron job configuration.
   * @throws CronException If a job with the same key exists or the job could not be scheduled.
   */
  void createJob(SanitizedCronJob config) throws CronException;

  /**
   * Remove a job and deschedule it.
   *
   * @param jobKey Key of the job to delete.
   * @return true if a job was removed.
   */
  boolean deleteJob(IJobKey jobKey);

  /**
   * A list of the currently scheduled jobs and when they will run according to the underlying
   * execution engine.
   *
   * @return A map from job to the cron schedule in use for that job.
   */
  Map<IJobKey, CrontabEntry> getScheduledJobs();
}

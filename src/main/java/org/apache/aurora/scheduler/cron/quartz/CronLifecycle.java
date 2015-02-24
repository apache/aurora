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
package org.apache.aurora.scheduler.cron.quartz;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.util.concurrent.AbstractIdleService;
import com.twitter.common.stats.Stats;

import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.cron.CronException;
import org.apache.aurora.scheduler.cron.SanitizedCronJob;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import static java.util.Objects.requireNonNull;

/**
 * Manager for startup and teardown of Quartz scheduler.
 */
class CronLifecycle extends AbstractIdleService {
  private static final Logger LOG = Logger.getLogger(CronLifecycle.class.getName());

  private static final AtomicInteger RUNNING_FLAG = Stats.exportInt("quartz_scheduler_running");
  private static final AtomicInteger LOADED_FLAG = Stats.exportInt("cron_jobs_loaded");
  private static final AtomicLong LAUNCH_FAILURES = Stats.exportLong("cron_job_launch_failures");

  private final Scheduler scheduler;
  private final CronJobManagerImpl cronJobManager;
  private final Storage storage;

  @Inject
  CronLifecycle(Scheduler scheduler, CronJobManagerImpl cronJobManager, Storage storage) {
    this.scheduler = requireNonNull(scheduler);
    this.cronJobManager = requireNonNull(cronJobManager);
    this.storage = requireNonNull(storage);
  }

  @Override
  protected void startUp() throws SchedulerException {
    LOG.info("Starting Quartz cron scheduler" + scheduler.getSchedulerName() + ".");
    scheduler.start();
    RUNNING_FLAG.set(1);

    for (IJobConfiguration job : Storage.Util.fetchCronJobs(storage)) {
      try {
        SanitizedCronJob cronJob = SanitizedCronJob.fromUnsanitized(job);
        cronJobManager.scheduleJob(
            cronJob.getCrontabEntry(),
            cronJob.getSanitizedConfig().getJobConfig().getKey());
      } catch (CronException | ConfigurationManager.TaskDescriptionException e) {
        logLaunchFailure(job, e);
      }
    }
    LOADED_FLAG.set(1);
  }

  private void logLaunchFailure(IJobConfiguration job, Exception e) {
    LAUNCH_FAILURES.incrementAndGet();
    LOG.log(Level.SEVERE, "Scheduling failed for recovered job " + job, e);
  }

  @Override
  protected void shutDown() throws SchedulerException {
    LOG.info("Shutting down Quartz cron scheduler.");
    scheduler.shutdown();
    RUNNING_FLAG.set(0);
  }
}

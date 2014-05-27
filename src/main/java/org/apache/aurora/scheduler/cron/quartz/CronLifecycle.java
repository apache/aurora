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

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;
import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.base.Command;
import com.twitter.common.stats.Stats;

import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.cron.CronException;
import org.apache.aurora.scheduler.cron.SanitizedCronJob;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Manager for startup and teardown of Quartz scheduler.
 */
class CronLifecycle extends AbstractIdleService implements PubsubEvent.EventSubscriber {
  private static final Logger LOG = Logger.getLogger(CronLifecycle.class.getName());

  private static final AtomicInteger RUNNING_FLAG = Stats.exportInt("quartz_scheduler_running");
  private static final AtomicInteger LOADED_FLAG = Stats.exportInt("cron_jobs_loaded");
  private static final AtomicLong LAUNCH_FAILURES = Stats.exportLong("cron_job_launch_failures");

  private final Scheduler scheduler;
  private final ShutdownRegistry shutdownRegistry;
  private final CronJobManagerImpl cronJobManager;

  @Inject
  CronLifecycle(
      Scheduler scheduler,
      ShutdownRegistry shutdownRegistry,
      CronJobManagerImpl cronJobManager) {

    this.scheduler = checkNotNull(scheduler);
    this.shutdownRegistry = checkNotNull(shutdownRegistry);
    this.cronJobManager = checkNotNull(cronJobManager);
  }

  /**
   * Notifies the cronScheduler job manager that the scheduler is active, and job configurations
   * are ready to load.
   *
   * @param schedulerActive Event.
   */
  @Subscribe
  public void schedulerActive(PubsubEvent.SchedulerActive schedulerActive) {
    startAsync();
    shutdownRegistry.addAction(new Command() {
      @Override
      public void execute() {
        CronLifecycle.this.stopAsync().awaitTerminated();
      }
    });
    awaitRunning();
  }

  @Override
  protected void startUp() throws SchedulerException {
    LOG.info("Starting Quartz cron scheduler" + scheduler.getSchedulerName() + ".");
    scheduler.start();
    RUNNING_FLAG.set(1);

    // TODO(ksweeney): Refactor the interface - we really only need the job keys here.
    for (IJobConfiguration job : cronJobManager.getJobs()) {
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

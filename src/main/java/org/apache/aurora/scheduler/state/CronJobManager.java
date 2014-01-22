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
package org.apache.aurora.scheduler.state;

import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.base.ExceptionalCommand;
import com.twitter.common.base.Supplier;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.BackoffHelper;

import org.apache.aurora.gen.CronCollisionPolicy;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.ScheduleException;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import org.apache.aurora.scheduler.configuration.SanitizedConfiguration;
import org.apache.aurora.scheduler.cron.CronException;
import org.apache.aurora.scheduler.cron.CronScheduler;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.SchedulerActive;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.Work;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.commons.lang.StringUtils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A job scheduler that receives jobs that should be run periodically on a cron schedule.
 */
public class CronJobManager extends JobManager implements EventSubscriber {

  public static final String MANAGER_KEY = "CRON";

  @VisibleForTesting
  static final String CRON_USER = "cron";

  private static final Logger LOG = Logger.getLogger(CronJobManager.class.getName());

  @CmdLine(name = "cron_start_initial_backoff", help =
      "Initial backoff delay while waiting for a previous cron run to start.")
  private static final Arg<Amount<Long, Time>> CRON_START_INITIAL_BACKOFF =
      Arg.create(Amount.of(1L, Time.SECONDS));

  @CmdLine(name = "cron_start_max_backoff", help =
      "Max backoff delay while waiting for a previous cron run to start.")
  private static final Arg<Amount<Long, Time>> CRON_START_MAX_BACKOFF =
      Arg.create(Amount.of(1L, Time.MINUTES));

  private final AtomicLong cronJobsTriggered = Stats.exportLong("cron_jobs_triggered");
  private final AtomicLong cronJobLaunchFailures = Stats.exportLong("cron_job_launch_failures");

  // Maps from the unique job identifier to the unique identifier used internally by the cron
  // scheduler.
  private final Map<IJobKey, String> scheduledJobs =
      Collections.synchronizedMap(Maps.<IJobKey, String>newHashMap());

  // Prevents runs from dogpiling while waiting for a run to transition out of the KILLING state.
  // This is necessary because killing a job (if dictated by cron collision policy) is an
  // asynchronous operation.
  private final Map<IJobKey, SanitizedConfiguration> pendingRuns =
      Collections.synchronizedMap(Maps.<IJobKey, SanitizedConfiguration>newHashMap());

  private final StateManager stateManager;
  private final Storage storage;
  private final CronScheduler cron;
  private final ShutdownRegistry shutdownRegistry;
  private final BackoffHelper delayedStartBackoff;
  private final Executor delayedRunExecutor;

  @Inject
  CronJobManager(
      StateManager stateManager,
      Storage storage,
      CronScheduler cron,
      ShutdownRegistry shutdownRegistry) {

    this(
        stateManager,
        storage,
        cron,
        shutdownRegistry,
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("CronDelay-%d").build()));
  }

  @VisibleForTesting
  CronJobManager(
      StateManager stateManager,
      Storage storage,
      CronScheduler cron,
      ShutdownRegistry shutdownRegistry,
      Executor delayedRunExecutor) {

    this.stateManager = checkNotNull(stateManager);
    this.storage = checkNotNull(storage);
    this.cron = checkNotNull(cron);
    this.shutdownRegistry = checkNotNull(shutdownRegistry);
    this.delayedStartBackoff =
        new BackoffHelper(CRON_START_INITIAL_BACKOFF.get(), CRON_START_MAX_BACKOFF.get());
    this.delayedRunExecutor = checkNotNull(delayedRunExecutor);

    Stats.exportSize("cron_num_pending_runs", pendingRuns);
  }

  private void mapScheduledJob(SanitizedCronJob cronJob) throws ScheduleException {
    IJobKey jobKey = cronJob.config.getJobConfig().getKey();
    synchronized (scheduledJobs) {
      Preconditions.checkState(
          !scheduledJobs.containsKey(jobKey),
          "Illegal state - cron schedule already exists for " + JobKeys.toPath(jobKey));
      scheduledJobs.put(jobKey, scheduleJob(cronJob));
    }
  }

  /**
   * Notifies the cron job manager that the scheduler is active, and job configurations are ready to
   * load.
   *
   * @param schedulerActive Event.
   */
  @Subscribe
  public void schedulerActive(SchedulerActive schedulerActive) {
    cron.start();
    shutdownRegistry.addAction(new ExceptionalCommand<CronException>() {
      @Override public void execute() throws CronException {
        cron.stop();
      }
    });

    Iterable<IJobConfiguration> crons =
        storage.consistentRead(new Work.Quiet<Iterable<IJobConfiguration>>() {
          @Override public Iterable<IJobConfiguration> apply(Storage.StoreProvider storeProvider) {
            return storeProvider.getJobStore().fetchJobs(MANAGER_KEY);
          }
        });

    for (IJobConfiguration job : crons) {
      try {
        mapScheduledJob(new SanitizedCronJob(job, cron));
      } catch (ScheduleException | TaskDescriptionException e) {
        logLaunchFailure(job, e);
      }
    }
  }

  private void logLaunchFailure(IJobConfiguration job, Exception e) {
    cronJobLaunchFailures.incrementAndGet();
    LOG.log(Level.SEVERE, "Scheduling failed for recovered job " + job, e);
  }

  /**
   * Triggers execution of a job.
   *
   * @param jobKey Key of the job to start.
   * @throws ScheduleException If the job could not be started with the cron system.
   * @throws TaskDescriptionException If the stored job associated with {@code jobKey} has field
   *         validation problems.
   */
  public void startJobNow(IJobKey jobKey) throws TaskDescriptionException, ScheduleException {
    checkNotNull(jobKey);

    Optional<IJobConfiguration> jobConfig = fetchJob(jobKey);
    checkArgument(jobConfig.isPresent(), "No such cron job " + JobKeys.toPath(jobKey));

    cronTriggered(new SanitizedCronJob(jobConfig.get(), cron));
  }

  private void delayedRun(final Query.Builder query, final SanitizedConfiguration config) {
    IJobConfiguration job = config.getJobConfig();
    final String jobPath = JobKeys.toPath(job);
    final IJobKey jobKey = job.getKey();
    LOG.info("Waiting for job to terminate before launching cron job " + jobPath);
    if (pendingRuns.put(jobKey, config) == null) {
      LOG.info("Launching a task to wait for job to finish: " + jobPath);
      // There was no run already pending for this job, launch a task to delay launch until the
      // existing run has terminated.
      delayedRunExecutor.execute(new Runnable() {
        @Override public void run() {
          runWhenTerminated(query, jobKey);
        }
      });
    }
  }

  private void runWhenTerminated(final Query.Builder query, final IJobKey jobKey) {
    try {
      delayedStartBackoff.doUntilSuccess(new Supplier<Boolean>() {
        @Override public Boolean get() {
          if (!hasTasks(query)) {
            LOG.info("Initiating delayed launch of cron " + jobKey);
            SanitizedConfiguration config = pendingRuns.remove(jobKey);
            checkNotNull(config, "Failed to fetch job for delayed run of " + jobKey);
            LOG.info("Launching " + config.getTaskConfigs().size() + " tasks.");
            stateManager.insertPendingTasks(config.getTaskConfigs());
            return true;
          } else {
            LOG.info("Not yet safe to run cron " + jobKey);
            return false;
          }
        }
      });
    } catch (InterruptedException e) {
      LOG.log(Level.WARNING, "Interrupted while trying to launch cron " + jobKey, e);
      Thread.currentThread().interrupt();
    }
  }

  private boolean hasTasks(Query.Builder query) {
    return !Storage.Util.consistentFetchTasks(storage, query).isEmpty();
  }

  public static CronCollisionPolicy orDefault(@Nullable CronCollisionPolicy policy) {
    return Optional.fromNullable(policy).or(CronCollisionPolicy.KILL_EXISTING);
  }

  /**
   * Triggers execution of a cron job, depending on the cron collision policy for the job.
   *
   * @param cronJob The job to be triggered.
   */
  private void cronTriggered(SanitizedCronJob cronJob) {
    SanitizedConfiguration config = cronJob.config;
    IJobConfiguration job = config.getJobConfig();
    LOG.info(String.format("Cron triggered for %s at %s with policy %s",
        JobKeys.toPath(job), new Date(), job.getCronCollisionPolicy()));
    cronJobsTriggered.incrementAndGet();

    ImmutableMap.Builder<Integer, ITaskConfig> builder = ImmutableMap.builder();
    final Query.Builder activeQuery = Query.jobScoped(job.getKey()).active();
    Set<IScheduledTask> activeTasks = Storage.Util.consistentFetchTasks(storage, activeQuery);

    if (activeTasks.isEmpty()) {
      builder.putAll(config.getTaskConfigs());
    } else {
      // Assign a default collision policy.
      CronCollisionPolicy collisionPolicy = orDefault(job.getCronCollisionPolicy());

      switch (collisionPolicy) {
        case KILL_EXISTING:
          try {
            schedulerCore.killTasks(activeQuery, CRON_USER);
            // Check immediately if the tasks are gone.  This could happen if the existing tasks
            // were pending.
            if (!hasTasks(activeQuery)) {
              builder.putAll(config.getTaskConfigs());
            } else {
              delayedRun(activeQuery, config);
            }
          } catch (ScheduleException e) {
            LOG.log(Level.SEVERE, "Failed to kill job.", e);
          }
          break;

        case CANCEL_NEW:
          break;

        case RUN_OVERLAP:
          LOG.severe("Ignoring trigger for job "
              + JobKeys.toPath(job)
              + " with deprecated collision policy RUN_OVERLAP due to unterminated active tasks.");
          break;

        default:
          LOG.severe("Unrecognized cron collision policy: " + job.getCronCollisionPolicy());
      }
    }

    Map<Integer, ITaskConfig> newTasks = builder.build();
    if (!newTasks.isEmpty()) {
      stateManager.insertPendingTasks(newTasks);
    }
  }

  /**
   * Updates (re-schedules) the existing cron job.
   *
   * @param config New job configuration to update to.
   * @throws ScheduleException If non-cron job confuration provided.
   */
  public void updateJob(SanitizedConfiguration config) throws ScheduleException {
    IJobConfiguration job = config.getJobConfig();
    if (!hasCronSchedule(job)) {
      throw new ScheduleException("A cron job may not be updated to a non-cron job.");
    }
    String key = scheduledJobs.remove(job.getKey());
    checkNotNull(key, "Attempted to update unknown job " + JobKeys.toPath(job));
    cron.deschedule(key);
    checkArgument(receiveJob(config));
  }

  @Override
  public String getUniqueKey() {
    return MANAGER_KEY;
  }

  private static boolean hasCronSchedule(IJobConfiguration job) {
    checkNotNull(job);
    return !StringUtils.isEmpty(job.getCronSchedule());
  }

  @Override
  public boolean receiveJob(SanitizedConfiguration config) throws ScheduleException {
    final IJobConfiguration job = config.getJobConfig();
    if (!hasCronSchedule(job)) {
      return false;
    }

    SanitizedCronJob cronJob = new SanitizedCronJob(config, cron);
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(Storage.MutableStoreProvider storeProvider) {
        storeProvider.getJobStore().saveAcceptedJob(MANAGER_KEY, job);
      }
    });
    mapScheduledJob(cronJob);

    return true;
  }

  private String scheduleJob(final SanitizedCronJob cronJob) throws ScheduleException {
    IJobConfiguration job = cronJob.config.getJobConfig();
    final String jobPath = JobKeys.toPath(job);
    LOG.info(String.format("Scheduling cron job %s: %s", jobPath, job.getCronSchedule()));
    try {
      return cron.schedule(job.getCronSchedule(), new Runnable() {
        @Override public void run() {
          // TODO(William Farner): May want to record information about job runs.
          LOG.info("Running cron job: " + jobPath);
          cronTriggered(cronJob);
        }
      });
    } catch (CronException e) {
      throw new ScheduleException("Failed to schedule cron job: " + e.getMessage(), e);
    }
  }

  @Override
  public Iterable<IJobConfiguration> getJobs() {
    return storage.consistentRead(new Work.Quiet<Iterable<IJobConfiguration>>() {
      @Override
      public Iterable<IJobConfiguration> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getJobStore().fetchJobs(MANAGER_KEY);
      }
    });
  }

  @Override
  public boolean hasJob(IJobKey jobKey) {
    return fetchJob(jobKey).isPresent();
  }

  private Optional<IJobConfiguration> fetchJob(final IJobKey jobKey) {
    checkNotNull(jobKey);
    return storage.consistentRead(new Work.Quiet<Optional<IJobConfiguration>>() {
      @Override public Optional<IJobConfiguration> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getJobStore().fetchJob(MANAGER_KEY, jobKey);
      }
    });
  }

  @Override
  public boolean deleteJob(final IJobKey jobKey) {
    checkNotNull(jobKey);

    if (!hasJob(jobKey)) {
      return false;
    }

    String scheduledJobKey = scheduledJobs.remove(jobKey);
    if (scheduledJobKey != null) {
      cron.deschedule(scheduledJobKey);
      storage.write(new MutateWork.NoResult.Quiet() {
        @Override protected void execute(Storage.MutableStoreProvider storeProvider) {
          storeProvider.getJobStore().removeJob(jobKey);
        }
      });
      LOG.info("Successfully deleted cron job " + jobKey);
    }
    return true;
  }

  private final Function<String, String> keyToSchedule = new Function<String, String>() {
    @Override public String apply(String key) {
      return cron.getSchedule(key).or("Not found.");
    }
  };

  public Map<IJobKey, String> getScheduledJobs() {
    synchronized (scheduledJobs) {
      return ImmutableMap.copyOf(Maps.transformValues(scheduledJobs, keyToSchedule));
    }
  }

  public Set<IJobKey> getPendingRuns() {
    synchronized (pendingRuns) {
      return ImmutableSet.copyOf(pendingRuns.keySet());
    }
  }

  /**
   * Used by functions that expect field validation before being called.
   */
  private static class SanitizedCronJob {
    private final SanitizedConfiguration config;

    SanitizedCronJob(IJobConfiguration unsanitized, CronScheduler cron)
        throws ScheduleException, TaskDescriptionException {

      this(SanitizedConfiguration.fromUnsanitized(unsanitized), cron);
    }

    SanitizedCronJob(SanitizedConfiguration config, CronScheduler cron) throws ScheduleException {
      final IJobConfiguration job = config.getJobConfig();
      if (!hasCronSchedule(job)) {
        throw new ScheduleException(
            String.format("Not a valid cronjob, %s has no cron schedule", JobKeys.toPath(job)));
      }

      if (!cron.isValidSchedule(job.getCronSchedule())) {
        throw new ScheduleException("Invalid cron schedule: " + job.getCronSchedule());
      }

      this.config = config;
    }
  }
}

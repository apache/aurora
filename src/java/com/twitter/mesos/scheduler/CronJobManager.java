package com.twitter.mesos.scheduler;

import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;

import org.apache.commons.lang.StringUtils;

import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.base.Command;
import com.twitter.common.base.Supplier;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.BackoffHelper;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.CronCollisionPolicy;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;

import it.sauronsoftware.cron4j.InvalidPatternException;
import it.sauronsoftware.cron4j.Predictor;
import it.sauronsoftware.cron4j.Scheduler;
import it.sauronsoftware.cron4j.SchedulingPattern;

/**
 * A job scheduler that receives jobs that should be run periodically on a cron schedule.
 *
 * TODO(William Farner): Some more work might be required here.  For example, when the cron job is
 * triggered, we may want to see if the same job is still running and allow the configuration to
 * specify the policy for when that occurs (i.e. overlap or kill the existing job).
 */
public class CronJobManager extends JobManager {

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

  // Cron manager.
  private final Scheduler scheduler = new Scheduler();

  private final AtomicLong cronJobsTriggered = Stats.exportLong("cron_jobs_triggered");

  // Maps from the our unique job identifier (<role>/<jobName>) to the unique identifier used
  // internally by the cron4j scheduler.
  private final Map<String, String> scheduledJobs =
      Collections.synchronizedMap(Maps.<String, String>newHashMap());

  // Prevents runs from dogpiling while waiting for a run to transition out of the KILLING state.
  // This is necessary because killing a job (if dictated by cron collision policy) is an
  // asynchronous operation.
  private final Map<String, JobConfiguration> pendingRuns =
      Collections.synchronizedMap(Maps.<String, JobConfiguration>newHashMap());
  private final BackoffHelper delayedStartBackoff;
  private final Executor delayedRunExecutor;

  private final Storage storage;

  @Inject
  public CronJobManager(Storage storage, ShutdownRegistry shutdownRegistry) {
    this(storage, shutdownRegistry, Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("CronDelay-%d").build()));
  }

  @VisibleForTesting
  CronJobManager(Storage storage, ShutdownRegistry shutdownRegistry,
      Executor delayedRunExecutor) {
    this.storage = Preconditions.checkNotNull(storage);
    this.delayedStartBackoff =
        new BackoffHelper(CRON_START_INITIAL_BACKOFF.get(), CRON_START_MAX_BACKOFF.get());
    this.delayedRunExecutor = Preconditions.checkNotNull(delayedRunExecutor);

    scheduler.setDaemon(true);
    scheduler.start();
    shutdownRegistry.addAction(new Command() {
      @Override public void execute() {
        scheduler.stop();
      }
    });

    Stats.exportSize("cron_num_pending_runs", pendingRuns);
  }

  private void mapScheduledJob(JobConfiguration job, String scheduledJobKey) {
    scheduledJobs.put(Tasks.jobKey(job), scheduledJobKey);
  }

  @Override
  public void start() {
    Iterable<JobConfiguration> crons =
        storage.doInTransaction(new Work.Quiet<Iterable<JobConfiguration>>() {
          @Override public Iterable<JobConfiguration> apply(Storage.StoreProvider storeProvider) {
            return storeProvider.getJobStore().fetchJobs(MANAGER_KEY);
          }
        });

    for (JobConfiguration job : crons) {
      try {
        mapScheduledJob(job, scheduleJob(job));
      } catch (ScheduleException e) {
        LOG.log(Level.SEVERE, "Scheduling failed for recovered job " + job, e);
      }
    }
  }

  /**
   * Predicts the next date at which a cron schedule will trigger.
   *
   * @param cronSchedule Cron schedule to predict the next time for.
   */
  public static Date predictNextRun(String cronSchedule) {
    return new Predictor(cronSchedule).nextMatchingDate();
  }

  /**
   * Triggers execution of a job.
   *
   * @param jobKey Key of the job to start.
   */
  public void startJobNow(final String jobKey) {
    Preconditions.checkNotNull(jobKey);

    JobConfiguration job = fetchJob(jobKey);
    Preconditions.checkArgument(job != null, "No such cron job " + jobKey);

    cronTriggered(job);
  }

  private void delayedRun(final TaskQuery query, final JobConfiguration job) {
    final String jobKey = Tasks.jobKey(job);
    LOG.info("Waiting for job to terminate before launching cron job " + jobKey);
    if (pendingRuns.put(jobKey, job) == null) {
      LOG.info("Launching a task to wait for job to finish: " + jobKey);

      // There was no run already pending for this job, launch a task to delay launch until the
      // existing run has terminated.
      delayedRunExecutor.execute(new Runnable() {
        @Override public void run() {
          runWhenTerminated(query, jobKey);
        }
      });
    }
  }

  private void runWhenTerminated(final TaskQuery query, final String jobKey) {
    try {
      delayedStartBackoff.doUntilSuccess(new Supplier<Boolean>() {
        @Override public Boolean get() {
          if (!hasTasks(query)) {
            LOG.info("Initiating delayed launch of cron " + jobKey);
            JobConfiguration job = pendingRuns.remove(jobKey);
            Preconditions.checkNotNull(job, "Failed to fetch job for delayed run of " + jobKey);
            schedulerCore.runJob(job);
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

  private boolean hasTasks(TaskQuery query) {
    return !schedulerCore.getTasks(query).isEmpty();
  }

  /**
   * Triggers execution of a cron job, depending on the cron collision policy for the job.
   *
   * @param job The config of the job to be triggered.
   */
  @VisibleForTesting
  void cronTriggered(final JobConfiguration job) {
    LOG.info(String.format("Cron triggered for %s at %s with policy %s",
        Tasks.jobKey(job), new Date(), job.getCronCollisionPolicy()));
    cronJobsTriggered.incrementAndGet();

    boolean runJob = false;

    TaskQuery query = Query.activeQuery(job.getOwner(), job.getName());

    if (!hasTasks(query)) {
      runJob = true;
    } else {
      // Assign a default collision policy.
      CronCollisionPolicy collisionPolicy = (job.getCronCollisionPolicy() == null)
          ? CronCollisionPolicy.KILL_EXISTING
          : job.getCronCollisionPolicy();

      switch (collisionPolicy) {
        case KILL_EXISTING:
          try {
            schedulerCore.killTasks(query, CRON_USER);
            // Check immediately if the tasks are gone.  This could happen if the existing tasks
            // were pending.
            if (!hasTasks(query)) {
              runJob = true;
            } else {
              delayedRun(query, job);
            }
          } catch (ScheduleException e) {
            LOG.log(Level.SEVERE, "Failed to kill job.", e);
          }
          break;

        case CANCEL_NEW:
          break;

        case RUN_OVERLAP:
          boolean hasPendingTasks = storage.doInTransaction(new Work.Quiet<Boolean>() {
            @Override public Boolean apply(StoreProvider storeProvider) {
              return !storeProvider.getTaskStore().fetchTasks(new TaskQuery()
                  .setOwner(job.getOwner())
                  .setJobName(job.getName())
                  .setStatuses(ImmutableSet.of(ScheduleStatus.PENDING))).isEmpty();
            }
          });

          if (!hasPendingTasks) {
            runJob = true;
          } else {
            LOG.info("Job " + Tasks.jobKey(job) + " has pending tasks, suppressing run.");
          }
          break;

        default:
          LOG.severe("Unrecognized cron collision policy: " + job.getCronCollisionPolicy());
      }
    }

    if (runJob) {
      schedulerCore.runJob(job);
    }
  }

  @Override
  public String getUniqueKey() {
    return MANAGER_KEY;
  }

  private static boolean hasCronSchedule(JobConfiguration job) {
    return !StringUtils.isEmpty(job.getCronSchedule());
  }

  @Override
  public boolean receiveJob(final JobConfiguration job) throws ScheduleException {
    Preconditions.checkNotNull(job);

    if (!hasCronSchedule(job)) {
      return false;
    }

    String scheduledJobKey = scheduleJob(job);
    storage.doInWriteTransaction(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(Storage.MutableStoreProvider storeProvider) {
        storeProvider.getJobStore().saveAcceptedJob(MANAGER_KEY, job);
      }
    });
    mapScheduledJob(job, scheduledJobKey);

    return true;
  }

  private String scheduleJob(final JobConfiguration job) throws ScheduleException {
    if (!hasCronSchedule(job)) {
      throw new ScheduleException(String.format("Not a valid cronjob, %s has no cron schedule",
          Tasks.jobKey(job)));
    }

    if (!validateSchedule(job.getCronSchedule())) {
      throw new ScheduleException("Invalid cron schedule: " + job.getCronSchedule());
    }

    LOG.info(String.format("Scheduling cron job %s: %s", Tasks.jobKey(job), job.getCronSchedule()));
    try {
      return scheduler.schedule(job.getCronSchedule(), new Runnable() {
        @Override public void run() {
          // TODO(William Farner): May want to record information about job runs.
          LOG.info("Running cron job: " + Tasks.jobKey(job));
          cronTriggered(job);
        }
      });
    } catch (InvalidPatternException e) {
      throw new ScheduleException("Failed to schedule cron job: " + e.getMessage(), e);
    }
  }

  private boolean validateSchedule(String cronSchedule) {
    return SchedulingPattern.validate(cronSchedule);
  }

  @Override
  public Iterable<JobConfiguration> getJobs() {
    return storage.doInTransaction(new Work.Quiet<Iterable<JobConfiguration>>() {
      @Override public Iterable<JobConfiguration> apply(Storage.StoreProvider storeProvider) {

        return storeProvider.getJobStore().fetchJobs(MANAGER_KEY);
      }
    });
  }

  @Override
  public boolean hasJob(final String jobKey) {
    Preconditions.checkNotNull(jobKey);

    return fetchJob(jobKey) != null;
  }

  private JobConfiguration fetchJob(final String jobKey) {
    return storage.doInTransaction(new Work.Quiet<JobConfiguration>() {
      @Override public JobConfiguration apply(Storage.StoreProvider storeProvider) {

        return storeProvider.getJobStore().fetchJob(MANAGER_KEY, jobKey);
      }
    });
  }

  @Override
  public boolean deleteJob(final String jobKey) {
    Preconditions.checkNotNull(jobKey);

    if (!hasJob(jobKey)) {
      return false;
    }

    String scheduledJobKey = scheduledJobs.remove(jobKey);
    if (scheduledJobKey != null) {
      scheduler.deschedule(scheduledJobKey);
      storage.doInWriteTransaction(new MutateWork.NoResult.Quiet() {
        @Override protected void execute(Storage.MutableStoreProvider storeProvider) {
          storeProvider.getJobStore().removeJob(jobKey);
        }
      });
      LOG.info("Successfully deleted cron job " + jobKey);
    }
    return true;
  }
}

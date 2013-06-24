package com.twitter.mesos.scheduler;

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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.eventbus.Subscribe;
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
import com.twitter.mesos.gen.JobKey;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.events.PubsubEvent.EventSubscriber;
import com.twitter.mesos.scheduler.events.PubsubEvent.StorageStarted;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;
import com.twitter.mesos.scheduler.storage.Storage.Work;

import it.sauronsoftware.cron4j.InvalidPatternException;
import it.sauronsoftware.cron4j.Predictor;
import it.sauronsoftware.cron4j.Scheduler;
import it.sauronsoftware.cron4j.SchedulingPattern;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.mesos.gen.ScheduleStatus.PENDING;

/**
 * A job scheduler that receives jobs that should be run periodically on a cron schedule.
 */
public class CronJobManager extends JobManager implements EventSubscriber {

  /**
   * An execution manager that executes work on a cron schedule.
   */
  public interface CronScheduler {
    /**
     * Schedules a task on a cron schedule.
     *
     * @param schedule Cron-style schedule.
     * @param task Work to run when on the cron schedule.
     * @return A unique ID to identify the scheduled cron task.
     */
    String schedule(String schedule, Runnable task);

    /**
     * Removes a scheduled cron item.
     *
     * @param key Key previously returned from {@link #schedule(String, Runnable)}.
     */
    void deschedule(String key);

    /**
     * Gets the cron schedule associated with a scheduling key.
     *
     * @param key Key previously returned from {@link #schedule(String, Runnable)}.
     * @return The task's cron schedule, if a matching task was found.
     */
    Optional<String> getSchedule(String key);

    public static class Cron4jScheduler implements CronScheduler {
      private final Scheduler scheduler;

      @Inject
      Cron4jScheduler(ShutdownRegistry shutdownRegistry) {
        checkNotNull(shutdownRegistry);
        scheduler = new Scheduler();
        scheduler.setDaemon(true);
        scheduler.start();
        shutdownRegistry.addAction(new Command() {
          @Override public void execute() {
            scheduler.stop();
          }
        });
      }

      @Override
      public String schedule(String schedule, Runnable task) {
        return scheduler.schedule(schedule, task);
      }

      @Override
      public void deschedule(String key) {
        scheduler.deschedule(key);
      }

      @Override
      public Optional<String> getSchedule(String key) {
        return Optional.fromNullable(scheduler.getSchedulingPattern(key))
            .transform(Functions.toStringFunction());
      }
    }
  }

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

  // Maps from the our unique job identifier (<role>/<jobName>) to the unique identifier used
  // internally by the cron4j scheduler.
  private final Map<String, String> scheduledJobs =
      Collections.synchronizedMap(Maps.<String, String>newHashMap());

  // Prevents runs from dogpiling while waiting for a run to transition out of the KILLING state.
  // This is necessary because killing a job (if dictated by cron collision policy) is an
  // asynchronous operation.
  private final Map<String, JobConfiguration> pendingRuns =
      Collections.synchronizedMap(Maps.<String, JobConfiguration>newHashMap());

  private final Storage storage;
  private final CronScheduler cron;
  private final BackoffHelper delayedStartBackoff;
  private final Executor delayedRunExecutor;

  @Inject
  CronJobManager(Storage storage, CronScheduler cron) {
    this(
        storage,
        cron,
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("CronDelay-%d").build()));
  }

  @VisibleForTesting
  CronJobManager(
      Storage storage,
      CronScheduler cron,
      Executor delayedRunExecutor) {
    this.storage = checkNotNull(storage);
    this.cron = checkNotNull(cron);
    this.delayedStartBackoff =
        new BackoffHelper(CRON_START_INITIAL_BACKOFF.get(), CRON_START_MAX_BACKOFF.get());
    this.delayedRunExecutor = checkNotNull(delayedRunExecutor);

    Stats.exportSize("cron_num_pending_runs", pendingRuns);
  }

  private void mapScheduledJob(JobConfiguration job, String scheduledJobKey) {
    String jobKey = Tasks.jobKey(job);
    synchronized (scheduledJobs) {
      Preconditions.checkState(
          !scheduledJobs.containsKey(jobKey),
          "Illegal state - cron schedule already exists for " + jobKey);
      scheduledJobs.put(jobKey, scheduledJobKey);
    }
  }

  /**
   * Notifies the cron job manager that storage is started, and job configurations are ready to
   * load.
   *
   * @param storageStarted Event.
   */
  @Subscribe
  public void storageStarted(StorageStarted storageStarted) {
    Iterable<JobConfiguration> crons =
        storage.consistentRead(new Work.Quiet<Iterable<JobConfiguration>>() {
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
   * @param role Owner of the job to start.
   * @param job Name of the job to start.
   */
  public void startJobNow(String role, String job) {
    String key = Tasks.jobKey(role, job);
    JobConfiguration jobConfig = fetchJob(key);
    Preconditions.checkArgument(jobConfig != null, "No such cron job " + key);

    cronTriggered(jobConfig);
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
            checkNotNull(job, "Failed to fetch job for delayed run of " + jobKey);
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
    return !Storage.Util.consistentFetchTasks(storage, query).isEmpty();
  }

  public static CronCollisionPolicy orDefault(@Nullable CronCollisionPolicy policy) {
    return Optional.fromNullable(policy).or(CronCollisionPolicy.KILL_EXISTING);
  }

  /**
   * Triggers execution of a cron job, depending on the cron collision policy for the job.
   *
   * @param job The config of the job to be triggered.
   */
  @VisibleForTesting
  void cronTriggered(JobConfiguration job) {
    LOG.info(String.format("Cron triggered for %s at %s with policy %s",
        Tasks.jobKey(job), new Date(), job.getCronCollisionPolicy()));
    cronJobsTriggered.incrementAndGet();

    Optional<JobConfiguration> runJob = Optional.absent();

    final TaskQuery activeQuery = Query
        .jobScoped(job.getOwner().getRole(), job.getName())
        .active()
        .get();

    Set<ScheduledTask> activeTasks = Storage.Util.consistentFetchTasks(storage, activeQuery);

    if (activeTasks.isEmpty()) {
      runJob = Optional.of(job);
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
              runJob = Optional.of(job);
            } else {
              delayedRun(activeQuery, job);
            }
          } catch (ScheduleException e) {
            LOG.log(Level.SEVERE, "Failed to kill job.", e);
          }
          break;

        case CANCEL_NEW:
          break;

        case RUN_OVERLAP:
          Map<Integer, ScheduledTask> byShard =
              Maps.uniqueIndex(activeTasks, Tasks.SCHEDULED_TO_SHARD_ID);
          Map<Integer, ScheduleStatus> existingTasks =
              Maps.transformValues(byShard, Tasks.GET_STATUS);
          if (existingTasks.isEmpty()) {
            runJob = Optional.of(job);
          } else if (Iterables.any(existingTasks.values(), Predicates.equalTo(PENDING))) {
            LOG.info("Job " + Tasks.jobKey(job) + " has pending tasks, suppressing run.");
          } else {
            // To safely overlap this run, we need to adjust the shard IDs of the overlapping
            // run (maintaining the role/job/shard UUID invariant).
            int shardOffset = Ordering.natural().max(existingTasks.keySet()) + 1;
            LOG.info("Adjusting shard IDs of " + Tasks.jobKey(job) + " by " + shardOffset
                + " for overlapping cron run.");
            JobConfiguration adjusted = job.deepCopy();
            for (TwitterTaskInfo task : adjusted.getTaskConfigs()) {
              task.setShardId(task.getShardId() + shardOffset);
            }

            runJob = Optional.of(adjusted);
          }
          break;

        default:
          LOG.severe("Unrecognized cron collision policy: " + job.getCronCollisionPolicy());
      }
    }

    if (runJob.isPresent()) {
      schedulerCore.runJob(runJob.get());
    }
  }

  void updateJob(JobConfiguration job) throws ScheduleException {
    if (!hasCronSchedule(job)) {
      throw new ScheduleException("A cron job may not be updated to a non-cron job.");
    }
    String key = scheduledJobs.remove(Tasks.jobKey(job));
    checkNotNull(key, "Attempted to update unknown job " + Tasks.jobKey(job));
    cron.deschedule(key);
    Preconditions.checkArgument(receiveJob(job));
  }

  @Override
  public String getUniqueKey() {
    return MANAGER_KEY;
  }

  private static boolean hasCronSchedule(JobConfiguration job) {
    checkNotNull(job);
    return !StringUtils.isEmpty(job.getCronSchedule());
  }

  @Override
  public boolean receiveJob(final JobConfiguration job) throws ScheduleException {
    if (!hasCronSchedule(job)) {
      return false;
    }

    String scheduledJobKey = scheduleJob(job);
    storage.write(new MutateWork.NoResult.Quiet() {
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
      return cron.schedule(job.getCronSchedule(), new Runnable() {
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
    return storage.consistentRead(new Work.Quiet<Iterable<JobConfiguration>>() {
      @Override public Iterable<JobConfiguration> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getJobStore().fetchJobs(MANAGER_KEY);
      }
    });
  }

  @Override
  public boolean hasJob(String role, String job) {
    return fetchJob(Tasks.jobKey(role, job)) != null;
  }

  @Override
  public boolean hasJob(JobKey jobKey) {
    // TODO(ksweeney): Remove delegation as part of MESOS-2403.
    return hasJob(jobKey.getRole(), jobKey.getName());
  }

  private JobConfiguration fetchJob(final String jobKey) {
    checkNotNull(jobKey);
    return storage.consistentRead(new Work.Quiet<JobConfiguration>() {
      @Override public JobConfiguration apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getJobStore().fetchJob(MANAGER_KEY, jobKey);
      }
    });
  }

  @Override
  public boolean deleteJob(String role, String job) {
    if (!hasJob(role, job)) {
      return false;
    }

    final String jobKey = Tasks.jobKey(role, job);
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

  public Map<String, String> getScheduledJobs() {
    synchronized (scheduledJobs) {
      return ImmutableMap.copyOf(Maps.transformValues(scheduledJobs, keyToSchedule));
    }
  }

  public Set<String> getPendingRuns() {
    synchronized (pendingRuns) {
      return ImmutableSet.copyOf(pendingRuns.keySet());
    }
  }
}

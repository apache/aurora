package com.twitter.mesos.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.twitter.common.stats.Stats;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.CronCollisionPolicy;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.scheduler.storage.JobStore;
import com.twitter.mesos.scheduler.storage.SchedulerStore;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.StorageRole;
import com.twitter.mesos.scheduler.storage.StorageRole.Role;
import com.twitter.mesos.scheduler.storage.TaskStore;
import it.sauronsoftware.cron4j.InvalidPatternException;
import it.sauronsoftware.cron4j.Scheduler;
import it.sauronsoftware.cron4j.SchedulingPattern;
import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A job scheduler that receives jobs that should be run periodically on a cron schedule.
 *
 * TODO(William Farner): Some more work might be required here.  For example, when the cron job is
 * triggered, we may want to see if the same job is still running and allow the configuration to
 * specify the policy for when that occurs (i.e. overlap or kill the existing job).
 *
 * @author William Farner
 */
public class CronJobManager extends JobManager {
  private static Logger LOG = Logger.getLogger(CronJobManager.class.getName());

  private static final String MANAGER_KEY = "CRON";

  // Cron manager.
  private final Scheduler scheduler = new Scheduler();

  private final AtomicLong cronJobsTriggered = Stats.exportLong("cron_jobs_triggered");

  // Maps from the our unique job identifier (<owner>/<jobName>) to the unique identifier used
  // internally by the cron4j scheduler.
  private final Map<String, String> scheduledJobs =
      Collections.synchronizedMap(Maps.<String, String>newHashMap());

  private final Storage storage;

  @Inject
  public CronJobManager(@StorageRole(Role.Primary) Storage storage) {
    this.storage = Preconditions.checkNotNull(storage);

    scheduler.start();
  }

  private void mapScheduledJob(JobConfiguration job, String scheduledJobKey) {
    scheduledJobs.put(Tasks.jobKey(job), scheduledJobKey);
  }

  @Override
  public void start() {
    storage.doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) {

        for (JobConfiguration job : jobStore.fetchJobs(MANAGER_KEY)) {
          try {
            String scheduledJobKey = scheduleJob(job);
            mapScheduledJob(job, scheduledJobKey);
          } catch (ScheduleException e) {
            LOG.log(Level.SEVERE, "While trying to restore state, scheduler module failed.", e);
          }
        }
      }
    });
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

  /**
   * Triggers execution of a cron job, depending on the cron collision policy for the job.
   *
   * @param job The config of the job to be triggered.
   */
  @VisibleForTesting
  void cronTriggered(JobConfiguration job) {
    LOG.info(String.format("Cron triggered for %s at %s", Tasks.jobKey(job), new Date()));
    cronJobsTriggered.incrementAndGet();

    boolean runJob = false;

    Query query = new Query(new TaskQuery().setOwner(job.getOwner()).setJobName(job.getName())
        .setStatuses(Tasks.ACTIVE_STATES));

    if (schedulerCore.getTasks(query).isEmpty()) {
      runJob = true;
    } else {
      // Assign a default collision policy.
      if (job.getCronCollisionPolicy() == null) {
        job.setCronCollisionPolicy(CronCollisionPolicy.KILL_EXISTING);
      }

      switch (job.getCronCollisionPolicy()) {
        case KILL_EXISTING:
          LOG.info("Cron collision policy requires killing existing job.");
          try {
            // TODO(William Farner): This kills the cron job itself, fix that.
            schedulerCore.killTasks(query);
            runJob = true;
          } catch (ScheduleException e) {
            LOG.log(Level.SEVERE, "Failed to kill job.", e);
          }

          break;
        case CANCEL_NEW:
          LOG.info("Cron collision policy prevented job from running.");
          break;
        case RUN_OVERLAP:
          LOG.info("Cron collision policy permitting overlapping job run.");
          runJob = true;
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
    storage.doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) {

        jobStore.saveAcceptedJob(MANAGER_KEY, job);
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

  @Override
  public JobUpdateResult updateJob(final JobConfiguration job) throws ScheduleException {
    Preconditions.checkNotNull(job);

    if (!hasCronSchedule(job) || !validateSchedule(job.getCronSchedule())) {
      throw new ScheduleException("Invalid cron schedule: " + job.getCronSchedule());
    }

    final String jobKey = Tasks.jobKey(job);
    return storage.doInTransaction(new Work<JobUpdateResult, ScheduleException>() {
      @Override public JobUpdateResult apply(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) throws ScheduleException {

        JobConfiguration existingJobConfig = jobStore.fetchJob(MANAGER_KEY, jobKey);
        Preconditions.checkState(existingJobConfig != null);

        if (job.equals(existingJobConfig)) {
          return JobUpdateResult.JOB_UNCHANGED;
        }

        deleteJob(jobKey);
        Preconditions.checkState(receiveJob(job), "Cron job manager failed to create updated job");
        return JobUpdateResult.COMPLETED;
      }
    });
  }

  private boolean validateSchedule(String cronSchedule) {
    return SchedulingPattern.validate(cronSchedule);
  }

  @Override
  public Iterable<JobConfiguration> getJobs() {
    return storage.doInTransaction(new Work.Quiet<Iterable<JobConfiguration>>() {
      @Override public Iterable<JobConfiguration> apply(SchedulerStore schedulerStore,
          JobStore jobStore, TaskStore taskStore) throws RuntimeException {

        return jobStore.fetchJobs(MANAGER_KEY);
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
      @Override public JobConfiguration apply(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) throws RuntimeException {

        return jobStore.fetchJob(MANAGER_KEY, jobKey);
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
      storage.doInTransaction(new Work.NoResult.Quiet() {
        @Override protected void execute(SchedulerStore schedulerStore, JobStore jobStore,
            TaskStore taskStore) throws RuntimeException {

          jobStore.deleteJob(jobKey);
        }
      });
      LOG.info("Successfully deleted cron job " + jobKey);
    }
    return true;
  }
}

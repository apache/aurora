package com.twitter.mesos.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.twitter.common.collections.Pair;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.CronCollisionPolicy;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.TaskQuery;
import it.sauronsoftware.cron4j.InvalidPatternException;
import it.sauronsoftware.cron4j.Scheduler;
import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A job scheduler that receives jobs that should be run periodically on a cron schedule.
 *
 * TODO(wfarner): Some more work might be required here.  For example, when the cron job is
 * triggered, we may want to see if the same job is still running and allow the configuration to
 * specify the policy for when that occurs (i.e. overlap or kill the existing job).
 *
 * @author wfarner
 */
public class CronJobManager extends JobManager {
  private static Logger LOG = Logger.getLogger(CronJobManager.class.getName());

  private static final String MANAGER_KEY = "CRON";

  private static final Function<Pair<String, JobConfiguration>, JobConfiguration> GET_JOB_COPY =
      new Function<Pair<String, JobConfiguration>, JobConfiguration>() {
        @Override public JobConfiguration apply(Pair<String, JobConfiguration> input) {
          return new JobConfiguration(input.getSecond());
        }
      };

  // Cron manager.
  private final Scheduler scheduler = new Scheduler();

  // Maps from the our unique job identifier (<owner>/<jobName>) to the unique identifier used
  // internally by the cron4j scheduler.
  private final Map<String, Pair<String, JobConfiguration>> scheduledJobs =
      Collections.synchronizedMap(Maps.<String, Pair<String, JobConfiguration>>newHashMap());

  public CronJobManager() {
    scheduler.start();
  }

  /**
   * Triggers execution of a job.
   *
   * @param jobKey Key of the job to start.
   */
  public void startJobNow(String jobKey) {
    Preconditions.checkArgument(hasJob(jobKey), "No such cron job " + jobKey);

    cronTriggered(jobKey);
  }

  /**
   * Triggers execution of a cron job, depending on the cron collision policy for the job.
   *
   * @param jobKey Key for the job triggered.
   */
  @VisibleForTesting
  void cronTriggered(String jobKey) {
    LOG.info(String.format("Cron triggered for %s at %s", jobKey, new Date()));

    JobConfiguration job = scheduledJobs.get(jobKey).getSecond();

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
            // TODO(wfarner): This kills the cron job itself, fix that.
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

    if (runJob) schedulerCore.runJob(job);
  }

  @Override
  public String getUniqueKey() {
    return MANAGER_KEY;
  }

  @Override
  public boolean receiveJob(final JobConfiguration job) throws ScheduleException {
    if (StringUtils.isEmpty(job.getCronSchedule())) return false;

    LOG.info(String.format("Scheduling cron job %s: %s", Tasks.jobKey(job), job.getCronSchedule()));
    try {
      scheduledJobs.put(Tasks.jobKey(job),
          Pair.of(scheduler.schedule(job.getCronSchedule(),
            new Runnable() {
              @Override public void run() {
                // TODO(wfarner): May want to record information about job runs.
                LOG.info("Cron job running.");
                cronTriggered(Tasks.jobKey(job));
              }
            }),
            job)
      );
    } catch (InvalidPatternException e) {
      throw new ScheduleException("Failed to schedule cron job.", e);
    }

    return true;
  }

  @Override
  public JobUpdateResult updateJob(JobConfiguration job) throws ScheduleException {
    String jobKey = Tasks.jobKey(job);
    Preconditions.checkState(hasJob(jobKey));

    if (job.equals(scheduledJobs.get(jobKey).getSecond())) {
      return JobUpdateResult.JOB_UNCHANGED;
    }

    deleteJob(jobKey);
    Preconditions.checkState(receiveJob(job), "Cron job manager failed to create updated job");
    return JobUpdateResult.COMPLETED;
  }

  @Override
  public Iterable<JobConfiguration> getJobs() {
    return Iterables.transform(scheduledJobs.values(), GET_JOB_COPY);
  }

  @Override
  public boolean hasJob(String jobKey) {
    return scheduledJobs.containsKey(jobKey);
  }

  @Override
  public boolean deleteJob(String jobKey) {
    if (!hasJob(jobKey)) return false;

    Pair<String, JobConfiguration> jobObj = scheduledJobs.remove(jobKey);

    if (jobObj != null) {
      scheduler.deschedule(jobObj.getFirst());
      LOG.info("Successfully deleted cron job " + jobKey);
    }

    return true;
  }
}

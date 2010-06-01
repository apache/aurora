package com.twitter.nexus.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.twitter.common.Pair;
import com.twitter.nexus.gen.CronCollisionPolicy;
import com.twitter.nexus.gen.JobConfiguration;
import com.twitter.nexus.gen.ScheduleStatus;
import com.twitter.nexus.gen.TaskQuery;
import it.sauronsoftware.cron4j.InvalidPatternException;
import it.sauronsoftware.cron4j.Scheduler;
import org.apache.commons.lang.StringUtils;

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

  // Cron manager.
  private final Scheduler scheduler = new Scheduler();

  // Maps from the our unique job identifier (<owner>/<jobName>) to the unique identifier used
  // internally by the cron4j scheduler.
  private final Map<String, Pair<String, JobConfiguration>> scheduledJobs = Maps.newHashMap();

  public CronJobManager() {
    scheduler.start();
  }

  /**
   * Triggers execution of a cron job, depending on the cron collision policy for the job.
   *
   * @param job Job triggered.
   */
  @VisibleForTesting
  void cronTriggered(JobConfiguration job) {
    LOG.info(String.format("Cron triggered for %s/%s at %s",
        job.getOwner(), job.getName(), new Date().toString()));

    boolean runJob = false;

    TaskQuery query = new TaskQuery().setOwner(job.getOwner()).setJobName(job.getName())
      .setStatuses(ImmutableSet.of(
          ScheduleStatus.PENDING, ScheduleStatus.STARTING, ScheduleStatus.RUNNING));

    if (Iterables.isEmpty(schedulerCore.getTasks(query))) {
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
  public boolean receiveJob(final JobConfiguration job) throws ScheduleException {
    if (StringUtils.isEmpty(job.getCronSchedule())) return false;

    LOG.info(String.format("Scheduling cron job %s/%s: %s",
        job.getOwner(), job.getName(), job.getCronSchedule()));
    try {
      scheduledJobs.put(makeKey(job),
          Pair.of(scheduler.schedule(job.getCronSchedule(),
            new Runnable() {
              @Override public void run() {
                // TODO(wfarner): May want to record information about job runs.
                LOG.info("Cron job running.");
                cronTriggered(job);
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
  public Iterable<JobConfiguration> getState() {
    return Iterables.transform(scheduledJobs.values(),
        new Function<Pair<String, JobConfiguration>, JobConfiguration>() {
      @Override public JobConfiguration apply(Pair<String, JobConfiguration> configPair) {
        return configPair.getSecond();
      }
    });
  }

  @Override
  public boolean hasJob(String owner, String job) {
    return scheduledJobs.containsKey(makeKey(owner, job));
  }

  @Override
  public boolean deleteJob(String owner, String job) {
    if (!hasJob(owner, job)) return false;

    Pair<String, JobConfiguration> jobObj = scheduledJobs.remove(makeKey(owner, job));

    if (jobObj!= null) {
      scheduler.deschedule(jobObj.getFirst());
      LOG.info("Successfully deleted cron job for " + makeKey(owner, job));
    }

    return true;
  }

  public Iterable<JobConfiguration> getJobs() {
    return Iterables.transform(scheduledJobs.values(),
        new Function<Pair<String, JobConfiguration>, JobConfiguration>() {
      @Override public JobConfiguration apply(Pair<String, JobConfiguration> jobObj) {
        return jobObj.getSecond();
      }
    });
  }

  private String makeKey(String owner, String job) {
    return owner + "/" + job;
  }

  private String makeKey(JobConfiguration job) {
    return makeKey(job.getOwner(), job.getName());
  }
}

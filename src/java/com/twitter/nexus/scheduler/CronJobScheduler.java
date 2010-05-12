package com.twitter.nexus.scheduler;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.twitter.common.Pair;
import com.twitter.common.base.Closure;
import com.twitter.nexus.gen.JobConfiguration;
import it.sauronsoftware.cron4j.InvalidPatternException;
import it.sauronsoftware.cron4j.Scheduler;
import org.apache.commons.lang.StringUtils;

import java.util.Date;
import java.util.Map;
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
public class CronJobScheduler extends JobScheduler {
  private static Logger LOG = Logger.getLogger(CronJobScheduler.class.getName());

  // Cron manager.
  private final Scheduler scheduler = new Scheduler();

  // Maps from the our uniqe job identifier (<owner>/<jobName>) to the unique identifier used
  // internally by the cron4j scheduler.
  private final Map<String, Pair<String, JobConfiguration>> scheduledJobs = Maps.newHashMap();

  public JobScheduler setJobRunner(Closure<JobConfiguration> jobRunner) {
    scheduler.start();
    return super.setJobRunner(jobRunner);
  }

  @Override
  public boolean receiveJob(final JobConfiguration job) throws ScheduleException {
    if (StringUtils.isEmpty(job.getCronSchedule())) return false;

    LOG.info("Scheduling cron job " + job.getName());
    try {
      scheduledJobs.put(makeKey(job),
          Pair.of(scheduler.schedule(job.getCronSchedule(),
            new Runnable() {
              @Override public void run() {
                // TODO(wfarner): May want to record information about job runs.
                LOG.info("Job executing @ " + new Date());
                jobRunner.execute(job);
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
  public boolean hasJob(String owner, String job) {
    return scheduledJobs.containsKey(makeKey(owner, job));
  }

  @Override
  public void deleteJob(String owner, String job) {
    if (!hasJob(owner, job)) return;

    Pair<String, JobConfiguration> jobObj = scheduledJobs.remove(makeKey(owner, job));

    if (jobObj!= null) {
      scheduler.deschedule(jobObj.getFirst());
      LOG.info("Successfully deleted cron job for " + makeKey(owner, job));
    }
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

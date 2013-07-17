package com.twitter.aurora.scheduler.state;

import java.util.Collections;

import com.google.inject.Inject;

import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.JobKey;
import com.twitter.aurora.scheduler.base.ScheduleException;

/**
 * Interface for a job manager.  A job manager is responsible for deciding whether and when to
 * trigger execution of a job.
 */
public abstract class JobManager {

  @Inject
  protected SchedulerCore schedulerCore;

  /**
   * Gets a key that uniquely identifies this manager type, to distinguish from other schedulers.
   * These keys end up being persisted, so they must be considered permanently immutable.
   *
   * @return Job manager key.
   */
  public abstract String getUniqueKey();

  /**
   * Submits a job to the manager.  The job may be submitted to the job runner before this method
   * returns or at any point in the future.  This method will return false if the manager will not
   * execute the job.
   *
   * @param job The job to schedule.
   * @return {@code true} If the manager accepted the job, {@code false} otherwise.
   * @throws ScheduleException If there is a problem with scheduling the job.
   */
  public abstract boolean receiveJob(JobConfiguration job) throws ScheduleException;

  /**
   * Fetches the configured jobs that this manager is storing.
   *
   * @return Jobs stored by this job manager.
   */
  // TODO(ksweeney): Consider adding a Map<JobKey, JobConfiguration> to complement this.
  public Iterable<JobConfiguration> getJobs() {
    return Collections.emptyList();
  }

  /**
   * Checks whether this manager is storing a job with the given key.
   *
   * @param jobKey Job key.
   * @return {@code true} if the manager has a matching job, {@code false} otherwise.
   */
  public abstract boolean hasJob(JobKey jobKey);

  /**
   * Instructs the manager to delete any jobs with the given key.
   *
   * @param jobKey Job key.
   * @return {@code true} if a matching job was deleted.
   */
  public boolean deleteJob(JobKey jobKey) {
    // Optionally overridden by implementing class.
    return false;
  }
}

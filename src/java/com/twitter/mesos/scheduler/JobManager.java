package com.twitter.mesos.scheduler;

import java.util.Collections;

import com.google.inject.Inject;

import com.twitter.mesos.gen.JobConfiguration;

/**
 * Interface for a job manager.  A job manager is responsible for deciding whether and when to
 * trigger execution of a job.  A job manager will be {@link #start() started} before any other
 * methods are called.
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
   * Called to signal the job manager to prepare any existing jobs it manages and prepare for
   * further job lifecycle requests.  By default this does nothing and subclasses should override
   * if they have start logic to apply.
   */
  public void start() {
    // noop
  }

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
   * @param role Job owner.
   * @param job Job name.
   * @return {@code true} if the manager has a matching job, {@code false} otherwise.
   */
  // TODO(ksweeney): Refactor to take a JobKey
  public abstract boolean hasJob(String role, String job);

  /**
   * Instructs the manager to delete any jobs with the given key.
   *
   * @param role Job owner.
   * @param job Job name.
   * @return {@code true} if a matching job was deleted.
   */
  // TODO(ksweeney): Refactor to take a JobKey
  public boolean deleteJob(String role, String job) {
    // Optionally overridden by implementing class.
    return false;
  }
}

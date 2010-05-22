package com.twitter.nexus.scheduler;

import com.google.common.base.Preconditions;
import com.twitter.common.Pair;
import com.twitter.common.base.Closure;
import com.twitter.nexus.gen.JobConfiguration;

import java.util.Arrays;
import java.util.List;

/**
 * Interface for a job scheduler module.  A job scheduler is responsible for deciding whether to
 * trigger execution of a job.
 *
 * @author wfarner
 */
public abstract class JobScheduler {
  protected Closure<JobConfiguration> jobRunner;

  public JobScheduler setJobRunner(Closure<JobConfiguration> jobRunner) {
    this.jobRunner = Preconditions.checkNotNull(jobRunner);
    return this;
  }

  /**
   * Submits a job to the scheduler.  The job may be submitted to the job runner before this method
   * returns or at any point in the future.  This method will return false if the scheduler will not
   * execute the job.
   *
   * @param job The job to schedule.
   * @return {@code true} If the scheduler accepted the job, {@code false} otherwise.
   * @throws ScheduleException If there is a problem with scheduling the job.
   */
  public abstract boolean receiveJob(JobConfiguration job) throws ScheduleException;

  /**
   * Fetches the configured jobs that this scheduler is storing.
   *
   * @return Jobs stored by this job scheduler.
   */
  public Iterable<JobConfiguration> getState() {
    return Arrays.asList();
  }

  /**
   * Checks whether this scheduler is storing a job owned by {@code owner} with the name
   * {@code jobName}.
   *
   * @param owner The owner of the job.
   * @param jobName The name of the job.
   * @return {@code true} if the scheduler has a matching job, {@code false} otherwise.
   */
  public boolean hasJob(String owner, String jobName) {
    // Optionally overridden by implementing class.
    return false;
  }

  /**
   * Instructs the scheduler to delete any jobs owned by {@code owner} with the name
   * {@code jobName}.
   *
   * @param owner The owner of the job to delete.
   * @param jobName The name of the job to delete.
   */
  public boolean deleteJob(String owner, String jobName) {
    // Optionally overridden by implementing class.
    return false;
  }
}

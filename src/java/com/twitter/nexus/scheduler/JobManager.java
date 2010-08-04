package com.twitter.nexus.scheduler;

import com.google.inject.Inject;
import com.twitter.nexus.gen.JobConfiguration;

import java.util.Arrays;

/**
 * Interface for a job manager.  A job manager is responsible for deciding whether and when to
 * trigger execution of a job.
 *
 * @author wfarner
 */
public abstract class JobManager {

  @Inject
  protected SchedulerCore schedulerCore;

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
  public Iterable<JobConfiguration> getState() {
    return Arrays.asList();
  }

  /**
   * Checks whether this manager is storing a job owned by {@code owner} with the name
   * {@code jobName}.
   *
   * @param owner The owner of the job.
   * @param jobName The name of the job.
   * @return {@code true} if the manager has a matching job, {@code false} otherwise.
   */
  public boolean hasJob(String owner, String jobName) {
    // Optionally overridden by implementing class.
    return false;
  }

  /**
   * Instructs the manager to delete any jobs owned by {@code owner} with the name
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

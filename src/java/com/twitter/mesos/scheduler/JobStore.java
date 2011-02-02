package com.twitter.mesos.scheduler;

import com.twitter.mesos.gen.JobConfiguration;

/**
 * Stores job configuration data.
 *
 * @author jsirois
 */
public interface JobStore {

  /**
   * Fetches all {@code JobConfiguration}s for jobs owned th the {@link JobManager} identified by
   * {@code managerId}; if there are none then an empty set is returned.
   *
   * @param managerId The unique identifier of the {@link JobManager} to find registered jobs for.
   * @return the set of job configurations owned by the specififed job manager
   */
  Iterable<JobConfiguration> fetchJobs(String managerId);

  /**
   * Saves the job configuration for a job that has been accepted by the scheduler.
   *
   * @param managerId The unique id of the {@link JobManager} that accepted the job.
   * @param jobConfig The configuration of the accepted job.
   */
  void saveAcceptedJob(String managerId, JobConfiguration jobConfig);

  /**
   * Deletes the job configuration for the job identified by {@code jobKey}.  If there is no stored
   * configuration for the identified job, this method returns silently.
   *
   * @param jobKey the key identifying the job to delete.
   */
  void deleteJob(String jobKey);
}

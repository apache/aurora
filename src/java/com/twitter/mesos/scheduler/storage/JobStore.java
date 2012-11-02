package com.twitter.mesos.scheduler.storage;

import java.util.Set;

import javax.annotation.Nullable;

import com.twitter.mesos.gen.JobConfiguration;

/**
 * Stores job configuration data.
 */
public interface JobStore {

  /**
   * Fetches all {@code JobConfiguration}s for jobs owned by the
   * {@link com.twitter.mesos.scheduler.JobManager} identified by {@code managerId}; if there are
   * none then an empty set is returned.
   *
   * @param managerId The unique identifier of the {@link com.twitter.mesos.scheduler.JobManager} to
   *     find registered jobs for.
   * @return the set of job configurations owned by the specififed job manager
   */
  Iterable<JobConfiguration> fetchJobs(String managerId);

  /**
   * Fetches the {@code JobConfiguration} for the specified {@code jobKey}; if there is none then
   * {@code null} is returned.
   *
   * @param managerId The unique identifier of the {@link com.twitter.mesos.scheduler.JobManager}
   *     that accepted the job
   * @param jobKey The jobKey identifying the job to be fetched.
   * @return the job configuration for the given {@code jobKey} or else {@code null} if none is
   *     found
   */
  @Nullable JobConfiguration fetchJob(String managerId, String jobKey);

  /**
   * Fetches all the unique manager ids that are present in the job store.
   *
   * @return The IDs of all stored job managers.
   */
  Set<String> fetchManagerIds();

  public interface Mutable extends JobStore {
    /**
     * Saves the job configuration for a job that has been accepted by the scheduler. Acts as an
     * update if the managerId already exists.
     *
     * @param managerId The unique id of the {@link com.twitter.mesos.scheduler.JobManager} that
     *     accepted the job.
     * @param jobConfig The configuration of the accepted job.
     */
    void saveAcceptedJob(String managerId, JobConfiguration jobConfig);

    /**
     * Removes the job configuration for the job identified by {@code jobKey}.
     * If there is no stored configuration for the identified job, this method returns silently.
     *
     * @param jobKey the key identifying the job to delete.
     */
    void removeJob(String jobKey);

    /**
     * Deletes all jobs.
     */
    void deleteJobs();

    public interface Transactioned extends Mutable, Transactional {
    }
  }
}

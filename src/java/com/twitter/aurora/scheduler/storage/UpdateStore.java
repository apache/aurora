package com.twitter.aurora.scheduler.storage;

import java.util.Set;

import com.google.common.base.Optional;

import com.twitter.aurora.gen.JobKey;
import com.twitter.aurora.gen.JobUpdateConfiguration;

/**
 * Stores all update configurations for on-going updates.
 * Includes the old configuration and the updated configuration for the tasks in a job.
 */
public interface UpdateStore {

  /**
   * Fetches the update configuration (if present) for the given job key.
   *
   * @param jobKey Job to fetch update configuration for.
   * @return Optional job update configuration.
   */
  Optional<JobUpdateConfiguration> fetchJobUpdateConfig(JobKey jobKey);

  /**
   * Fetches all active shard update configurations for a role.
   *
   * @param role Role to fetch update configs for.
   * @return A multimap from job name to shard configurations.
   */
  Set<JobUpdateConfiguration> fetchUpdateConfigs(String role);

  /**
   * Fetches all roles with update records.
   *
   * @return Updating roles.
   */
  Set<String> fetchUpdatingRoles();

  public interface Mutable extends UpdateStore {

    /**
     * Saves a job update configuration.
     *
     * @param updateConfiguration Configuration to store.
     */
    void saveJobUpdateConfig(JobUpdateConfiguration updateConfiguration);

    /**
     * Removes the update configuration for the job.
     *
     * @param jobKey Key of the job.
     */
    void removeShardUpdateConfigs(JobKey jobKey);

    /**
     * Deletes all update configurations.
     */
    void deleteShardUpdateConfigs();
  }
}

package com.twitter.mesos.scheduler.storage;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Optional;
import com.google.common.collect.Multimap;

import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.storage.JobUpdateConfiguration;
import com.twitter.mesos.gen.storage.TaskUpdateConfiguration;

/**
 * Stores all update configurations for on-going updates.
 * Includes the old configuration and the updated configuration for the tasks in a job.
 *
 * @author William Farner, Sathya Hariesh.
 */
public interface UpdateStore {
  void saveJobUpdateConfig(JobUpdateConfiguration updateConfiguration);

  Optional<JobUpdateConfiguration> fetchJobUpdateConfig(String role, String job);

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

  /**
   * Removes the update configuration for the job.
   *
   * @param role Owner role of the job.
   * @param job Name of the job.
   */
  void removeShardUpdateConfigs(String role, String job);

  /**
   * Deletes all update configurations.
   */
  void deleteShardUpdateConfigs();
}

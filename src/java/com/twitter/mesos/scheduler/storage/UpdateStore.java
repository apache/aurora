package com.twitter.mesos.scheduler.storage;

import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.Multimap;

import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.storage.TaskUpdateConfiguration;

/**
 * Stores all update configurations for on-going updates.
 * Includes the old configuration and the updated configuration for the tasks in a job.
 *
 * @author William Farner, Sathya Hariesh.
 */
public interface UpdateStore {
  public static class ShardUpdateConfiguration {
    private final String updateToken;
    private final TaskUpdateConfiguration taskConfig;

    public ShardUpdateConfiguration(String updateToken, TaskUpdateConfiguration taskConfig) {
      this.updateToken = updateToken;
      this.taskConfig = taskConfig;
    }

    public String getUpdateToken() {
      return updateToken;
    }

    public TwitterTaskInfo getOldConfig() {
      return taskConfig.getOldConfig();
    }

    public TwitterTaskInfo getNewConfig() {
      return taskConfig.getNewConfig();
    }
  }

  /**
   * Saves the Task Update Configuration for the corresponding jobKey and updateToken.
   *
   * @param role Owner role of the job.
   * @param job Name of the job.
   * @param updateToken Token associated with the update.  If non-null, the token must match the
   *     the stored token for the update.
   * @param updateConfiguration A set of TaskUpdateConfiguration for the tasks in a job.
   */
  void saveShardUpdateConfigs(String role, String job, String updateToken,
      Set<TaskUpdateConfiguration> updateConfiguration);

  /**
   * Fetches the ShardUpdateConfiguration for the specified jobKey and shardId.
   *
   * @param role Owner role of the job.
   * @param job Name of the job.
   * @param shardId Shard to fetch.
   * @return update configuration or {@code null} if a new configuration does not exist.
   */
  @Nullable ShardUpdateConfiguration fetchShardUpdateConfig(String role, String job, int shardId);

  /**
   * Multi-get version of {@link #fetchShardUpdateConfig(String, String, int)}.
   *
   * @param role Owner role of the job.
   * @param job Name of the job.
   * @param shardIds Shards to fetch.
   * @return update configurations.  Configurations not found will not be represented in the set.
   *     An empty set will be returned if no matches are found.
   */
  Set<ShardUpdateConfiguration> fetchShardUpdateConfigs(String role, String job,
      Set<Integer> shardIds);

  /**
   * Fetches all of the shard update configurations for a job.
   *

   * @return Shards to be killed when update completes.
   */
  Set<ShardUpdateConfiguration> fetchShardUpdateConfigs(String role, String job);

  /**
   * Fetches all active shard update configurations for a role.
   *
   * @param role Role to fetch update configs for.
   * @return A multimap from job name to shard configurations.
   */
  Multimap<String, ShardUpdateConfiguration> fetchShardUpdateConfigs(String role);

  /**
   * Removes the update configuration for the job.
   *
   * @param role Owner role of the job.
   * @param job Name of the job.
   */
  void removeShardUpdateConfigs(String role, String job);
}

package com.twitter.mesos.scheduler.storage;

import com.twitter.mesos.gen.TaskUpdateConfiguration;
import com.twitter.mesos.gen.TwitterTaskInfo;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

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

    public TaskUpdateConfiguration getTaskConfig() {
      return taskConfig;
    }

    public TwitterTaskInfo getOldConfig() {
      return taskConfig.getOldConfig();
    }

    public TwitterTaskInfo getNewConfig() {
      return taskConfig.getNewConfig();
    }
  }

  /**
   * Adds the Task Update Configuration for the corresponding jobKey and updateToken.
   *
   * @param jobKey Key of the job update to update.
   * @param updateToken Token associated with the update.  If non-null, the token must match the
   *     the stored token for the update.
   * @param updateConfiguration A set of TaskUpdateConfiguration for the tasks in a job.
   */
  void add(String jobKey, String updateToken, Set<TaskUpdateConfiguration> updateConfiguration);

  /**
   * Fetches the ShardUpdateConfiguration for the specified jobKey and shardId.
   *
   * @param jobKey Key of the job update to update.
   * @param shardId Shard to fetch.
   * @return update configuration or {@code null} if a new configuration does not exist.
   */
  @Nullable ShardUpdateConfiguration fetchShardUpdateConfig(String jobKey, int shardId);

  /**
   * Multi-get version of {@link #fetchShardUpdateConfig(String, int)}.
   *
   * @param jobKey Key of the job update to update.
   * @param shardIds Shards to fetch.
   * @return update configurations.  Configurations not found will not be represented in the set.
   *     An empty set will be returned if no matches are found.
   */
  Set<ShardUpdateConfiguration> fetchShardUpdateConfigs(String jobKey, Set<Integer> shardIds);

  /**
   * Fetches the shards that need to be killed for an update with reduced shards.
   *
   * @param jobKey Key of the job update to update.
   * @return Shards to be killed when update completes.
   */
  Set<ShardUpdateConfiguration> fetchShardUpdateConfigs(String jobKey);

  /**
   * Removes the update configuration for the job.
   *
   * @param jobKey Key of the job update to update.
   */
  void remove(String jobKey);
}

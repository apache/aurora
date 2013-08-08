package com.twitter.aurora.scheduler.base;

import javax.annotation.Nullable;

import com.google.common.base.Function;

import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.gen.TaskUpdateConfiguration;

/**
 * Utility class for dealing with individual shards of a job.
 */
public final class Shards {

  /**
   * Gets the original task configuration for a shard update.  Result may be null if the update
   * adds the shard.
   */
  public static final Function<TaskUpdateConfiguration, TaskConfig> GET_ORIGINAL_CONFIG =
      new Function<TaskUpdateConfiguration, TaskConfig>() {
        @Nullable
        @Override public TaskConfig apply(TaskUpdateConfiguration updateConfig) {
          return updateConfig.getOldConfig();
        }
      };

  /**
   * Gets the updated task configuration for a shard update.  Result may be null if the update
   * removes the shard.
   */
  public static final Function<TaskUpdateConfiguration, TaskConfig> GET_NEW_CONFIG =
      new Function<TaskUpdateConfiguration, TaskConfig>() {
        @Nullable
        @Override public TaskConfig apply(TaskUpdateConfiguration updateConfig) {
          return updateConfig.getNewConfig();
        }
      };

  private Shards() {
    // Utility.
  }
}

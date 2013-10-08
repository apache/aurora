/*
 * Copyright 2013 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.aurora.scheduler.base;

import javax.annotation.Nullable;

import com.google.common.base.Function;

import com.twitter.aurora.gen.TaskUpdateConfiguration;
import com.twitter.aurora.scheduler.storage.entities.ITaskConfig;

/**
 * Utility class for dealing with individual shards of a job.
 */
public final class Shards {

  /**
   * Gets the original task configuration for a shard update.  Result may be null if the update
   * adds the shard.
   */
  public static final Function<TaskUpdateConfiguration, ITaskConfig> GET_ORIGINAL_CONFIG =
      new Function<TaskUpdateConfiguration, ITaskConfig>() {
        @Nullable
        @Override public ITaskConfig apply(TaskUpdateConfiguration updateConfig) {
          return updateConfig.isSetOldConfig()
              ? ITaskConfig.build(updateConfig.getOldConfig()) : null;
        }
      };

  /**
   * Gets the updated task configuration for a shard update.  Result may be null if the update
   * removes the shard.
   */
  public static final Function<TaskUpdateConfiguration, ITaskConfig> GET_NEW_CONFIG =
      new Function<TaskUpdateConfiguration, ITaskConfig>() {
        @Nullable
        @Override public ITaskConfig apply(TaskUpdateConfiguration updateConfig) {
          return updateConfig.isSetNewConfig()
              ? ITaskConfig.build(updateConfig.getNewConfig()) : null;
        }
      };

  private Shards() {
    // Utility.
  }
}

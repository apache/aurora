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

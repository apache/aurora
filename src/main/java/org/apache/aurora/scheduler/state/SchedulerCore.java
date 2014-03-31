/**
 * Copyright 2013 Apache Software Foundation
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
package org.apache.aurora.scheduler.state;

import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.ScheduleException;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import org.apache.aurora.scheduler.configuration.SanitizedConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

/**
 * Scheduling core, stores scheduler state and makes decisions about which tasks to schedule when
 * a resource offer is made.
 *
 * When a job is submitted to the scheduler core, it will store the job configuration and offer
 * the job to all configured scheduler modules, which are responsible for triggering execution of
 * the job.  Until a job is triggered by a scheduler module, it is retained in the scheduler core
 * in the PENDING state.
 */
public interface SchedulerCore {

  /**
   * Creates a new job, whose tasks will become candidates for scheduling.
   *
   * @param sanitizedConfiguration The configuration of the job to create tasks for.
   * @throws ScheduleException If there was an error scheduling a cron job.
   * @throws TaskDescriptionException If an invalid task description was given.
   */
  void createJob(SanitizedConfiguration sanitizedConfiguration)
      throws ScheduleException, TaskDescriptionException;

  /**
   * Adds new instances specified by the instances set.
   * <p>
   * Provided instance IDs should be disjoint from the instance IDs active in the job.
   *
   * @param jobKey IJobKey identifying the parent job.
   * @param instanceIds Set of instance IDs to be added to the job.
   * @param config ITaskConfig to use with new instances.
   * @throws ScheduleException If any of the existing instance IDs already exist.
   */
  void addInstances(IJobKey jobKey, ImmutableSet<Integer> instanceIds, ITaskConfig config)
      throws ScheduleException;

  /**
   * Assigns a new state to tasks.
   *
   * @param taskId ID of the task to transition.
   * @param status The new state of the tasks.
   * @param message Additional information about the state transition.
   */
  void setTaskStatus(String taskId, ScheduleStatus status, Optional<String> message);

  /**
   * Kills a specific set of tasks.
   *
   * @param query Builder for a query to identify tasks
   * @param user Name of the user performing the kill.
   * @throws ScheduleException If a problem occurs with the kill request.
   */
  void killTasks(Query.Builder query, String user) throws ScheduleException;

  /**
   * Initiates a restart of shards within an active job.
   *
   * @param jobKey Key of job to be restarted.
   * @param shards Shards to be restarted.
   * @param requestingUser User performing the restart action.
   * @throws ScheduleException If there are no matching active shards.
   */
  void restartShards(IJobKey jobKey, Set<Integer> shards, String requestingUser)
      throws ScheduleException;

  /**
   * Indicates to the scheduler that tasks were deleted on the assigned host.
   *
   * @param taskIds IDs of tasks that were deleted.
   */
  void tasksDeleted(Set<String> taskIds);
}

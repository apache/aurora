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
package com.twitter.aurora.scheduler.state;

import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.apache.mesos.Protos.SlaveID;

import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.storage.entities.IAssignedTask;
import com.twitter.aurora.scheduler.storage.entities.IJobKey;
import com.twitter.aurora.scheduler.storage.entities.ITaskConfig;

/**
 * Thin interface for the state manager.
 */
public interface StateManager {

  /**
   * Performs a simple state change, transitioning all tasks matching a query to the given
   * state and applying the given audit message.
   * TODO(William Farner): Consider removing the return value.
   *
   * @param query Builder of the query to perform, the results of which will be modified.
   * @param newState State to move the resulting tasks into.
   * @param auditMessage Audit message to apply along with the state change.
   * @return the number of successful state changes.
   */
  int changeState(Query.Builder query, ScheduleStatus newState, Optional<String> auditMessage);

  /**
   * Assigns a task to a specific slave.
   * This will modify the task record to reflect the host assignment and return the updated record.
   *
   * @param taskId ID of the task to mutate.
   * @param slaveHost Host name that the task is being assigned to.
   * @param slaveId ID of the slave that the task is being assigned to.
   * @param assignedPorts Ports on the host that are being assigned to the task.
   * @return The updated task record, or {@code null} if the task was not found.
   */
  IAssignedTask assignTask(
      String taskId,
      String slaveHost,
      SlaveID slaveId,
      Set<Integer> assignedPorts);

  /**
   * Inserts new tasks into the store. Tasks will immediately move into PENDING and will be eligible
   * for scheduling.
   *
   * @param tasks Tasks to insert, mapped by their instance IDs.
   */
  void insertPendingTasks(Map<Integer, ITaskConfig> tasks);

  /**
   * Deletes records of tasks from the task store.
   * This will not perform any state checking or state transitions, but will immediately remove
   * the tasks from the store.  It will also silently ignore attempts to delete task IDs that do
   * not exist.
   *
   * @param taskIds IDs of tasks to delete.
   */
  void deleteTasks(final Set<String> taskIds);

  /**
   * Adds new instances specified by the instances set.
   * Requires unique (per job) instance IDs. Will fail with {@link InstanceException} if
   * unable to add job instances due to colliding instance IDs.
   *
   * @param jobKey {@link IJobKey} identifying the parent job.
   * @param instancesIds Set of instance IDs to be added to the job.
   * @param config {@link ITaskConfig} to use with new instances.
   * @throws InstanceException If any of the existing instance IDs already exist.
   */
  void addInstances(IJobKey jobKey, ImmutableSet<Integer> instancesIds, ITaskConfig config)
      throws InstanceException;

  /**
   * Thrown when instance related operation fails.
   */
  static class InstanceException extends Exception {
    public InstanceException(String msg) {
      super(msg);
    }
  }
}

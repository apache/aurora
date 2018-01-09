/**
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
package org.apache.aurora.scheduler.storage;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import com.google.common.base.CharMatcher;
import com.google.common.base.Function;
import com.google.common.base.Predicate;

import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.entities.ITaskQuery;

/**
 * Stores all tasks configured with the scheduler.
 */
public interface TaskStore {

  /**
   * Fetches a task.
   *
   * @param taskId ID of the task to fetch.
   * @return The task, if it exists.
   */
  Optional<IScheduledTask> fetchTask(String taskId);

  /**
   * Fetches a read-only view of tasks matching a query and filters. Intended for use with a
   * {@link Query.Builder}.
   *
   * @param query Builder of the query to identify tasks with.
   * @return A read-only view of matching tasks.
   */
  Collection<IScheduledTask> fetchTasks(Query.Builder query);

  /**
   * Fetches all job keys represented in the task store.
   *
   * @return Job keys of stored tasks.
   */
  Set<IJobKey> getJobKeys();

  interface Mutable extends TaskStore {

    /**
     * A convenience interface to allow callers to more concisely implement a task mutation.
     */
    interface TaskMutation extends Function<IScheduledTask, IScheduledTask> {
    }

    /**
     * Saves tasks to the store.  Tasks are copied internally, meaning that the tasks are stored in
     * the state they were in when the method is called, and further object modifications will not
     * affect the tasks.  If any of the tasks already exist in the store, they will be overwritten
     * by the supplied {@code newTasks}.
     *
     * @param tasks Tasks to add.
     */
    void saveTasks(Set<IScheduledTask> tasks);

    /**
     * Removes all tasks from the store.
     * TODO(wfarner): Move this and other mass-delete methods to an interface that is only
     * accessible by SnapshotStoreImpl.
     */
    void deleteAllTasks();

    /**
     * Deletes specific tasks from the store.
     *
     * @param taskIds IDs of tasks to delete.
     */
    void deleteTasks(Set<String> taskIds);

    /**
     * Mutates a single task, if present.
     *
     * @param taskId Unique ID of the task to mutate.
     * @param mutator The mutate operation.
     * @return The result of the mutate operation, if performed.
     */
    Optional<IScheduledTask> mutateTask(
        String taskId,
        Function<IScheduledTask, IScheduledTask> mutator);
  }

  final class Util {
    private Util() {
      // Utility class.
    }

    public static Predicate<IScheduledTask> queryFilter(final Query.Builder queryBuilder) {
      return task -> {
        ITaskQuery query = queryBuilder.get();
        ITaskConfig config = task.getAssignedTask().getTask();
        // TODO(wfarner): Investigate why blank inputs are treated specially for the role field.
        if (query.getRole() != null
            && !CharMatcher.whitespace().matchesAllOf(query.getRole())
            && !query.getRole().equals(config.getJob().getRole())) {
          return false;
        }
        if (query.getEnvironment() != null
            && !query.getEnvironment().equals(config.getJob().getEnvironment())) {
          return false;
        }
        if (query.getJobName() != null && !query.getJobName().equals(config.getJob().getName())) {
          return false;
        }

        if (!query.getJobKeys().isEmpty() && !query.getJobKeys().contains(config.getJob())) {
          return false;
        }
        if (!query.getTaskIds().isEmpty() && !query.getTaskIds().contains(Tasks.id(task))) {
          return false;
        }

        if (!query.getStatuses().isEmpty() && !query.getStatuses().contains(task.getStatus())) {
          return false;
        }
        if (!query.getSlaveHosts().isEmpty()
            && !query.getSlaveHosts().contains(task.getAssignedTask().getSlaveHost())) {
          return false;
        }
        if (!query.getInstanceIds().isEmpty()
            && !query.getInstanceIds().contains(task.getAssignedTask().getInstanceId())) {
          return false;
        }

        return true;
      };
    }
  }
}

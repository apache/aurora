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
package org.apache.aurora.scheduler.storage;

import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

/**
 * Stores all tasks configured with the scheduler.
 */
public interface TaskStore {

  /**
   * Fetches a read-only view of tasks matching a query and filters. Intended for use with a
   * {@link org.apache.aurora.scheduler.base.Query.Builder}.
   *
   * @param query Builder of the query to identify tasks with.
   * @return A read-only view of matching tasks.
   */
  ImmutableSet<IScheduledTask> fetchTasks(Query.Builder query);

  public interface Mutable extends TaskStore {

    /**
     * A convenience interface to allow callers to more concisely implement a task mutation.
     */
    public interface TaskMutation extends Function<IScheduledTask, IScheduledTask> {
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
     */
    void deleteAllTasks();

    /**
     * Deletes specific tasks from the store.
     *
     * @param taskIds IDs of tasks to delete.
     */
    void deleteTasks(Set<String> taskIds);

    /**
     * Offers temporary mutable access to tasks.  If a task ID is not found, it will be silently
     * skipped, and no corresponding task will be returned.
     *
     * @param query Query to match tasks against.
     * @param mutator The mutate operation.
     * @return Immutable copies of only the tasks that were mutated.
     */
    ImmutableSet<IScheduledTask> mutateTasks(
        Query.Builder query,
        Function<IScheduledTask, IScheduledTask> mutator);

    /**
     * Rewrites a task's configuration in-place.
     * <p>
     * <b>WARNING</b>: this is a dangerous operation, and should not be used without exercising
     * great care.  This feature should be used as a last-ditch effort to rewrite things that
     * the scheduler otherwise can't (e.g. {@link ITaskConfig#executorConfig}) rewrite in a
     * controlled/tested backfill operation.
     *
     * @param taskId ID of the task to alter.
     * @param taskConfiguration Configuration object to swap with the existing task's
     *                          configuration.
     * @return {@code true} if the modification took effect, or {@code false} if the task does not
     *         exist in the store.
     */
    boolean unsafeModifyInPlace(String taskId, ITaskConfig taskConfiguration);
  }
}

/**
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
package org.apache.aurora.scheduler.events;

import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.base.Optional;

import org.apache.aurora.gen.HostStatus;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Event notifications related to tasks.
 */
public interface PubsubEvent {

  /**
   * Interface with no functionality, but identifies a class as supporting task pubsub events.
   */
  public interface EventSubscriber {
  }

  /**
   * Event sent when tasks were deleted.
   */
  public static class TasksDeleted implements PubsubEvent {
    private final Set<IScheduledTask> tasks;

    public TasksDeleted(Set<IScheduledTask> tasks) {
      this.tasks = checkNotNull(tasks);
    }

    public Set<IScheduledTask> getTasks() {
      return tasks;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof TasksDeleted)) {
        return false;
      }

      TasksDeleted other = (TasksDeleted) o;
      return Objects.equal(tasks, other.tasks);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(tasks);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("tasks", Tasks.ids(tasks))
          .toString();
    }
  }

  /**
   * Event sent when a task changed state.
   */
  public static final class TaskStateChange implements PubsubEvent {
    private final IScheduledTask task;
    private final Optional<ScheduleStatus> oldState;

    private TaskStateChange(IScheduledTask task, Optional<ScheduleStatus> oldState) {
      this.task = checkNotNull(task);
      this.oldState = checkNotNull(oldState);
    }

    /**
     * Creates a state change event that represents the initial value of a task.
     *
     * @param task Task structure.
     * @return A state change event.
     */
    public static TaskStateChange initialized(IScheduledTask task) {
      return new TaskStateChange(task, Optional.<ScheduleStatus>absent());
    }

    /**
     * Creates a state change event that represents a transition from one state to another.
     *
     * @param task Current task structure.
     * @param oldState State the task was previously in.
     * @return A state change event.
     */
    public static TaskStateChange transition(IScheduledTask task, ScheduleStatus oldState) {
      return new TaskStateChange(task, Optional.of(oldState));
    }

    public String getTaskId() {
      return Tasks.id(task);
    }

    public Optional<ScheduleStatus> getOldState() {
      return oldState;
    }

    public boolean isTransition() {
      return oldState.isPresent();
    }

    public IScheduledTask getTask() {
      return task;
    }

    public ScheduleStatus getNewState() {
      return task.getStatus();
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof TaskStateChange)) {
        return false;
      }

      TaskStateChange other = (TaskStateChange) o;
      return Objects.equal(task, other.task)
          && Objects.equal(oldState, other.oldState);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(task, oldState);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("task", Tasks.id(task))
          .add("oldState", getOldState())
          .add("newState", getNewState())
          .toString();
    }
  }

  /**
   * Event sent when a host changed maintenance state.
   */
  public static class HostMaintenanceStateChange implements PubsubEvent {
    private final HostStatus status;

    public HostMaintenanceStateChange(HostStatus status) {
      this.status = checkNotNull(status);
    }

    public HostStatus getStatus() {
      return status;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof HostMaintenanceStateChange)) {
        return false;
      }

      HostMaintenanceStateChange other = (HostMaintenanceStateChange) o;
      return Objects.equal(status, other.status);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(status);
    }
  }

  /**
   * Event sent when a scheduling assignment was vetoed.
   */
  public static class Vetoed implements PubsubEvent {
    private final String taskId;
    private final Set<Veto> vetoes;

    public Vetoed(String taskId, Set<Veto> vetoes) {
      this.taskId = checkNotNull(taskId);
      this.vetoes = checkNotNull(vetoes);
    }

    public String getTaskId() {
      return taskId;
    }

    public Set<Veto> getVetoes() {
      return vetoes;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Vetoed)) {
        return false;
      }

      Vetoed other = (Vetoed) o;
      return Objects.equal(taskId, other.taskId)
          && Objects.equal(vetoes, other.vetoes);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(taskId, vetoes);
    }
  }

  public static class DriverRegistered implements PubsubEvent {
    @Override
    public boolean equals(Object o) {
      return o != null && getClass().equals(o.getClass());
    }

    @Override
    public int hashCode() {
      return getClass().hashCode();
    }
  }

  public static class DriverDisconnected implements PubsubEvent {
    @Override
    public boolean equals(Object o) {
      return o != null && getClass().equals(o.getClass());
    }

    @Override
    public int hashCode() {
      return getClass().hashCode();
    }
  }

  public static class SchedulerActive implements PubsubEvent {
    @Override
    public boolean equals(Object o) {
      return o != null && getClass().equals(o.getClass());
    }

    @Override
    public int hashCode() {
      return getClass().hashCode();
    }
  }
}

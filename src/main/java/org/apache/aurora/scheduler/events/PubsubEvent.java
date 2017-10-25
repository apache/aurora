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
package org.apache.aurora.scheduler.events;

import java.util.Objects;
import java.util.Set;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.gson.Gson;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.Protos.TaskStatus;

import static java.util.Objects.requireNonNull;

/**
 * Event notifications related to tasks.
 */
public interface PubsubEvent {

  /**
   * Interface with no functionality, but identifies a class as supporting task pubsub events.
   */
  interface EventSubscriber {
  }

  /**
   * Event sent when tasks were deleted.
   */
  class TasksDeleted implements PubsubEvent {
    private final Set<IScheduledTask> tasks;

    public TasksDeleted(Set<IScheduledTask> tasks) {
      this.tasks = requireNonNull(tasks);
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
      return Objects.equals(tasks, other.tasks);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tasks);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("tasks", Tasks.ids(tasks))
          .toString();
    }
  }

  /**
   * Event sent when a task changed state.
   * <p>
   * This class is final as it should only be constructed through declared factory methods.
   */
  final class TaskStateChange implements PubsubEvent {
    private final IScheduledTask task;
    private final Optional<ScheduleStatus> oldState;

    private TaskStateChange(IScheduledTask task, Optional<ScheduleStatus> oldState) {
      this.task = requireNonNull(task);
      this.oldState = requireNonNull(oldState);
    }

    /**
     * Creates a state change event that represents the initial value of a task.
     *
     * @param task Task structure.
     * @return A state change event.
     */
    public static TaskStateChange initialized(IScheduledTask task) {
      return new TaskStateChange(task, Optional.absent());
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
      return Objects.equals(task, other.task)
          && Objects.equals(oldState, other.oldState);
    }

    @Override
    public int hashCode() {
      return Objects.hash(task, oldState);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("task", Tasks.id(task))
          .add("oldState", getOldState())
          .add("newState", getNewState())
          .toString();
    }

    public String toJson() {
      return new Gson().toJson(this);
    }

  }

  /**
   * Event sent when a host's attributes change.
   */
  class HostAttributesChanged implements PubsubEvent {
    private final IHostAttributes attributes;

    public HostAttributesChanged(IHostAttributes attributes) {
      this.attributes = requireNonNull(attributes);
    }

    public IHostAttributes getAttributes() {
      return attributes;
    }

    @Override
    public int hashCode() {
      return attributes.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof HostAttributesChanged)) {
        return false;
      }

      HostAttributesChanged other = (HostAttributesChanged) o;
      return Objects.equals(attributes, other.getAttributes());
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("attributes", getAttributes())
          .toString();
    }
  }

  class DriverRegistered implements PubsubEvent {
    @Override
    public boolean equals(Object o) {
      return o != null && getClass().equals(o.getClass());
    }

    @Override
    public int hashCode() {
      return getClass().hashCode();
    }
  }

  class DriverDisconnected implements PubsubEvent {
    @Override
    public boolean equals(Object o) {
      return o != null && getClass().equals(o.getClass());
    }

    @Override
    public int hashCode() {
      return getClass().hashCode();
    }
  }

  class TaskStatusReceived implements PubsubEvent {
    private final Protos.TaskState state;
    private final Optional<TaskStatus.Source> source;
    private final Optional<TaskStatus.Reason> reason;
    private final Optional<Long> epochTimestampMicros;

    public TaskStatusReceived(
        Protos.TaskState state,
        Optional<TaskStatus.Source> source,
        Optional<TaskStatus.Reason> reason,
        Optional<Long> epochTimestampMicros) {

      this.state = requireNonNull(state);
      this.source = requireNonNull(source);
      this.reason = requireNonNull(reason);
      this.epochTimestampMicros = requireNonNull(epochTimestampMicros);
    }

    public Protos.TaskState getState() {
      return state;
    }

    public Optional<TaskStatus.Source> getSource() {
      return source;
    }

    public Optional<TaskStatus.Reason> getReason() {
      return reason;
    }

    public Optional<Long> getEpochTimestampMicros() {
      return epochTimestampMicros;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof TaskStatusReceived)) {
        return false;
      }

      TaskStatusReceived other = (TaskStatusReceived) o;
      return Objects.equals(state, other.state)
          && Objects.equals(source, other.source)
          && Objects.equals(reason, other.reason)
          && Objects.equals(epochTimestampMicros, other.epochTimestampMicros);
    }

    @Override
    public int hashCode() {
      return Objects.hash(state, source, reason, epochTimestampMicros);
    }
  }
}

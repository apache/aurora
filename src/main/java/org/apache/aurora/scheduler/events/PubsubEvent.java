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
package org.apache.aurora.scheduler.events;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Set;

import com.google.common.base.Objects;

import org.apache.aurora.gen.HostStatus;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

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
  public static class TaskStateChange implements PubsubEvent {
    private final IScheduledTask task;
    private final ScheduleStatus oldState;

    public TaskStateChange(IScheduledTask task, ScheduleStatus oldState) {
      this.task = checkNotNull(task);
      this.oldState = checkNotNull(oldState);
    }

    public String getTaskId() {
      return Tasks.id(task);
    }

    public ScheduleStatus getOldState() {
      return oldState;
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

  public static class TaskRescheduled implements PubsubEvent {
    private final String role;
    private final String job;
    private final int instance;

    public TaskRescheduled(String role, String job, int instance) {
      this.role = role;
      this.job = job;
      this.instance = instance;
    }

    public String getRole() {
      return role;
    }

    public String getJob() {
      return job;
    }

    public int getInstance() {
      return instance;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof TaskRescheduled)) {
        return false;
      }

      TaskRescheduled other = (TaskRescheduled) o;
      return Objects.equal(role, other.role)
          && Objects.equal(job, other.job)
          && Objects.equal(instance, other.instance);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(role, job, instance);
    }
  }

  public static class StorageStarted implements PubsubEvent {
    @Override
    public boolean equals(Object o) {
      return (o != null) && getClass().equals(o.getClass());
    }

    @Override
    public int hashCode() {
      return getClass().hashCode();
    }
  }

  public static class DriverRegistered implements PubsubEvent {
    @Override
    public boolean equals(Object o) {
      return (o != null) && getClass().equals(o.getClass());
    }

    @Override
    public int hashCode() {
      return getClass().hashCode();
    }
  }

  public static class DriverDisconnected implements PubsubEvent {
    @Override
    public boolean equals(Object o) {
      return (o != null) && getClass().equals(o.getClass());
    }

    @Override
    public int hashCode() {
      return getClass().hashCode();
    }
  }

  public static final class Interceptors {
    private Interceptors() {
      // Utility class.
    }

    public enum Event {
      None(null),
      StorageStarted(new StorageStarted()),
      DriverRegistered(new DriverRegistered()),
      DriverDisconnected(new DriverDisconnected());

      private final PubsubEvent event;
      private Event(PubsubEvent event) {
        this.event = event;
      }

      public PubsubEvent getEvent() {
        return event;
      }
    }

    /**
     * An annotation to place on methods of injected classes that which to fire events before
     * and/or after their invocation.
     */
    @Target(METHOD) @Retention(RUNTIME)
    public @interface SendNotification {
      /**
       * Event to fire prior to invocation.
       */
      Event before() default Event.None;

      /**
       * Event to fire after invocation.
       */
      Event after() default Event.None;
    }
  }
}

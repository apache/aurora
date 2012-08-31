package com.twitter.mesos.scheduler.events;

import java.util.Set;

import com.google.common.base.Objects;

import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.scheduler.SchedulingFilter.Veto;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Event notifications related to tasks.
 */
public interface TaskPubsubEvent {

  /**
   * Interface with no functionality, but identifies a class as supporting task pubsub events.
   */
  public interface EventSubscriber {
  }

  /**
   * Event sent when tasks were deleted.
   */
  public static class Deleted implements TaskPubsubEvent {
    private final Set<String> taskIds;

    public Deleted(Set<String> taskIds) {
      this.taskIds = checkNotNull(taskIds);
    }

    public Set<String> getTaskIds() {
      return taskIds;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Deleted)) {
        return false;
      }

      Deleted other = (Deleted) o;
      return Objects.equal(taskIds, other.taskIds);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(taskIds);
    }
  }

  /**
   * Event sent when a task changed state.
   */
  public static class StateChange implements TaskPubsubEvent {
    private final String taskId;
    private final ScheduleStatus oldState;
    private final ScheduleStatus newState;

    public StateChange(String taskId, ScheduleStatus oldState, ScheduleStatus newState) {
      this.taskId = checkNotNull(taskId);
      this.oldState = checkNotNull(oldState);
      this.newState = checkNotNull(newState);
    }

    public String getTaskId() {
      return taskId;
    }

    public ScheduleStatus getOldState() {
      return oldState;
    }

    public ScheduleStatus getNewState() {
      return newState;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof StateChange)) {
        return false;
      }

      StateChange other = (StateChange) o;
      return Objects.equal(taskId, other.taskId)
          && Objects.equal(oldState, other.oldState)
          && Objects.equal(newState, other.newState);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(taskId, oldState, newState);
    }
  }

  /**
   * Event sent when a scheduling assignment was vetoed.
   */
  public static class Vetoed implements TaskPubsubEvent {
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

  public static class Rescheduled implements TaskPubsubEvent {
    private final String role;
    private final String job;
    private final int shard;

    public Rescheduled(String role, String job, int shard) {
      this.role = role;
      this.job = job;
      this.shard = shard;
    }

    public String getRole() {
      return role;
    }

    public String getJob() {
      return job;
    }

    public int getShard() {
      return shard;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Rescheduled)) {
        return false;
      }

      Rescheduled other = (Rescheduled) o;
      return Objects.equal(role, other.role)
          && Objects.equal(job, other.job)
          && Objects.equal(shard, other.shard);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(role, job, shard);
    }
  }
}

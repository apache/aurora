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
    final Set<String> taskIds;

    public Deleted(Set<String> taskIds) {
      this.taskIds = checkNotNull(taskIds);
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
    final String taskId;
    final ScheduleStatus oldState;
    final ScheduleStatus newState;

    public StateChange(String taskId, ScheduleStatus oldState, ScheduleStatus newState) {
      this.taskId = checkNotNull(taskId);
      this.oldState = checkNotNull(oldState);
      this.newState = checkNotNull(newState);
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
    final String taskId;
    final String slaveHost;
    final Set<Veto> vetoes;

    public Vetoed(String taskId, String slaveHost, Set<Veto> vetoes) {
      this.taskId = checkNotNull(taskId);
      this.slaveHost = checkNotNull(slaveHost);
      this.vetoes = checkNotNull(vetoes);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Vetoed)) {
        return false;
      }

      Vetoed other = (Vetoed) o;
      return Objects.equal(taskId, other.taskId)
          && Objects.equal(slaveHost, other.slaveHost)
          && Objects.equal(vetoes, other.vetoes);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(taskId, slaveHost, vetoes);
    }
  }
}

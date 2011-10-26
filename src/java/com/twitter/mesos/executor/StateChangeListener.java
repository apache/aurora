package com.twitter.mesos.executor;

import com.google.common.base.Optional;
import com.google.inject.Inject;

import com.twitter.mesos.executor.sync.SyncBuffer;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.comm.TaskStateUpdate;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Receives notifications about changes to task states, and when tasks are deleted.
 *
 * @author William Farner
 */
public interface StateChangeListener {

  /**
   * Notifies the listener that a task has changed to a different state.
   *
   * @param taskId Task id.
   * @param status Updated status.
   * @param message Optional message to associated with the transition.
   */
  void changedState(String taskId, ScheduleStatus status, Optional<String> message);

  /**
   * Notifies the listener that a task has been deleted.
   *
   * @param taskId Task id.
   */
  void deleted(String taskId);

  /**
   * State change listener that forwards updates to the scheduler, and populates the sync buffer.
   */
  static class StateChangeListenerImpl implements StateChangeListener {

    private final Driver driver;
    private final SyncBuffer syncBuffer;

    @Inject
    public StateChangeListenerImpl(Driver driver, SyncBuffer syncBuffer) {
      this.driver = checkNotNull(driver);
      this.syncBuffer = checkNotNull(syncBuffer);
    }

    @Override
    public void changedState(String taskId, ScheduleStatus status, Optional<String> message) {
      syncBuffer.add(taskId, new TaskStateUpdate().setStatus(status));
      driver.sendStatusUpdate(taskId, status, message);
    }

    @Override
    public void deleted(String taskId) {
      syncBuffer.add(taskId, new TaskStateUpdate().setDeleted(true));
    }
  }
}

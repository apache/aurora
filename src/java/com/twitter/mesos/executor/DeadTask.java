package com.twitter.mesos.executor;

import java.io.File;

import com.google.common.base.Preconditions;

import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;

/**
 * A task that has completed execution.
 */
public class DeadTask extends TaskOnDisk {

  private final AssignedTask task;
  private final AuditedStatus state;

  /**
   * Creates a new dead task.
   *
   * @param taskRoot Root directory for this task's recorded state.
   * @throws TaskStorageException If the task could not be restored from the task root.
   */
  DeadTask(File taskRoot) throws TaskStorageException {
    super(taskRoot);

    Preconditions.checkNotNull(taskRoot);
    Preconditions.checkArgument(taskRoot.exists(), "Sandbox does not exist: " + taskRoot);
    Preconditions.checkArgument(taskRoot.isDirectory(), "Not a directory: " + taskRoot);

    this.task = restoreTask();
    this.state = new AuditedStatus(restoreStatus());
    Preconditions.checkState(!Tasks.isActive(state.getStatus()),
        "A dead task may not be assigned an active state.");
  }

  @Override
  public String getId() {
    return task.getTaskId();
  }

  @Override
  public boolean isRunning() {
    return false;
  }

  @Override
  public boolean isCompleted() {
    return true;
  }

  @Override
  public Object getMutex() {
    throw new UnsupportedOperationException("A dead task should not be mutated.");
  }

  @Override
  public void stage() throws TaskRunException {
    throw new UnsupportedOperationException("A dead task cannot be staged.");
  }

  @Override
  public AuditedStatus blockUntilTerminated() {
    throw new UnsupportedOperationException("Should not attempt to block on a dead task.");
  }

  @Override
  public void terminate(AuditedStatus terminalState) {
    throw new UnsupportedOperationException("The state of a dead task cannot be changed.");
  }

  @Override
  public AssignedTask getAssignedTask() {
    return task;
  }

  @Override
  public AuditedStatus getAuditedStatus() {
    return state;
  }

  @Override
  public void run() {
    throw new UnsupportedOperationException("A dead task cannot be run.");
  }

  @Override
  public String toString() {
    return String.format("DeadTask(s: %s, t: %s)", state, task);
  }
}

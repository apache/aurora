package com.twitter.mesos.executor;

import java.io.File;

import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduleStatus;

/**
 * @author William Farner
 */
public interface Task {

  public String getId();

  public void stage() throws TaskRunException;

  public void run() throws TaskRunException;

  public ScheduleStatus blockUntilTerminated();

  public boolean isRunning();

  /**
   * Whether underlying process is terminated after completion.
   *
   * @return {@code true} iff the underlying process is completed
   */
  public boolean isCompleted();

  public void terminate(ScheduleStatus terminalState);

  public File getSandboxDir();

  public AssignedTask getAssignedTask();

  public ScheduleStatus getScheduleStatus();

  public static class TaskRunException extends Exception {

    // TODO(William Farner): Attach a GUID here (probably via UUID) to allow for automatic stack
    //    trace linking between disparate parts of the system.

    public TaskRunException(String msg, Throwable t) {
      super(msg, t);
    }

    public TaskRunException(String msg) {
      super(msg);
    }
  }
}

package com.twitter.mesos.executor;

import com.twitter.mesos.gen.ResourceConsumption;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.TwitterTaskInfo;

import java.io.File;

/**
 * @author wfarner
 */
public interface Task {

  public int getId();

  public void run() throws TaskRunException;

  public boolean isRunning();

  public void terminate(ScheduleStatus terminalState);

  public File getRootDir();

  public TwitterTaskInfo getTaskInfo();

  public ScheduleStatus getScheduleStatus();

  public ResourceConsumption getResourceConsumption();

  public static class TaskRunException extends Exception {
    public TaskRunException(String msg, Throwable t) {
      super(msg, t);
    }

    public TaskRunException(String msg) {
      super(msg);
    }
  }
}

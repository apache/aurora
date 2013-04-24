package com.twitter.mesos.scheduler;

/**
* Exception class to signal a failure to schedule a task or job.
*/
public class ScheduleException extends Exception {
  public ScheduleException(String msg) {
    super(msg);
  }

  public ScheduleException(String msg, Throwable t) {
    super(msg, t);
  }

  public ScheduleException(Throwable t) {
    super(t);
  }
}

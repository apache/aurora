package com.twitter.mesos.scheduler;

/**
* Exception class to signal a failure to schedule a task or job.
*
* @author wfarner
*/
public class ScheduleException extends Exception {
  public ScheduleException(String msg) {
    super(msg);
  }

  public ScheduleException(String msg, Throwable t) {
    super(msg, t);
  }
}

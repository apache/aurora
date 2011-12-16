package com.twitter.mesos.scheduler;

/**
 * Indicates some form of unexpected scheduler exception.
 *
 * @author John Sirois
 */
public class SchedulerException extends RuntimeException {
  public SchedulerException(String message) {
    super(message);
  }
  public SchedulerException(String message, Throwable cause) {
    super(message, cause);
  }
  public SchedulerException(Throwable cause) {
    super(cause);
  }
}

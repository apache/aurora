package com.twitter.aurora.scheduler.cron;

/**
 * Exception class to signal a failure in the underlying cron implementation.
 */
public class CronException extends Exception {
  public CronException(String msg) {
    super(msg);
  }

  public CronException(String msg, Throwable t) {
    super(msg, t);
  }

  public CronException(Throwable t) {
    super(t);
  }
}

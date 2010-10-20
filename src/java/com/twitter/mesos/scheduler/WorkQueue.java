package com.twitter.mesos.scheduler;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Work queue for asynchronous tasks performed by the mesos scheduler.
 *
 * @author wfarner
 */
public interface WorkQueue {
  /**
   * Executes a unit of work in the task queue.
   *
   * @param work The work to execute.
   */
  void doWork(Callable<Boolean> work);

  /**
   * Schedules work to execute after a minimum delay.
   *
   * @param work The work to execute.
   * @param executeDelay Amount of time to wait before making the work eligible for execution.
   * @param delayUnit Time unit for {@code executeDelay}.
   */
  void scheduleWork(Callable<Boolean> work, long executeDelay, TimeUnit delayUnit);
}

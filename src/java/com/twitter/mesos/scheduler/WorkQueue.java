package com.twitter.mesos.scheduler;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Variable;
import com.twitter.common.stats.VariableRegistry;
import com.twitter.common.util.BackoffStrategy;
import com.twitter.common.util.TruncatedBinaryBackoff;
import com.twitter.common.util.concurrent.BackingOffFutureTask;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Work queue for asynchronous tasks performed by the mesos scheduler.
 *
 * @author wfarner
 */
class WorkQueue {
  private ScheduledThreadPoolExecutor workQueue = new ScheduledThreadPoolExecutor(
      NUM_WORK_QUEUE_THREADS,
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("SchedulerWorkQueue-[%d]").build());
  private static final int NUM_WORK_QUEUE_THREADS = 1;
  private static final Amount<Long, Time> TASK_INITIAL_BACKOFF = Amount.of(1L, Time.SECONDS);
  private static final Amount<Long, Time> TASK_MAX_BACKOFF = Amount.of(64L, Time.SECONDS);
  private static final int TASK_MAX_RETRIES = 10;
  private static final BackoffStrategy TASK_BACKOFF_STRATEGY =
      new TruncatedBinaryBackoff(TASK_INITIAL_BACKOFF, TASK_MAX_BACKOFF);

  @Inject
  public WorkQueue() {
    VariableRegistry.register(new Variable<Integer>() {
      @Override public String getName() { return "work_queue_active_thread_count"; }
      @Override public Integer getValue() { return workQueue.getActiveCount(); }
    });
    VariableRegistry.register(new Variable<Long>() {
      @Override public String getName() { return "work_queue_completed_count"; }
      @Override public Long getValue() { return workQueue.getCompletedTaskCount(); }
    });
    VariableRegistry.register(new Variable<Long>() {
      @Override public String getName() { return "work_task_count"; }
      @Override public Long getValue() { return workQueue.getTaskCount(); }
    });
  }

  /**
   * Executes a unit of work in the task queue.
   *
   * @param work The work to execute.
   */
  public void doWork(Callable<Boolean> work) {
    workQueue.execute(
        new BackingOffFutureTask(workQueue, work, TASK_MAX_RETRIES, TASK_BACKOFF_STRATEGY));
  }

  /**
   * Schedules work to execute after a minimum delay.
   *
   * @param work The work to execute.
   * @param executeDelay Amount of time to wait before making the work eligible for execution.
   * @param delayUnit Time unit for {@code executeDelay}.
   */
  public void scheduleWork(Callable<Boolean> work, long executeDelay, TimeUnit delayUnit) {
    workQueue.schedule(
        new BackingOffFutureTask(workQueue, work, TASK_MAX_RETRIES, TASK_BACKOFF_STRATEGY),
        executeDelay, delayUnit);
  }
}

package com.twitter.mesos.scheduler.async;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.StatImpl;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.BackoffStrategy;
import com.twitter.common.util.TruncatedBinaryBackoff;
import com.twitter.mesos.scheduler.async.TaskScheduler.TaskSchedulerImpl;
import com.twitter.mesos.scheduler.events.TaskEventModule;

/**
 * Binding module for async task management.
 */
public class AsyncModule extends AbstractModule {

  @CmdLine(name = "async_worker_threads",
      help = "The number of worker threads to process async task operations with.")
  private static final Arg<Integer> ASYNC_WORKER_THREADS = Arg.create(1);

  @CmdLine(name = "transient_task_state_timeout",
      help = "The amount of time after which to treat a task stuck in a transient state as LOST.")
  private static final Arg<Amount<Long, Time>> TRANSIENT_TASK_STATE_TIMEOUT =
      Arg.create(Amount.of(5L, Time.MINUTES));

  @CmdLine(name = "initial_schedule_delay",
      help = "Initial amount of time to wait before attempting to schedule a PENDING task.")
  private static final Arg<Amount<Long, Time>> INITIAL_SCHEDULE_DELAY =
      Arg.create(Amount.of(1L, Time.SECONDS));

  @CmdLine(name = "max_schedule_delay",
      help = "Maximum delay between attempts to schedule a PENDING tasks.")
  private static final Arg<Amount<Long, Time>> MAX_SCHEDULE_DELAY =
      Arg.create(Amount.of(30L, Time.SECONDS));

  @Override
  protected void configure() {
    // AsyncModule itself is not a subclass of PrivateModule because TaskEventModule internally uses
    // a MultiBinder, which cannot span multiple injectors.
    binder().install(new PrivateModule() {
      @Override protected void configure() {
        bind(new TypeLiteral<Amount<Long, Time>>() { })
            .toInstance(TRANSIENT_TASK_STATE_TIMEOUT.get());

        // Don't worry about clean shutdown, these can be daemon and cleanup-free.
        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(
            ASYNC_WORKER_THREADS.get(),
            new ThreadFactoryBuilder().setNameFormat("AsyncHandler-%d").setDaemon(true).build());
        Stats.exportSize("async_queue_size", executor.getQueue());
        Stats.export(new StatImpl<Long>("async_tasks_completed") {
          @Override public Long read() {
            return executor.getCompletedTaskCount();
          }
        });

        bind(ScheduledExecutorService.class).toInstance(executor);

        bind(TaskTimeout.class).in(Singleton.class);
        expose(TaskTimeout.class);

        bind(BackoffStrategy.class).toInstance(
            new TruncatedBinaryBackoff(INITIAL_SCHEDULE_DELAY.get(), MAX_SCHEDULE_DELAY.get()));
        bind(TaskScheduler.class).to(TaskSchedulerImpl.class);
        bind(TaskSchedulerImpl.class).in(Singleton.class);
        expose(TaskScheduler.class);
      }
    });

    TaskEventModule.bindSubscriber(binder(), TaskScheduler.class);
    TaskEventModule.bindSubscriber(binder(), TaskTimeout.class);
  }
}

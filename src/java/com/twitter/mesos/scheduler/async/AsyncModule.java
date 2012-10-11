package com.twitter.mesos.scheduler.async;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.mesos.scheduler.events.TaskEventModule;

/**
 * Binding module for async task management.
 */
public class AsyncModule extends AbstractModule {

  @CmdLine(name = "transient_timeout_worker_threads",
      help = "The number of worker threads to process transient task timeouts with.")
  private static final Arg<Integer> TRANSIENT_TIMEOUT_WORKER_THREADS = Arg.create(1);

  @CmdLine(name = "transient_task_state_timeout",
      help = "The amount of time after which to treat a task stuck in a transient state as LOST.")
  private static final Arg<Amount<Long, Time>> TRANSIENT_TASK_STATE_TIMEOUT =
      Arg.create(Amount.of(5L, Time.MINUTES));

  @Override
  protected void configure() {
    // AsyncModule itself is not a subclass of PrivateModule because TaskEventModule internally uses
    // a MultiBinder, which cannot span multiple injectors.
    binder().install(new PrivateModule() {
      @Override protected void configure() {
        bind(new TypeLiteral<Amount<Long, Time>>() { })
            .toInstance(TRANSIENT_TASK_STATE_TIMEOUT.get());

        // Don't worry about clean shutdown, these can be daemon and cleanup-free.
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(
            TRANSIENT_TIMEOUT_WORKER_THREADS.get(),
            new ThreadFactoryBuilder().setNameFormat("TransientWorker-%d").setDaemon(true).build());
        bind(ScheduledExecutorService.class).toInstance(executor);

        bind(TaskTimeout.class).in(Singleton.class);
        expose(TaskTimeout.class);
      }
    });

    TaskEventModule.bindSubscriber(binder(), TaskTimeout.class);
  }
}

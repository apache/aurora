package com.twitter.aurora.scheduler.async;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import com.twitter.aurora.scheduler.async.OfferQueue.OfferQueueImpl;
import com.twitter.aurora.scheduler.async.OfferQueue.OfferReturnDelay;
import com.twitter.aurora.scheduler.async.TaskGroups.SchedulingAction;
import com.twitter.aurora.scheduler.configuration.ConfigurationManager;
import com.twitter.aurora.scheduler.events.TaskEventModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.StatImpl;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.BackoffStrategy;
import com.twitter.common.util.Random;
import com.twitter.common.util.TruncatedBinaryBackoff;

import static com.twitter.aurora.scheduler.async.HistoryPruner.PruneThreshold;

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

  @CmdLine(name = "min_offer_hold_time",
      help = "Minimum amount of time to hold a resource offer before declining.")
  private static final Arg<Amount<Integer, Time>> MIN_OFFER_HOLD_TIME =
      Arg.create(Amount.of(5, Time.MINUTES));

  @CmdLine(name = "history_prune_threshold",
      help = "Time after which the scheduler will prune terminated task history.")
  private static final Arg<Amount<Long, Time>> HISTORY_PRUNE_THRESHOLD =
      Arg.create(Amount.of(2L, Time.DAYS));

  @CmdLine(name = "max_schedule_attempts_per_sec",
      help = "Maximum number of scheduling attempts to make per second.")
  private static final Arg<Double> MAX_SCHEDULE_ATTEMPTS_PER_SEC = Arg.create(10D);

  @Override
  protected void configure() {
    // Don't worry about clean shutdown, these can be daemon and cleanup-free.
    final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(
        ASYNC_WORKER_THREADS.get(),
        new ThreadFactoryBuilder().setNameFormat("AsyncProcessor-%d").setDaemon(true).build());
    Stats.exportSize("timeout_queue_size", executor.getQueue());
    Stats.export(new StatImpl<Long>("async_tasks_completed") {
      @Override public Long read() {
        return executor.getCompletedTaskCount();
      }
    });

    // AsyncModule itself is not a subclass of PrivateModule because TaskEventModule internally uses
    // a MultiBinder, which cannot span multiple injectors.
    binder().install(new PrivateModule() {
      @Override protected void configure() {
        bind(new TypeLiteral<Amount<Long, Time>>() { })
            .toInstance(TRANSIENT_TASK_STATE_TIMEOUT.get());
        bind(ScheduledExecutorService.class).toInstance(executor);

        bind(TaskTimeout.class).in(Singleton.class);
        expose(TaskTimeout.class);
      }
    });
    TaskEventModule.bindSubscriber(binder(), TaskTimeout.class);

    binder().install(new PrivateModule() {
      @Override protected void configure() {
        bind(BackoffStrategy.class).toInstance(
            new TruncatedBinaryBackoff(INITIAL_SCHEDULE_DELAY.get(), MAX_SCHEDULE_DELAY.get()));
        bind(RateLimiter.class).toInstance(RateLimiter.create(MAX_SCHEDULE_ATTEMPTS_PER_SEC.get()));
        bind(SchedulingAction.class).to(TaskScheduler.class);
        bind(TaskScheduler.class).in(Singleton.class);
        bind(TaskGroups.class).in(Singleton.class);
        expose(TaskGroups.class);
      }
    });
    TaskEventModule.bindSubscriber(binder(), TaskGroups.class);

    binder().install(new PrivateModule() {
      @Override protected void configure() {
        bind(OfferReturnDelay.class).to(RandomJitterReturnDelay.class);
        bind(ScheduledExecutorService.class).toInstance(executor);
        bind(OfferQueue.class).to(OfferQueueImpl.class);
        bind(OfferQueueImpl.class).in(Singleton.class);
        expose(OfferQueue.class);
      }
    });
    TaskEventModule.bindSubscriber(binder(), OfferQueue.class);

    binder().install(new PrivateModule() {
      @Override protected void configure() {
        // TODO(ksweeney): Create a configuration validator module so this can be injected.
        bind(Integer.class).annotatedWith(PruneThreshold.class)
            .toInstance(ConfigurationManager.MAX_TASKS_PER_JOB.get() * 2);
        bind(new TypeLiteral<Amount<Long, Time>>() { }).annotatedWith(PruneThreshold.class)
            .toInstance(HISTORY_PRUNE_THRESHOLD.get());
        bind(ScheduledExecutorService.class).toInstance(executor);

        bind(HistoryPruner.class).in(Singleton.class);
        expose(HistoryPruner.class);
      }
    });
    TaskEventModule.bindSubscriber(binder(), HistoryPruner.class);
  }

  /**
   * Returns offers after a random duration within a fixed window.
   */
  private static class RandomJitterReturnDelay implements OfferReturnDelay {
    private static final int JITTER_WINDOW_MS = Amount.of(1, Time.MINUTES).as(Time.MILLISECONDS);

    private final int minHoldTimeMs = MIN_OFFER_HOLD_TIME.get().as(Time.MILLISECONDS);
    private final Random random = new Random.SystemRandom(new java.util.Random());

    @Override public Amount<Integer, Time> get() {
      return Amount.of(minHoldTimeMs + random.nextInt(JITTER_WINDOW_MS), Time.MILLISECONDS);
    }
  }
}

/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.async;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.logging.Logger;

import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.Positive;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.StatImpl;
import com.twitter.common.stats.Stats;
import com.twitter.common.stats.StatsProvider;
import com.twitter.common.util.Random;
import com.twitter.common.util.TruncatedBinaryBackoff;

import org.apache.aurora.scheduler.async.GcExecutorLauncher.GcExecutorSettings;
import org.apache.aurora.scheduler.async.GcExecutorLauncher.RandomGcExecutorSettings;
import org.apache.aurora.scheduler.async.HistoryPruner.HistoryPrunnerSettings;
import org.apache.aurora.scheduler.async.OfferQueue.OfferQueueImpl;
import org.apache.aurora.scheduler.async.OfferQueue.OfferReturnDelay;
import org.apache.aurora.scheduler.async.RescheduleCalculator.RescheduleCalculatorImpl;
import org.apache.aurora.scheduler.async.TaskGroups.TaskGroupsSettings;
import org.apache.aurora.scheduler.async.TaskScheduler.TaskSchedulerImpl;
import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.apache.aurora.scheduler.filter.AttributeAggregate;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import static org.apache.aurora.scheduler.async.Preemptor.PreemptorImpl;
import static org.apache.aurora.scheduler.async.Preemptor.PreemptorImpl.PreemptionDelay;
import static org.apache.aurora.scheduler.async.TaskScheduler.TaskSchedulerImpl.ReservationDuration;

/**
 * Binding module for async task management.
 */
public class AsyncModule extends AbstractModule {

  private static final Logger LOG = Logger.getLogger(AsyncModule.class.getName());

  @CmdLine(name = "async_worker_threads",
      help = "The number of worker threads to process async task operations with.")
  private static final Arg<Integer> ASYNC_WORKER_THREADS = Arg.create(1);

  @CmdLine(name = "transient_task_state_timeout",
      help = "The amount of time after which to treat a task stuck in a transient state as LOST.")
  private static final Arg<Amount<Long, Time>> TRANSIENT_TASK_STATE_TIMEOUT =
      Arg.create(Amount.of(5L, Time.MINUTES));

  @Positive
  @CmdLine(name = "initial_schedule_delay",
      help = "Initial amount of time to wait before attempting to schedule a PENDING task.")
  private static final Arg<Amount<Long, Time>> INITIAL_SCHEDULE_DELAY =
      Arg.create(Amount.of(1L, Time.MILLISECONDS));

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

  @CmdLine(name = "history_max_per_job_threshold",
      help = "Maximum number of terminated tasks to retain in a job history.")
  private static final Arg<Integer> HISTORY_MAX_PER_JOB_THRESHOLD = Arg.create(100);

  @CmdLine(name = "history_min_retention_threshold",
      help = "Minimum guaranteed time for task history retention before any pruning is attempted.")
  private static final Arg<Amount<Long, Time>> HISTORY_MIN_RETENTION_THRESHOLD =
      Arg.create(Amount.of(1L, Time.HOURS));

  @CmdLine(name = "max_schedule_attempts_per_sec",
      help = "Maximum number of scheduling attempts to make per second.")
  private static final Arg<Double> MAX_SCHEDULE_ATTEMPTS_PER_SEC = Arg.create(20D);

  @CmdLine(name = "flapping_task_threshold",
      help = "A task that repeatedly runs for less than this time is considered to be flapping.")
  private static final Arg<Amount<Long, Time>> FLAPPING_THRESHOLD =
      Arg.create(Amount.of(5L, Time.MINUTES));

  @CmdLine(name = "initial_flapping_task_delay",
      help = "Initial amount of time to wait before attempting to schedule a flapping task.")
  private static final Arg<Amount<Long, Time>> INITIAL_FLAPPING_DELAY =
      Arg.create(Amount.of(30L, Time.SECONDS));

  @CmdLine(name = "max_flapping_task_delay",
      help = "Maximum delay between attempts to schedule a flapping task.")
  private static final Arg<Amount<Long, Time>> MAX_FLAPPING_DELAY =
      Arg.create(Amount.of(5L, Time.MINUTES));

  @CmdLine(name = "max_reschedule_task_delay_on_startup",
      help = "Upper bound of random delay for pending task rescheduling on scheduler startup.")
  private static final Arg<Amount<Integer, Time>> MAX_RESCHEDULING_DELAY =
      Arg.create(Amount.of(30, Time.SECONDS));

  @CmdLine(name = "preemption_delay",
      help = "Time interval after which a pending task becomes eligible to preempt other tasks")
  private static final Arg<Amount<Long, Time>> PREEMPTION_DELAY =
      Arg.create(Amount.of(10L, Time.MINUTES));

  @CmdLine(name = "enable_preemptor",
      help = "Enable the preemptor and preemption")
  private static final Arg<Boolean> ENABLE_PREEMPTOR = Arg.create(true);

  private static final Preemptor NULL_PREEMPTOR = new Preemptor() {
    @Override
    public Optional<String> findPreemptionSlotFor(
        String taskId,
        AttributeAggregate attributeAggregate) {

      return Optional.absent();
    }
  };

  @CmdLine(name = "offer_reservation_duration", help = "Time to reserve a slave's offers while "
      + "trying to satisfy a task preempting another.")
  private static final Arg<Amount<Long, Time>> RESERVATION_DURATION =
      Arg.create(Amount.of(3L, Time.MINUTES));

  @BindingAnnotation
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  private @interface PreemptionBinding { }

  @VisibleForTesting
  static final Key<Preemptor> PREEMPTOR_KEY = Key.get(Preemptor.class, PreemptionBinding.class);

  @CmdLine(name = "executor_gc_interval",
      help = "Max interval on which to run the GC executor on a host to clean up dead tasks.")
  private static final Arg<Amount<Long, Time>> EXECUTOR_GC_INTERVAL =
      Arg.create(Amount.of(1L, Time.HOURS));

  @CmdLine(name = "gc_executor_path", help = "Path to the gc executor launch script.")
  private static final Arg<String> GC_EXECUTOR_PATH = Arg.create(null);

  @Override
  protected void configure() {
    // Don't worry about clean shutdown, these can be daemon and cleanup-free.
    final ScheduledThreadPoolExecutor executor =
        AsyncUtil.loggingScheduledExecutor(ASYNC_WORKER_THREADS.get(), "AsyncProcessor-%d", LOG);

    Stats.exportSize("timeout_queue_size", executor.getQueue());
    Stats.export(new StatImpl<Long>("async_tasks_completed") {
      @Override
      public Long read() {
        return executor.getCompletedTaskCount();
      }
    });

    // AsyncModule itself is not a subclass of PrivateModule because TaskEventModule internally uses
    // a MultiBinder, which cannot span multiple injectors.
    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(new TypeLiteral<Amount<Long, Time>>() { })
            .toInstance(TRANSIENT_TASK_STATE_TIMEOUT.get());
        bind(ScheduledExecutorService.class).toInstance(executor);

        bind(TaskTimeout.class).in(Singleton.class);
        requireBinding(StatsProvider.class);
        expose(TaskTimeout.class);
      }
    });
    PubsubEventModule.bindSubscriber(binder(), TaskTimeout.class);

    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(TaskGroupsSettings.class).toInstance(new TaskGroupsSettings(
            new TruncatedBinaryBackoff(INITIAL_SCHEDULE_DELAY.get(), MAX_SCHEDULE_DELAY.get()),
            RateLimiter.create(MAX_SCHEDULE_ATTEMPTS_PER_SEC.get())));

        bind(RescheduleCalculatorImpl.RescheduleCalculatorSettings.class)
            .toInstance(new RescheduleCalculatorImpl.RescheduleCalculatorSettings(
                new TruncatedBinaryBackoff(INITIAL_FLAPPING_DELAY.get(), MAX_FLAPPING_DELAY.get()),
                FLAPPING_THRESHOLD.get(),
                MAX_RESCHEDULING_DELAY.get()));

        bind(RescheduleCalculator.class).to(RescheduleCalculatorImpl.class).in(Singleton.class);
        expose(RescheduleCalculator.class);
        if (ENABLE_PREEMPTOR.get()) {
          bind(PREEMPTOR_KEY).to(PreemptorImpl.class);
          bind(PreemptorImpl.class).in(Singleton.class);
          LOG.info("Preemptor Enabled.");
        } else {
          bind(PREEMPTOR_KEY).toInstance(NULL_PREEMPTOR);
          LOG.warning("Preemptor Disabled.");
        }
        expose(PREEMPTOR_KEY);
        bind(new TypeLiteral<Amount<Long, Time>>() { }).annotatedWith(PreemptionDelay.class)
            .toInstance(PREEMPTION_DELAY.get());
        bind(TaskGroups.class).in(Singleton.class);
        expose(TaskGroups.class);
      }
    });
    bindTaskScheduler(binder(), PREEMPTOR_KEY, RESERVATION_DURATION.get());
    PubsubEventModule.bindSubscriber(binder(), TaskGroups.class);

    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(OfferReturnDelay.class).to(RandomJitterReturnDelay.class);
        bind(ScheduledExecutorService.class).toInstance(executor);
        bind(OfferQueue.class).to(OfferQueueImpl.class);
        bind(OfferQueueImpl.class).in(Singleton.class);
        expose(OfferQueue.class);
      }
    });
    PubsubEventModule.bindSubscriber(binder(), OfferQueue.class);

    install(new PrivateModule() {
      @Override
      protected void configure() {
        // TODO(ksweeney): Create a configuration validator module so this can be injected.
        // TODO(William Farner): Revert this once large task counts is cheap ala hierarchichal store
        bind(HistoryPrunnerSettings.class).toInstance(new HistoryPrunnerSettings(
            HISTORY_PRUNE_THRESHOLD.get(),
            HISTORY_MIN_RETENTION_THRESHOLD.get(),
            HISTORY_MAX_PER_JOB_THRESHOLD.get()
        ));
        bind(ScheduledExecutorService.class).toInstance(executor);

        bind(HistoryPruner.class).in(Singleton.class);
        expose(HistoryPruner.class);
      }
    });
    PubsubEventModule.bindSubscriber(binder(), HistoryPruner.class);

    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(ScheduledExecutorService.class).toInstance(executor);
        bind(TaskThrottler.class).in(Singleton.class);
        expose(TaskThrottler.class);
      }
    });
    PubsubEventModule.bindSubscriber(binder(), TaskThrottler.class);

    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(GcExecutorSettings.class).toInstance(new RandomGcExecutorSettings(
            EXECUTOR_GC_INTERVAL.get(),
            Optional.fromNullable(GC_EXECUTOR_PATH.get())));
        bind(Executor.class).toInstance(executor);

        bind(GcExecutorLauncher.class).in(Singleton.class);
        expose(GcExecutorLauncher.class);
      }
    });
  }

  /**
   * This method exists because we want to test the wiring up of TaskSchedulerImpl class to the
   * PubSub system in the TaskSchedulerImplTest class. The method has a complex signature because
   * the binding of the TaskScheduler and friends occurs in a PrivateModule which does not interact
   * well with the MultiBinder that backs the PubSub system.
   */
  @VisibleForTesting
  static void bindTaskScheduler(
      Binder binder,
      final Key<Preemptor> preemptorKey,
      final Amount<Long, Time> reservationDuration) {
        binder.install(new PrivateModule() {
          @Override
          protected void configure() {
            bind(Preemptor.class).to(preemptorKey);
            bind(new TypeLiteral<Amount<Long, Time>>() { }).annotatedWith(ReservationDuration.class)
                .toInstance(reservationDuration);
            bind(TaskScheduler.class).to(TaskSchedulerImpl.class);
            bind(TaskSchedulerImpl.class).in(Singleton.class);
            expose(TaskScheduler.class);
          }
        });
        PubsubEventModule.bindSubscriber(binder, TaskScheduler.class);
  }

  /**
   * Returns offers after a random duration within a fixed window.
   */
  private static class RandomJitterReturnDelay implements OfferReturnDelay {
    private static final int JITTER_WINDOW_MS = Amount.of(1, Time.MINUTES).as(Time.MILLISECONDS);

    private final int minHoldTimeMs = MIN_OFFER_HOLD_TIME.get().as(Time.MILLISECONDS);
    private final Random random = new Random.SystemRandom(new java.util.Random());

    @Override
    public Amount<Integer, Time> get() {
      return Amount.of(minHoldTimeMs + random.nextInt(JITTER_WINDOW_MS), Time.MILLISECONDS);
    }
  }
}

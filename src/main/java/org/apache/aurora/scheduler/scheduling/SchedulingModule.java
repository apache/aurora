/**
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
package org.apache.aurora.scheduler.scheduling;

import javax.inject.Singleton;

import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;

import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.common.args.constraints.Positive;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.util.TruncatedBinaryBackoff;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.apache.aurora.scheduler.preemptor.BiCache;
import org.apache.aurora.scheduler.scheduling.RescheduleCalculator.RescheduleCalculatorImpl;

import static org.apache.aurora.scheduler.SchedulerServicesModule.addSchedulerActiveServiceBinding;

/**
 * Binding module for task scheduling logic.
 */
public class SchedulingModule extends AbstractModule {

  @CmdLine(name = "max_schedule_attempts_per_sec",
      help = "Maximum number of scheduling attempts to make per second.")
  private static final Arg<Double> MAX_SCHEDULE_ATTEMPTS_PER_SEC = Arg.create(40D);

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

  @Positive
  @CmdLine(name = "first_schedule_delay",
      help = "Initial amount of time to wait before first attempting to schedule a PENDING task.")
  private static final Arg<Amount<Long, Time>> FIRST_SCHEDULE_DELAY =
      Arg.create(Amount.of(1L, Time.MILLISECONDS));

  @Positive
  @CmdLine(name = "initial_schedule_penalty",
      help = "Initial amount of time to wait before attempting to schedule a task that has failed"
          + " to schedule.")
  private static final Arg<Amount<Long, Time>> INITIAL_SCHEDULE_PENALTY =
      Arg.create(Amount.of(1L, Time.SECONDS));

  @CmdLine(name = "max_schedule_penalty",
      help = "Maximum delay between attempts to schedule a PENDING tasks.")
  private static final Arg<Amount<Long, Time>> MAX_SCHEDULE_PENALTY =
      Arg.create(Amount.of(1L, Time.MINUTES));

  @CmdLine(name = "offer_reservation_duration", help = "Time to reserve a agent's offers while "
      + "trying to satisfy a task preempting another.")
  private static final Arg<Amount<Long, Time>> RESERVATION_DURATION =
      Arg.create(Amount.of(3L, Time.MINUTES));

  @Positive
  @CmdLine(name = "scheduling_max_batch_size",
      help = "The maximum number of scheduling attempts that can be processed in a batch.")
  private static final Arg<Integer> SCHEDULING_MAX_BATCH_SIZE = Arg.create(3);

  @Positive
  @CmdLine(name = "max_tasks_per_schedule_attempt",
      help = "The maximum number of tasks to pick in a single scheduling attempt.")
  private static final Arg<Integer> MAX_TASKS_PER_SCHEDULE_ATTEMPT = Arg.create(5);

  @Override
  protected void configure() {
    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(TaskGroups.TaskGroupsSettings.class).toInstance(new TaskGroups.TaskGroupsSettings(
            FIRST_SCHEDULE_DELAY.get(),
            new TruncatedBinaryBackoff(
                INITIAL_SCHEDULE_PENALTY.get(),
                MAX_SCHEDULE_PENALTY.get()),
            RateLimiter.create(MAX_SCHEDULE_ATTEMPTS_PER_SEC.get()),
            MAX_TASKS_PER_SCHEDULE_ATTEMPT.get()));

        bind(RescheduleCalculatorImpl.RescheduleCalculatorSettings.class)
            .toInstance(new RescheduleCalculatorImpl.RescheduleCalculatorSettings(
                new TruncatedBinaryBackoff(INITIAL_FLAPPING_DELAY.get(), MAX_FLAPPING_DELAY.get()),
                FLAPPING_THRESHOLD.get(),
                MAX_RESCHEDULING_DELAY.get()));

        bind(RescheduleCalculator.class).to(RescheduleCalculatorImpl.class).in(Singleton.class);
        expose(RescheduleCalculator.class);
        bind(TaskGroups.class).in(Singleton.class);
        expose(TaskGroups.class);
      }
    });
    PubsubEventModule.bindSubscriber(binder(), TaskGroups.class);

    bind(new TypeLiteral<Integer>() { })
        .annotatedWith(TaskGroups.SchedulingMaxBatchSize.class)
        .toInstance(SCHEDULING_MAX_BATCH_SIZE.get());
    bind(TaskGroups.TaskGroupBatchWorker.class).in(Singleton.class);
    addSchedulerActiveServiceBinding(binder()).to(TaskGroups.TaskGroupBatchWorker.class);

    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(new TypeLiteral<BiCache<String, TaskGroupKey>>() { }).in(Singleton.class);
        bind(BiCache.BiCacheSettings.class).toInstance(
            new BiCache.BiCacheSettings(RESERVATION_DURATION.get(), "reservation_cache_size"));
        bind(TaskScheduler.class).to(TaskScheduler.TaskSchedulerImpl.class);
        bind(TaskScheduler.TaskSchedulerImpl.class).in(Singleton.class);
        expose(TaskScheduler.class);
      }
    });
    PubsubEventModule.bindSubscriber(binder(), TaskScheduler.class);

    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(TaskThrottler.class).in(Singleton.class);
        expose(TaskThrottler.class);
      }
    });
    PubsubEventModule.bindSubscriber(binder(), TaskThrottler.class);
  }
}

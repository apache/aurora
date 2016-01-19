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
import org.apache.aurora.scheduler.preemptor.BiCache.BiCacheSettings;
import org.apache.aurora.scheduler.scheduling.RescheduleCalculator.RescheduleCalculatorImpl;

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

  @CmdLine(name = "offer_reservation_duration", help = "Time to reserve a slave's offers while "
      + "trying to satisfy a task preempting another.")
  private static final Arg<Amount<Long, Time>> RESERVATION_DURATION =
      Arg.create(Amount.of(3L, Time.MINUTES));

  interface Params {
    double maxScheduleAttemptsPerSec();

    Amount<Long, Time> flappingTaskThreshold();

    Amount<Long, Time> initialFlappingTaskDelay();

    Amount<Long, Time> maxFlappingTaskDelay();

    Amount<Integer, Time> maxRescheduleTaskDelayOnStartup();

    Amount<Long, Time> firstScheduleDelay();

    Amount<Long, Time> initialSchedulePenalty();

    Amount<Long, Time> maxSchedulePenalty();

    Amount<Long, Time> offerReservationDuration();
  }

  private final Params params;

  public SchedulingModule() {
    this.params = new Params() {
      @Override
      public double maxScheduleAttemptsPerSec() {
        return MAX_SCHEDULE_ATTEMPTS_PER_SEC.get();
      }

      @Override
      public Amount<Long, Time> flappingTaskThreshold() {
        return FLAPPING_THRESHOLD.get();
      }

      @Override
      public Amount<Long, Time> initialFlappingTaskDelay() {
        return INITIAL_FLAPPING_DELAY.get();
      }

      @Override
      public Amount<Long, Time> maxFlappingTaskDelay() {
        return MAX_FLAPPING_DELAY.get();
      }

      @Override
      public Amount<Integer, Time> maxRescheduleTaskDelayOnStartup() {
        return MAX_RESCHEDULING_DELAY.get();
      }

      @Override
      public Amount<Long, Time> firstScheduleDelay() {
        return FIRST_SCHEDULE_DELAY.get();
      }

      @Override
      public Amount<Long, Time> initialSchedulePenalty() {
        return INITIAL_SCHEDULE_PENALTY.get();
      }

      @Override
      public Amount<Long, Time> maxSchedulePenalty() {
        return MAX_SCHEDULE_PENALTY.get();
      }

      @Override
      public Amount<Long, Time> offerReservationDuration() {
        return RESERVATION_DURATION.get();
      }
    };
  }

  @Override
  protected void configure() {
    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(TaskGroups.TaskGroupsSettings.class).toInstance(new TaskGroups.TaskGroupsSettings(
            params.firstScheduleDelay(),
            new TruncatedBinaryBackoff(
                params.initialSchedulePenalty(),
                params.maxSchedulePenalty()),
            RateLimiter.create(params.maxScheduleAttemptsPerSec())));

        bind(RescheduleCalculatorImpl.RescheduleCalculatorSettings.class)
            .toInstance(new RescheduleCalculatorImpl.RescheduleCalculatorSettings(
                new TruncatedBinaryBackoff(
                    params.initialFlappingTaskDelay(),
                    params.maxFlappingTaskDelay()),
                params.flappingTaskThreshold(),
                params.maxRescheduleTaskDelayOnStartup()));

        bind(RescheduleCalculator.class).to(RescheduleCalculatorImpl.class).in(Singleton.class);
        expose(RescheduleCalculator.class);
        bind(TaskGroups.class).in(Singleton.class);
        expose(TaskGroups.class);
      }
    });
    PubsubEventModule.bindSubscriber(binder(), TaskGroups.class);

    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(new TypeLiteral<BiCache<String, TaskGroupKey>>() { }).in(Singleton.class);
        bind(BiCacheSettings.class).toInstance(
            new BiCacheSettings(params.offerReservationDuration(), "reservation_cache_size"));
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

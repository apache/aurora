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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;

import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.util.TruncatedBinaryBackoff;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.config.validators.PositiveAmount;
import org.apache.aurora.scheduler.config.validators.PositiveNumber;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.apache.aurora.scheduler.preemptor.BiCache;
import org.apache.aurora.scheduler.scheduling.RescheduleCalculator.RescheduleCalculatorImpl;

import static org.apache.aurora.scheduler.SchedulerServicesModule.addSchedulerActiveServiceBinding;

/**
 * Binding module for task scheduling logic.
 */
public class SchedulingModule extends AbstractModule {

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-max_schedule_attempts_per_sec",
        description = "Maximum number of scheduling attempts to make per second.")
    public double maxScheduleAttemptsPerSec = 40D;

    @Parameter(names = "-flapping_task_threshold",
        description =
            "A task that repeatedly runs for less than this time is considered to be flapping.")
    public TimeAmount flappingThreshold = new TimeAmount(5, Time.MINUTES);

    @Parameter(names = "-initial_flapping_task_delay",
        description =
            "Initial amount of time to wait before attempting to schedule a flapping task.")
    public TimeAmount initialFlappingDelay = new TimeAmount(30, Time.SECONDS);

    @Parameter(names = "-max_flapping_task_delay",
        description = "Maximum delay between attempts to schedule a flapping task.")
    public TimeAmount maxFlappingDelay = new TimeAmount(5, Time.MINUTES);

    @Parameter(names = "-max_reschedule_task_delay_on_startup",
        description =
            "Upper bound of random delay for pending task rescheduling on scheduler startup.")
    public TimeAmount maxReschedulingDelay = new TimeAmount(30, Time.SECONDS);

    @Parameter(names = "-first_schedule_delay",
        validateValueWith = PositiveAmount.class,
        description =
            "Initial amount of time to wait before first attempting to schedule a PENDING task.")
    public TimeAmount firstScheduleDelay = new TimeAmount(1, Time.MILLISECONDS);

    @Parameter(names = "-initial_schedule_penalty",
        validateValueWith = PositiveAmount.class,
        description = "Initial amount of time to wait before attempting to schedule a task that "
            + "has failed to schedule.")
    public TimeAmount initialSchedulePenalty = new TimeAmount(1, Time.SECONDS);

    @Parameter(names = "-max_schedule_penalty",
        description = "Maximum delay between attempts to schedule a PENDING tasks.")
    public TimeAmount maxSchedulePenalty = new TimeAmount(1, Time.MINUTES);

    @Parameter(names = "-offer_reservation_duration",
        description = "Time to reserve a agent's offers while "
            + "trying to satisfy a task preempting another.")
    public TimeAmount reservationDuration = new TimeAmount(3, Time.MINUTES);

    @Parameter(names = "-scheduling_max_batch_size",
        validateValueWith = PositiveNumber.class,
        description = "The maximum number of scheduling attempts that can be processed in a batch.")
    public int schedulingMaxBatchSize = 3;

    @Parameter(names = "-max_tasks_per_schedule_attempt",
        validateValueWith = PositiveNumber.class,
        description = "The maximum number of tasks to pick in a single scheduling attempt.")
    public int maxTasksPerScheduleAttempt = 5;
  }

  private final Options options;

  public SchedulingModule(Options options) {
    this.options = options;
  }

  @Override
  protected void configure() {
    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(TaskGroups.TaskGroupsSettings.class).toInstance(new TaskGroups.TaskGroupsSettings(
            options.firstScheduleDelay,
            new TruncatedBinaryBackoff(options.initialSchedulePenalty, options.maxSchedulePenalty),
            RateLimiter.create(options.maxScheduleAttemptsPerSec),
            options.maxTasksPerScheduleAttempt));

        bind(RescheduleCalculatorImpl.RescheduleCalculatorSettings.class)
            .toInstance(new RescheduleCalculatorImpl.RescheduleCalculatorSettings(
                new TruncatedBinaryBackoff(options.initialFlappingDelay, options.maxFlappingDelay),
                options.flappingThreshold,
                options.maxReschedulingDelay));

        bind(RescheduleCalculator.class).to(RescheduleCalculatorImpl.class).in(Singleton.class);
        expose(RescheduleCalculator.class);
        bind(TaskGroups.class).in(Singleton.class);
        expose(TaskGroups.class);
      }
    });
    PubsubEventModule.bindSubscriber(binder(), TaskGroups.class);

    bind(new TypeLiteral<Integer>() { })
        .annotatedWith(TaskGroups.SchedulingMaxBatchSize.class)
        .toInstance(options.schedulingMaxBatchSize);
    bind(TaskGroups.TaskGroupBatchWorker.class).in(Singleton.class);
    addSchedulerActiveServiceBinding(binder()).to(TaskGroups.TaskGroupBatchWorker.class);

    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(new TypeLiteral<BiCache<String, TaskGroupKey>>() { }).in(Singleton.class);
        bind(BiCache.BiCacheSettings.class).toInstance(
            new BiCache.BiCacheSettings(options.reservationDuration, "reservation"));
        bind(TaskScheduler.class).to(TaskSchedulerImpl.class);
        bind(TaskSchedulerImpl.class).in(Singleton.class);
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

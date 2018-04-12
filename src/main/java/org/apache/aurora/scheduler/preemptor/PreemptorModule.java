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
package org.apache.aurora.scheduler.preemptor;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.List;
import java.util.Optional;

import javax.inject.Inject;
import javax.inject.Qualifier;
import javax.inject.Singleton;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.app.MoreModules;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.config.CliOptions;
import org.apache.aurora.scheduler.config.splitters.CommaSplitter;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.config.validators.PositiveAmount;
import org.apache.aurora.scheduler.config.validators.PositiveNumber;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.apache.aurora.scheduler.preemptor.BiCache.BiCacheSettings;
import org.apache.aurora.scheduler.state.ClusterStateImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

public class PreemptorModule extends AbstractModule {

  private static final Logger LOG = LoggerFactory.getLogger(PreemptorModule.class);

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-enable_preemptor",
        description = "Enable the preemptor and preemption",
        arity = 1)
    public boolean enablePreemptor = true;

    @Parameter(names = "-preemption_delay",
        validateValueWith = PositiveAmount.class,
        description =
            "Time interval after which a pending task becomes eligible to preempt other tasks")
    public TimeAmount preemptionDelay = new TimeAmount(3, Time.MINUTES);

    @Parameter(names = "-preemption_slot_hold_time",
        validateValueWith = PositiveAmount.class,
        description = "Time to hold a preemption slot found before it is discarded.")
    public TimeAmount preemptionSlotHoldTime = new TimeAmount(5, Time.MINUTES);

    @Parameter(names = "-preemption_slot_search_initial_delay",
        validateValueWith = PositiveAmount.class,
        description =
            "Initial amount of time to delay preemption slot searching after scheduler start up.")
    public TimeAmount preemptionSlotSearchInitialDelay = new TimeAmount(3, Time.MINUTES);

    @Parameter(names = "-preemption_slot_search_interval",
        validateValueWith = PositiveAmount.class,
        description = "Time interval between pending task preemption slot searches.")
    public TimeAmount preemptionSlotSearchInterval = new TimeAmount(1, Time.MINUTES);

    @Parameter(names = "-preemption_reservation_max_batch_size",
        validateValueWith = PositiveNumber.class,
        description = "The maximum number of reservations for a task group to be made in a batch.")
    public int reservationMaxBatchSize = 5;

    @Parameter(names = "-preemption_slot_finder_modules",
        description = "Guice modules for custom preemption slot searching for pending tasks.",
        splitter = CommaSplitter.class)
    @SuppressWarnings("rawtypes")
    public List<Class> slotFinderModules = ImmutableList.of(
        PendingTaskProcessorModule.class,
        PreemptionVictimFilterModule.class);
  }

  private final CliOptions cliOptions;

  /*
   * Binding annotation for the async processor that finds preemption slots.
   */
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  public @interface PreemptionSlotFinder { }

  public PreemptorModule(CliOptions cliOptions) {
    this.cliOptions = cliOptions;
  }

  @Override
  protected void configure() {
    Options options = cliOptions.preemptor;
    install(new PrivateModule() {
      @Override
      protected void configure() {
        if (options.enablePreemptor) {
          LOG.info("Preemptor Enabled.");
          bind(PreemptorMetrics.class).in(Singleton.class);
          bind(Preemptor.class).to(Preemptor.PreemptorImpl.class);
          bind(Preemptor.PreemptorImpl.class).in(Singleton.class);
          bind(new TypeLiteral<Amount<Long, Time>>() { })
              .annotatedWith(PendingTaskProcessor.PreemptionDelay.class)
              .toInstance(options.preemptionDelay);
          bind(BiCacheSettings.class).toInstance(
              new BiCacheSettings(options.preemptionSlotHoldTime, "preemption_slot"));
          bind(new TypeLiteral<BiCache<PreemptionProposal, TaskGroupKey>>() { })
              .in(Singleton.class);
          bind(new TypeLiteral<Integer>() { })
              .annotatedWith(PendingTaskProcessor.ReservationBatchSize.class)
              .toInstance(options.reservationMaxBatchSize);

          for (Module module: MoreModules.instantiateAll(options.slotFinderModules, cliOptions)) {
            install(module);
          }

          // We need to convert the initial delay time unit to be the same as the search interval
          long preemptionSlotSearchInitialDelay = options.preemptionSlotSearchInitialDelay
              .as(options.preemptionSlotSearchInterval.getUnit());
          bind(PreemptorService.class).in(Singleton.class);
          bind(AbstractScheduledService.Scheduler.class).toInstance(
              AbstractScheduledService.Scheduler.newFixedRateSchedule(
                  preemptionSlotSearchInitialDelay,
                  options.preemptionSlotSearchInterval.getValue(),
                  options.preemptionSlotSearchInterval.getUnit().getTimeUnit()));

          expose(PreemptorService.class);
          expose(Runnable.class).annotatedWith(PreemptionSlotFinder.class);
        } else {
          bind(Preemptor.class).toInstance(NULL_PREEMPTOR);
          LOG.warn("Preemptor Disabled.");
        }
        expose(Preemptor.class);
      }
    });

    // We can't do this in the private module due to the known conflict between multibindings
    // and private modules due to multiple injectors.  We accept the added complexity here to keep
    // the other bindings private.
    PubsubEventModule.bindSubscriber(binder(), ClusterStateImpl.class);
    if (options.enablePreemptor) {
      SchedulerServicesModule.addSchedulerActiveServiceBinding(binder())
          .to(PreemptorService.class);
    }
  }

  static class PreemptorService extends AbstractScheduledService {
    private final Runnable slotFinder;
    private final Scheduler schedule;

    @Inject
    PreemptorService(@PreemptionSlotFinder Runnable slotFinder, Scheduler schedule) {
      this.slotFinder = requireNonNull(slotFinder);
      this.schedule = requireNonNull(schedule);
    }

    @Override
    protected void runOneIteration() {
      slotFinder.run();
    }

    @Override
    protected Scheduler scheduler() {
      return schedule;
    }
  }

  private static final Preemptor NULL_PREEMPTOR =
      (task, jobState, storeProvider) -> Optional.empty();
}

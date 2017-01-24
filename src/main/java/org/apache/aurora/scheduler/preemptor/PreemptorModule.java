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

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;

import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.common.args.constraints.Positive;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.apache.aurora.scheduler.preemptor.BiCache.BiCacheSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

public class PreemptorModule extends AbstractModule {

  private static final Logger LOG = LoggerFactory.getLogger(PreemptorModule.class);

  @CmdLine(name = "enable_preemptor",
      help = "Enable the preemptor and preemption")
  private static final Arg<Boolean> ENABLE_PREEMPTOR = Arg.create(true);

  @CmdLine(name = "preemption_delay",
      help = "Time interval after which a pending task becomes eligible to preempt other tasks")
  private static final Arg<Amount<Long, Time>> PREEMPTION_DELAY =
      Arg.create(Amount.of(3L, Time.MINUTES));

  @CmdLine(name = "preemption_slot_hold_time",
      help = "Time to hold a preemption slot found before it is discarded.")
  private static final Arg<Amount<Long, Time>> PREEMPTION_SLOT_HOLD_TIME =
      Arg.create(Amount.of(5L, Time.MINUTES));

  @CmdLine(name = "preemption_slot_search_interval",
      help = "Time interval between pending task preemption slot searches.")
  private static final Arg<Amount<Long, Time>> PREEMPTION_SLOT_SEARCH_INTERVAL =
      Arg.create(Amount.of(1L, Time.MINUTES));

  @Positive
  @CmdLine(name = "preemption_reservation_max_batch_size",
      help = "The maximum number of reservations for a task group to be made in a batch.")
  private static final Arg<Integer> RESERVATION_MAX_BATCH_SIZE = Arg.create(5);

  private final boolean enablePreemptor;
  private final Amount<Long, Time> preemptionDelay;
  private final Amount<Long, Time> slotSearchInterval;
  private final Integer reservationBatchSize;

  @VisibleForTesting
  public PreemptorModule(
      boolean enablePreemptor,
      Amount<Long, Time> preemptionDelay,
      Amount<Long, Time> slotSearchInterval,
      Integer reservationBatchSize) {

    this.enablePreemptor = enablePreemptor;
    this.preemptionDelay = requireNonNull(preemptionDelay);
    this.slotSearchInterval = requireNonNull(slotSearchInterval);
    this.reservationBatchSize = requireNonNull(reservationBatchSize);
  }

  public PreemptorModule() {
    this(
        ENABLE_PREEMPTOR.get(),
        PREEMPTION_DELAY.get(),
        PREEMPTION_SLOT_SEARCH_INTERVAL.get(),
        RESERVATION_MAX_BATCH_SIZE.get());
  }

  @Override
  protected void configure() {
    install(new PrivateModule() {
      @Override
      protected void configure() {
        if (enablePreemptor) {
          LOG.info("Preemptor Enabled.");
          bind(PreemptorMetrics.class).in(Singleton.class);
          bind(PreemptionVictimFilter.class)
              .to(PreemptionVictimFilter.PreemptionVictimFilterImpl.class);
          bind(PreemptionVictimFilter.PreemptionVictimFilterImpl.class).in(Singleton.class);
          bind(Preemptor.class).to(Preemptor.PreemptorImpl.class);
          bind(Preemptor.PreemptorImpl.class).in(Singleton.class);
          bind(new TypeLiteral<Amount<Long, Time>>() { })
              .annotatedWith(PendingTaskProcessor.PreemptionDelay.class)
              .toInstance(preemptionDelay);
          bind(BiCacheSettings.class).toInstance(
              new BiCacheSettings(PREEMPTION_SLOT_HOLD_TIME.get(), "preemption_slot_cache_size"));
          bind(new TypeLiteral<BiCache<PreemptionProposal, TaskGroupKey>>() { })
              .in(Singleton.class);
          bind(new TypeLiteral<Integer>() { })
              .annotatedWith(PendingTaskProcessor.ReservationBatchSize.class)
              .toInstance(reservationBatchSize);
          bind(PendingTaskProcessor.class).in(Singleton.class);
          bind(ClusterState.class).to(ClusterStateImpl.class);
          bind(ClusterStateImpl.class).in(Singleton.class);
          expose(ClusterStateImpl.class);

          bind(PreemptorService.class).in(Singleton.class);
          bind(AbstractScheduledService.Scheduler.class).toInstance(
              AbstractScheduledService.Scheduler.newFixedRateSchedule(
                  0L,
                  slotSearchInterval.getValue(),
                  slotSearchInterval.getUnit().getTimeUnit()));

          expose(PreemptorService.class);
          expose(PendingTaskProcessor.class);
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
    if (enablePreemptor) {
      SchedulerServicesModule.addSchedulerActiveServiceBinding(binder())
          .to(PreemptorService.class);
    }
  }

  static class PreemptorService extends AbstractScheduledService {
    private final PendingTaskProcessor slotFinder;
    private final Scheduler schedule;

    @Inject
    PreemptorService(PendingTaskProcessor slotFinder, Scheduler schedule) {
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
      (task, jobState, storeProvider) -> Optional.absent();
}

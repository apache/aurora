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
package org.apache.aurora.scheduler.async.preemptor;

import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.async.preemptor.BiCache.BiCacheSettings;
import org.apache.aurora.scheduler.async.preemptor.PreemptionSlotFinder.PreemptionSlot;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;

import static java.util.Objects.requireNonNull;

public class PreemptorModule extends AbstractModule {

  private static final Logger LOG = Logger.getLogger(PreemptorModule.class.getName());

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

  private final boolean enablePreemptor;
  private final Amount<Long, Time> preemptionDelay;
  private final Amount<Long, Time> slotSearchInterval;

  @VisibleForTesting
  public PreemptorModule(
      boolean enablePreemptor,
      Amount<Long, Time> preemptionDelay,
      Amount<Long, Time> slotSearchInterval) {

    this.enablePreemptor = enablePreemptor;
    this.preemptionDelay = requireNonNull(preemptionDelay);
    this.slotSearchInterval = requireNonNull(slotSearchInterval);
  }

  public PreemptorModule() {
    this(ENABLE_PREEMPTOR.get(), PREEMPTION_DELAY.get(), PREEMPTION_SLOT_SEARCH_INTERVAL.get());
  }

  @Override
  protected void configure() {
    install(new PrivateModule() {
      @Override
      protected void configure() {
        if (enablePreemptor) {
          LOG.info("Preemptor Enabled.");
          bind(PreemptorMetrics.class).in(Singleton.class);
          bind(PreemptionSlotFinder.class)
              .to(PreemptionSlotFinder.PreemptionSlotFinderImpl.class);
          bind(PreemptionSlotFinder.PreemptionSlotFinderImpl.class).in(Singleton.class);
          bind(Preemptor.class).to(Preemptor.PreemptorImpl.class);
          bind(Preemptor.PreemptorImpl.class).in(Singleton.class);
          bind(new TypeLiteral<Amount<Long, Time>>() { })
              .annotatedWith(PendingTaskProcessor.PreemptionDelay.class)
              .toInstance(preemptionDelay);
          bind(BiCacheSettings.class).toInstance(
              new BiCacheSettings(PREEMPTION_SLOT_HOLD_TIME.get(), "preemption_slot_cache_size"));
          bind(new TypeLiteral<BiCache<PreemptionSlot, TaskGroupKey>>() { }).in(Singleton.class);
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
        } else {
          bind(Preemptor.class).toInstance(NULL_PREEMPTOR);
          LOG.warning("Preemptor Disabled.");
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

  private static final Preemptor NULL_PREEMPTOR = new Preemptor() {
    @Override
    public Optional<String> attemptPreemptionFor(
        IAssignedTask task,
        AttributeAggregate jobState,
        Storage.MutableStoreProvider storeProvider) {

      return Optional.absent();
    }
  };
}

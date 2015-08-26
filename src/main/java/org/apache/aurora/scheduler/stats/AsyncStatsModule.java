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
package org.apache.aurora.scheduler.stats;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.AbstractScheduledService.Scheduler;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;

import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.Resources;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.base.Conversions;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.stats.SlotSizeCounter.MachineResource;
import org.apache.aurora.scheduler.stats.SlotSizeCounter.MachineResourceProvider;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.ResourceSlot.NONE;
import static org.apache.aurora.scheduler.Resources.NON_REVOCABLE;
import static org.apache.aurora.scheduler.Resources.REVOCABLE;

/**
 * Module to configure export of cluster-wide resource allocation and consumption statistics.
 */
public class AsyncStatsModule extends AbstractModule {
  @CmdLine(name = "async_task_stat_update_interval",
      help = "Interval on which to try to update resource consumption stats.")
  private static final Arg<Amount<Long, Time>> TASK_STAT_INTERVAL =
      Arg.create(Amount.of(1L, Time.HOURS));

  @CmdLine(name = "async_slot_stat_update_interval",
      help = "Interval on which to try to update open slot stats.")
  private static final Arg<Amount<Long, Time>> SLOT_STAT_INTERVAL =
      Arg.create(Amount.of(1L, Time.MINUTES));

  @Override
  protected void configure() {
    bind(TaskStatCalculator.class).in(Singleton.class);
    bind(CachedCounters.class).in(Singleton.class);
    bind(MachineResourceProvider.class).to(OfferAdapter.class);
    bind(SlotSizeCounter.class).in(Singleton.class);

    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(TaskStatUpdaterService.class).in(Singleton.class);
        bind(Scheduler.class).toInstance(
            Scheduler.newFixedRateSchedule(
                TASK_STAT_INTERVAL.get().getValue(),
                TASK_STAT_INTERVAL.get().getValue(),
                TASK_STAT_INTERVAL.get().getUnit().getTimeUnit()));
        expose(TaskStatUpdaterService.class);
      }
    });
    SchedulerServicesModule.addSchedulerActiveServiceBinding(binder())
        .to(TaskStatUpdaterService.class);

    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(SlotSizeCounterService.class).in(Singleton.class);
        bind(Scheduler.class).toInstance(
            Scheduler.newFixedRateSchedule(
                SLOT_STAT_INTERVAL.get().getValue(),
                SLOT_STAT_INTERVAL.get().getValue(),
                SLOT_STAT_INTERVAL.get().getUnit().getTimeUnit()));
        expose(SlotSizeCounterService.class);
      }
    });
    SchedulerServicesModule.addSchedulerActiveServiceBinding(binder())
        .to(SlotSizeCounterService.class);
  }

  static class TaskStatUpdaterService extends AbstractScheduledService {
    private final TaskStatCalculator taskStats;
    private final Scheduler schedule;

    @Inject
    TaskStatUpdaterService(TaskStatCalculator taskStats, Scheduler schedule) {
      this.taskStats = requireNonNull(taskStats);
      this.schedule = requireNonNull(schedule);
    }

    @Override
    protected void runOneIteration() {
      taskStats.run();
    }

    @Override
    protected Scheduler scheduler() {
      return schedule;
    }
  }

  static class SlotSizeCounterService extends AbstractScheduledService {
    private final SlotSizeCounter slotSizeCounter;
    private final Scheduler schedule;

    @Inject
    SlotSizeCounterService(SlotSizeCounter slotSizeCounter, Scheduler schedule) {
      this.slotSizeCounter = requireNonNull(slotSizeCounter);
      this.schedule = requireNonNull(schedule);
    }

    @Override
    protected void runOneIteration() {
      slotSizeCounter.run();
    }

    @Override
    protected Scheduler scheduler() {
      return schedule;
    }
  }

  static class OfferAdapter implements MachineResourceProvider {
    private final OfferManager offerManager;

    @Inject
    OfferAdapter(OfferManager offerManager) {
      this.offerManager = requireNonNull(offerManager);
    }

    @Override
    public Iterable<MachineResource> get() {
      Iterable<HostOffer> offers = offerManager.getOffers();

      ImmutableList.Builder<MachineResource> builder = ImmutableList.builder();
      for (HostOffer offer : offers) {
        ResourceSlot revocable = Resources.from(offer.getOffer()).filter(REVOCABLE).slot();
        ResourceSlot nonRevocable =
            Resources.from(offer.getOffer()).filter(NON_REVOCABLE).slot();
        boolean isDedicated = Conversions.isDedicated(offer.getOffer());

        // It's insufficient to compare revocable against NONE here as RAM, DISK and PORTS
        // are always rolled in to revocable as non-compressible resources. Only if revocable
        // CPU is non-zero should we expose the revocable resources as aggregates.
        if (revocable.getNumCpus() > 0.0) {
          builder.add(new MachineResource(fromSlot(revocable), isDedicated, true));
        }

        if (!nonRevocable.equals(NONE)) {
          builder.add(new MachineResource(fromSlot(nonRevocable), isDedicated, false));
        }
      }
      return builder.build();
    }

    private static IResourceAggregate fromSlot(ResourceSlot slot) {
      return IResourceAggregate.build(new ResourceAggregate()
          .setNumCpus(slot.getNumCpus())
          .setRamMb(slot.getRam().as(Data.MB))
          .setDiskMb(slot.getDisk().as(Data.MB)));
    }
  }
}

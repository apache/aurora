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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.AbstractScheduledService.Scheduler;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.base.Conversions;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.stats.SlotSizeCounter.MachineResource;
import org.apache.aurora.scheduler.stats.SlotSizeCounter.MachineResourceProvider;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.resources.ResourceBag.IS_MESOS_REVOCABLE;
import static org.apache.aurora.scheduler.resources.ResourceBag.IS_POSITIVE;
import static org.apache.aurora.scheduler.resources.ResourceManager.bagFromMesosResources;
import static org.apache.aurora.scheduler.resources.ResourceManager.getNonRevocableOfferResources;
import static org.apache.aurora.scheduler.resources.ResourceManager.getRevocableOfferResources;

/**
 * Module to configure export of cluster-wide resource allocation and consumption statistics.
 */
public class AsyncStatsModule extends AbstractModule {

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-async_task_stat_update_interval",
        description = "Interval on which to try to update resource consumption stats.")
    public TimeAmount taskStatInterval = new TimeAmount(1, Time.HOURS);

    @Parameter(names = "-async_slot_stat_update_interval",
        description = "Interval on which to try to update open slot stats.")
    public TimeAmount slotStatInterval = new TimeAmount(1, Time.MINUTES);
  }

  private final Options options;

  public AsyncStatsModule(Options options) {
    this.options = options;
  }

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
        Amount<Long, Time> taskStatInterval = options.taskStatInterval;
        bind(Scheduler.class).toInstance(
            Scheduler.newFixedRateSchedule(
                taskStatInterval.getValue(),
                taskStatInterval.getValue(),
                taskStatInterval.getUnit().getTimeUnit()));
        expose(TaskStatUpdaterService.class);
      }
    });
    SchedulerServicesModule.addSchedulerActiveServiceBinding(binder())
        .to(TaskStatUpdaterService.class);

    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(SlotSizeCounterService.class).in(Singleton.class);
        Amount<Long, Time> slotStatInterval = options.slotStatInterval;
        bind(Scheduler.class).toInstance(
            Scheduler.newFixedRateSchedule(
                slotStatInterval.getValue(),
                slotStatInterval.getValue(),
                slotStatInterval.getUnit().getTimeUnit()));
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
      Iterable<HostOffer> offers = offerManager.getAll();

      ImmutableList.Builder<MachineResource> builder = ImmutableList.builder();
      for (HostOffer offer : offers) {
        ResourceBag revocable = bagFromMesosResources(getRevocableOfferResources(offer.getOffer()));
        ResourceBag nonRevocable =
            bagFromMesosResources(getNonRevocableOfferResources(offer.getOffer()));
        boolean isDedicated = Conversions.isDedicated(offer.getOffer());

        // It's insufficient to compare revocable against EMPTY here as RAM, DISK and PORTS
        // are always rolled in to revocable as non-compressible resources. Only if revocable
        // CPU is non-zero should we expose the revocable resources as aggregates.
        if (!revocable.filter(IS_POSITIVE.and(IS_MESOS_REVOCABLE)).getResourceVectors().isEmpty()) {
          builder.add(new MachineResource(revocable, isDedicated, true));
        }

        if (!nonRevocable.equals(ResourceBag.EMPTY)) {
          builder.add(new MachineResource(nonRevocable, isDedicated, false));
        }
      }
      return builder.build();
    }
  }
}

/**
 * Copyright 2013 Apache Software Foundation
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
package org.apache.aurora.scheduler.stats;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.BindingAnnotation;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.common.quantity.Time;

import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.scheduler.async.OfferQueue;
import org.apache.aurora.scheduler.base.Conversions;
import org.apache.aurora.scheduler.configuration.Resources;
import org.apache.aurora.scheduler.stats.SlotSizeCounter.MachineResource;
import org.apache.aurora.scheduler.stats.SlotSizeCounter.MachineResourceProvider;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.mesos.Protos.Offer;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import static com.google.common.base.Preconditions.checkNotNull;

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

  @BindingAnnotation
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  private @interface StatExecutor { }

  @Override
  protected void configure() {
    final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("AsyncStat-%d").setDaemon(true).build());

    bind(TaskStatCalculator.class).in(Singleton.class);
    bind(CachedCounters.class).in(Singleton.class);
    bind(MachineResourceProvider.class).to(OfferAdapter.class);
    bind(SlotSizeCounter.class).in(Singleton.class);

    bind(ScheduledExecutorService.class).annotatedWith(StatExecutor.class).toInstance(executor);
    LifecycleModule.bindStartupAction(binder(), StatUpdater.class);
  }

  static class StatUpdater implements Command {
    private final ScheduledExecutorService executor;
    private final TaskStatCalculator taskStats;
    private final SlotSizeCounter slotCounter;

    @Inject
    StatUpdater(
        @StatExecutor ScheduledExecutorService executor,
        TaskStatCalculator taskStats,
        SlotSizeCounter slotCounter) {

      this.executor = checkNotNull(executor);
      this.taskStats = checkNotNull(taskStats);
      this.slotCounter = checkNotNull(slotCounter);
    }

    @Override
    public void execute() {
      long taskInterval = TASK_STAT_INTERVAL.get().as(Time.SECONDS);
      executor.scheduleAtFixedRate(taskStats, taskInterval, taskInterval, TimeUnit.SECONDS);
      long slotInterval = SLOT_STAT_INTERVAL.get().as(Time.SECONDS);
      executor.scheduleAtFixedRate(slotCounter, slotInterval, slotInterval, TimeUnit.SECONDS);
    }
  }

  static class OfferAdapter implements MachineResourceProvider {
    private static final Function<Offer, MachineResource> TO_RESOURCE =
        new Function<Offer, MachineResource>() {
          @Override
          public MachineResource apply(Offer offer) {
            Resources resources = Resources.from(offer);
            IResourceAggregate quota = IResourceAggregate.build(new ResourceAggregate()
                .setNumCpus(resources.getNumCpus())
                .setRamMb(resources.getRam().as(Data.MB))
                .setDiskMb(resources.getDisk().as(Data.MB)));
            return new MachineResource(quota, Conversions.isDedicated(offer));
          }
        };

    private final OfferQueue offerQueue;

    @Inject
    OfferAdapter(OfferQueue offerQueue) {
      this.offerQueue = checkNotNull(offerQueue);
    }

    @Override
    public Iterable<MachineResource> get() {
      Iterable<Offer> offers = offerQueue.getOffers();
      return FluentIterable.from(offers).transform(TO_RESOURCE);
    }
  }
}

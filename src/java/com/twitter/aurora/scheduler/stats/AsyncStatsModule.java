/*
 * Copyright 2013 Twitter, Inc.
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
package com.twitter.aurora.scheduler.stats;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;

import com.twitter.aurora.scheduler.stats.ResourceCounter.GlobalMetric;
import com.twitter.aurora.scheduler.stats.ResourceCounter.Metric;
import com.twitter.aurora.scheduler.storage.Storage.StorageException;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Module to configure export of cluster-wide resource allocation and consumption statistics.
 */
public class AsyncStatsModule extends AbstractModule {

  @CmdLine(name = "async_stat_update_interval",
      help = "Interval on which to try to update async resource consumption stats.")
  private static final Arg<Amount<Long, Time>> ASYNC_STAT_INTERVAL =
      Arg.create(Amount.of(1L, Time.HOURS));

  @BindingAnnotation
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  private @interface StatExecutor { }

  @Override
  protected void configure() {
    final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("AsyncStat-%d").setDaemon(true).build());
    bind(ScheduledExecutorService.class).annotatedWith(StatExecutor.class).toInstance(executor);
    LifecycleModule.bindStartupAction(binder(), StatUpdater.class);
  }

  static class StatUpdater implements Command {
    private static final Logger LOG = Logger.getLogger(StatUpdater.class.getName());

    private final ScheduledExecutorService executor;
    private final ResourceCounter counter;
    private final LoadingCache<String, AtomicLong> stats = CacheBuilder.newBuilder().build(
        new CacheLoader<String, AtomicLong>() {
          @Override public AtomicLong load(String key) {
            return Stats.exportLong(key);
          }
        }
    );

    @Inject
    StatUpdater(@StatExecutor ScheduledExecutorService executor, ResourceCounter counter) {
      this.executor = Preconditions.checkNotNull(executor);
      this.counter = Preconditions.checkNotNull(counter);
    }

    @Override
    public void execute() {
      Runnable compute = new Runnable() {
        private void update(String prefix, Metric metric) {
          stats.getUnchecked(prefix + "_cpu").set(metric.getCpu());
          stats.getUnchecked(prefix + "_ram_gb").set(metric.getRamGb());
          stats.getUnchecked(prefix + "_disk_gb").set(metric.getDiskGb());
        }

        @Override public void run() {
          try {
            for (GlobalMetric metric : counter.computeConsumptionTotals()) {
              update("resources_" + metric.type.name().toLowerCase(), metric);
            }
            update("resources_allocated_quota", counter.computeQuotaAllocationTotals());
          } catch (StorageException e) {
            LOG.fine("Unable to fetch metrics, storage is likely not ready.");
          }
        }
      };

      long intervalSecs = ASYNC_STAT_INTERVAL.get().as(Time.SECONDS);
      executor.scheduleAtFixedRate(compute, intervalSecs, intervalSecs, TimeUnit.SECONDS);
    }
  }
}

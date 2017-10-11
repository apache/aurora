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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Supplier;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

import org.apache.aurora.common.application.ShutdownRegistry;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.Stat;
import org.apache.aurora.common.stats.StatRegistry;
import org.apache.aurora.common.stats.Stats;
import org.apache.aurora.common.stats.TimeSeriesRepository;
import org.apache.aurora.common.stats.TimeSeriesRepositoryImpl;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.config.types.TimeAmount;

/**
 * Binding module for injections related to the in-process stats system.
 */
public class StatsModule extends AbstractModule {

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-stat_sampling_interval",
        description = "Statistic value sampling interval.")
    public TimeAmount samplingInterval = new TimeAmount(1, Time.SECONDS);

    @Parameter(names = "-stat_retention_period",
        description = "Time for a stat to be retained in memory before expiring.")
    public TimeAmount retentionPeriod = new TimeAmount(1, Time.HOURS);
  }

  private final Options options;

  public StatsModule(Options options) {
    this.options = options;
  }

  @Override
  protected void configure() {
    requireBinding(ShutdownRegistry.class);

    // Bindings for TimeSeriesRepositoryImpl.
    bind(StatRegistry.class).toInstance(Stats.STAT_REGISTRY);
    bind(new TypeLiteral<Amount<Long, Time>>() { })
        .annotatedWith(Names.named(TimeSeriesRepositoryImpl.SAMPLE_RETENTION_PERIOD))
        .toInstance(options.retentionPeriod);
    bind(new TypeLiteral<Amount<Long, Time>>() { })
        .annotatedWith(Names.named(TimeSeriesRepositoryImpl.SAMPLE_PERIOD))
        .toInstance(options.samplingInterval);
    bind(TimeSeriesRepository.class).to(TimeSeriesRepositoryImpl.class);
    bind(TimeSeriesRepositoryImpl.class).in(Singleton.class);

    bind(new TypeLiteral<Supplier<Iterable<Stat<?>>>>() { }).toInstance(
        Stats::getVariables
    );

    SchedulerServicesModule.addAppStartupServiceBinding(binder())
        .to(TimeSeriesRepositoryImpl.class);
  }
}

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

import com.google.common.base.Supplier;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

import org.apache.aurora.common.application.ShutdownRegistry;
import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.Stat;
import org.apache.aurora.common.stats.StatRegistry;
import org.apache.aurora.common.stats.Stats;
import org.apache.aurora.common.stats.TimeSeriesRepository;
import org.apache.aurora.common.stats.TimeSeriesRepositoryImpl;
import org.apache.aurora.scheduler.SchedulerServicesModule;

/**
 * Binding module for injections related to the in-process stats system.
 */
public class StatsModule extends AbstractModule {

  @CmdLine(name = "stat_sampling_interval", help = "Statistic value sampling interval.")
  private static final Arg<Amount<Long, Time>> SAMPLING_INTERVAL =
      Arg.create(Amount.of(1L, Time.SECONDS));

  @CmdLine(name = "stat_retention_period",
      help = "Time for a stat to be retained in memory before expiring.")
  private static final Arg<Amount<Long, Time>> RETENTION_PERIOD =
      Arg.create(Amount.of(1L, Time.HOURS));

  @Override
  protected void configure() {
    requireBinding(ShutdownRegistry.class);

    // Bindings for TimeSeriesRepositoryImpl.
    bind(StatRegistry.class).toInstance(Stats.STAT_REGISTRY);
    bind(new TypeLiteral<Amount<Long, Time>>() { })
        .annotatedWith(Names.named(TimeSeriesRepositoryImpl.SAMPLE_RETENTION_PERIOD))
        .toInstance(RETENTION_PERIOD.get());
    bind(new TypeLiteral<Amount<Long, Time>>() { })
        .annotatedWith(Names.named(TimeSeriesRepositoryImpl.SAMPLE_PERIOD))
        .toInstance(SAMPLING_INTERVAL.get());
    bind(TimeSeriesRepository.class).to(TimeSeriesRepositoryImpl.class);
    bind(TimeSeriesRepositoryImpl.class).in(Singleton.class);

    bind(new TypeLiteral<Supplier<Iterable<Stat<?>>>>() { }).toInstance(
        Stats::getVariables
    );

    SchedulerServicesModule.addAppStartupServiceBinding(binder())
        .to(TimeSeriesRepositoryImpl.class);
  }
}

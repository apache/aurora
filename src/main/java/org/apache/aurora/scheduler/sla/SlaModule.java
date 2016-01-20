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
package org.apache.aurora.scheduler.sla;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Qualifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.common.args.constraints.Positive;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.sla.MetricCalculator.MetricCalculatorSettings;
import org.apache.aurora.scheduler.sla.MetricCalculator.MetricCategory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.sla.MetricCalculator.MetricCategory.JOB_UPTIMES;
import static org.apache.aurora.scheduler.sla.MetricCalculator.MetricCategory.MEDIANS;
import static org.apache.aurora.scheduler.sla.MetricCalculator.MetricCategory.PLATFORM_UPTIME;

/**
 * Binding module for the sla processor.
 */
public class SlaModule extends AbstractModule {

  private static final Logger LOG = LoggerFactory.getLogger(SlaModule.class);

  @Positive
  @CmdLine(name = "sla_stat_refresh_interval", help = "The SLA stat refresh interval.")
  private static final Arg<Amount<Long, Time>> SLA_REFRESH_INTERVAL =
      Arg.create(Amount.of(1L, Time.MINUTES));

  @CmdLine(name = "sla_prod_metrics",
      help = "Metric categories collected for production tasks.")
  private static final Arg<Set<MetricCategory>> SLA_PROD_METRICS =
      Arg.<Set<MetricCategory>>create(ImmutableSet.of(JOB_UPTIMES, PLATFORM_UPTIME, MEDIANS));

  @CmdLine(name = "sla_non_prod_metrics",
      help = "Metric categories collected for non production tasks.")
  private static final Arg<Set<MetricCategory>> SLA_NON_PROD_METRICS =
      Arg.<Set<MetricCategory>>create(ImmutableSet.of());

  @VisibleForTesting
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  @interface SlaExecutor { }

  private final Amount<Long, Time> refreshInterval;
  private final Set<MetricCategory> prodMetrics;
  private final Set<MetricCategory> nonProdMetrics;

  @VisibleForTesting
  SlaModule(
      Amount<Long, Time> refreshInterval,
      Set<MetricCategory> prodMetrics,
      Set<MetricCategory> nonProdMetrics) {

    this.refreshInterval = refreshInterval;
    this.prodMetrics = prodMetrics;
    this.nonProdMetrics = nonProdMetrics;
  }

  public SlaModule() {
    this(SLA_REFRESH_INTERVAL.get(), SLA_PROD_METRICS.get(), SLA_NON_PROD_METRICS.get());
  }

  @Override
  protected void configure() {
    bind(MetricCalculatorSettings.class)
        .toInstance(new MetricCalculatorSettings(
            refreshInterval.as(Time.MILLISECONDS),
            prodMetrics,
            nonProdMetrics));

    bind(MetricCalculator.class).in(Singleton.class);
    bind(ScheduledExecutorService.class)
        .annotatedWith(SlaExecutor.class)
        .toInstance(AsyncUtil.singleThreadLoggingScheduledExecutor("SlaStat-%d", LOG));

    bind(SlaUpdater.class).in(Singleton.class);
    SchedulerServicesModule.addSchedulerActiveServiceBinding(binder()).to(SlaUpdater.class);
  }

  // TODO(ksweeney): This should use AbstractScheduledService.
  static class SlaUpdater extends AbstractIdleService {
    private final ScheduledExecutorService executor;
    private final MetricCalculator calculator;
    private final MetricCalculatorSettings settings;

    @Inject
    SlaUpdater(
        @SlaExecutor ScheduledExecutorService executor,
        MetricCalculator calculator,
        MetricCalculatorSettings settings) {

      this.executor = requireNonNull(executor);
      this.calculator = requireNonNull(calculator);
      this.settings = requireNonNull(settings);
    }

    @Override
    protected void startUp() {
      long interval = settings.getRefreshRateMs();
      executor.scheduleAtFixedRate(calculator, interval, interval, TimeUnit.MILLISECONDS);
      LOG.debug("Scheduled SLA calculation with {} msec interval.", interval);
    }

    @Override
    protected void shutDown() {
      // Ignored. VM shutdown is required to stop computing SLAs.
    }
  }
}

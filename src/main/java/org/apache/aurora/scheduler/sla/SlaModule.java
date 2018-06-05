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
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Qualifier;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.config.splitters.CommaSplitter;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.config.validators.PositiveAmount;
import org.apache.aurora.scheduler.sla.MetricCalculator.MetricCalculatorSettings;
import org.apache.aurora.scheduler.sla.MetricCalculator.MetricCategory;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.channel.DefaultKeepAliveStrategy;
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
import static org.asynchttpclient.Dsl.asyncHttpClient;

/**
 * Binding module for the sla processor.
 */
public class SlaModule extends AbstractModule {

  private static final Logger LOG = LoggerFactory.getLogger(SlaModule.class);

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-sla_stat_refresh_interval",
        validateValueWith = PositiveAmount.class,
        description = "The SLA stat refresh interval.")
    public TimeAmount slaRefreshInterval = new TimeAmount(1, Time.MINUTES);

    @Parameter(names = "-sla_prod_metrics",
        description = "Metric categories collected for production tasks.",
        splitter = CommaSplitter.class)
    public List<MetricCategory> slaProdMetrics =
        ImmutableList.of(JOB_UPTIMES, PLATFORM_UPTIME, MEDIANS);

    @Parameter(names = "-sla_non_prod_metrics",
        description = "Metric categories collected for non production tasks.",
        splitter = CommaSplitter.class)
    public List<MetricCategory> slaNonProdMetrics = ImmutableList.of();

    @Parameter(names = "-sla_coordinator_timeout",
        validateValueWith = PositiveAmount.class,
        description = "Timeout interval for communicating with Coordinator.")
    public TimeAmount slaCoordinatorTimeout = new TimeAmount(1, Time.MINUTES);

    @Parameter(names = "-max_parallel_coordinated_maintenance",
        description = "Maximum number of coordinators that can be contacted in parallel.")
    public Integer maxParallelCoordinators = 10;

    @Parameter(names = "-min_required_instances_for_sla_check",
        description = "Minimum number of instances required for a job to be eligible for SLA "
            + "check. This does not apply to jobs that have a CoordinatorSlaPolicy.")
    public Integer minRequiredInstances = 20;

    @Parameter(names = "-max_sla_duration_secs",
        validateValueWith = PositiveAmount.class,
        description = "Maximum duration window for which SLA requirements are to be satisfied."
            + "This does not apply to jobs that have a CoordinatorSlaPolicy."
    )
    public TimeAmount maxSlaDuration = new TimeAmount(2, Time.HOURS);
  }

  @VisibleForTesting
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  @interface SlaExecutor { }

  private final Options options;

  public SlaModule(Options options) {
    this.options = options;
  }

  @Override
  protected void configure() {
    bind(MetricCalculatorSettings.class)
        .toInstance(new MetricCalculatorSettings(
            options.slaRefreshInterval.as(Time.MILLISECONDS),
            ImmutableSet.copyOf(options.slaProdMetrics),
            ImmutableSet.copyOf(options.slaNonProdMetrics)));

    bind(MetricCalculator.class).in(Singleton.class);
    bind(ScheduledExecutorService.class)
        .annotatedWith(SlaExecutor.class)
        .toInstance(AsyncUtil.singleThreadLoggingScheduledExecutor("SlaStat-%d", LOG));

    bind(SlaUpdater.class).in(Singleton.class);
    SchedulerServicesModule.addSchedulerActiveServiceBinding(binder()).to(SlaUpdater.class);

    DefaultAsyncHttpClientConfig config = new DefaultAsyncHttpClientConfig.Builder()
        .setConnectTimeout(options.slaCoordinatorTimeout.as(Time.MILLISECONDS).intValue())
        .setHandshakeTimeout(options.slaCoordinatorTimeout.as(Time.MILLISECONDS).intValue())
        .setSslSessionTimeout(options.slaCoordinatorTimeout.as(Time.MILLISECONDS).intValue())
        .setReadTimeout(options.slaCoordinatorTimeout.as(Time.MILLISECONDS).intValue())
        .setRequestTimeout(options.slaCoordinatorTimeout.as(Time.MILLISECONDS).intValue())
        .setKeepAliveStrategy(new DefaultKeepAliveStrategy())
        .build();
    AsyncHttpClient httpClient = asyncHttpClient(config);

    bind(AsyncHttpClient.class)
        .annotatedWith(SlaManager.HttpClient.class)
        .toInstance(httpClient);

    bind(new TypeLiteral<Integer>() { })
        .annotatedWith(SlaManager.MinRequiredInstances.class)
        .toInstance(options.minRequiredInstances);

    bind(new TypeLiteral<Integer>() { })
        .annotatedWith(SlaManager.MaxParallelCoordinators.class)
        .toInstance(options.maxParallelCoordinators);

    bind(ScheduledExecutorService.class)
        .annotatedWith(SlaManager.SlaManagerExecutor.class)
        .toInstance(AsyncUtil.loggingScheduledExecutor(
            options.maxParallelCoordinators,
            "SlaManager-%d", LOG));

    bind(SlaManager.class).in(javax.inject.Singleton.class);
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

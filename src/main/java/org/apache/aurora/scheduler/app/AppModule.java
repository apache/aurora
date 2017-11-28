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
package org.apache.aurora.scheduler.app;

import java.util.List;

import javax.inject.Singleton;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;

import org.apache.aurora.GuiceUtils;
import org.apache.aurora.common.inject.TimedInterceptor;
import org.apache.aurora.common.stats.Stats;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.Container._Fields;
import org.apache.aurora.gen.DockerParameter;
import org.apache.aurora.scheduler.SchedulerModule;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.app.SchedulerMain.Options.DriverKind;
import org.apache.aurora.scheduler.async.AsyncModule;
import org.apache.aurora.scheduler.config.CliOptions;
import org.apache.aurora.scheduler.config.validators.PositiveNumber;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.ConfigurationManagerSettings;
import org.apache.aurora.scheduler.events.NotifyingSchedulingFilter;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.filter.SchedulingFilterImpl;
import org.apache.aurora.scheduler.http.JettyServerModule;
import org.apache.aurora.scheduler.mesos.SchedulerDriverModule;
import org.apache.aurora.scheduler.metadata.MetadataModule;
import org.apache.aurora.scheduler.offers.OfferManagerModule;
import org.apache.aurora.scheduler.preemptor.PreemptorModule;
import org.apache.aurora.scheduler.pruning.PruningModule;
import org.apache.aurora.scheduler.quota.QuotaModule;
import org.apache.aurora.scheduler.reconciliation.ReconciliationModule;
import org.apache.aurora.scheduler.scheduling.SchedulingModule;
import org.apache.aurora.scheduler.sla.SlaModule;
import org.apache.aurora.scheduler.state.StateModule;
import org.apache.aurora.scheduler.stats.AsyncStatsModule;
import org.apache.aurora.scheduler.thrift.Thresholds;
import org.apache.aurora.scheduler.updater.UpdaterModule;
import org.apache.mesos.Scheduler;

import static java.util.Objects.requireNonNull;

/**
 * Binding module for the aurora scheduler application.
 */
public class AppModule extends AbstractModule {

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-max_tasks_per_job",
        validateValueWith = PositiveNumber.class,
        description = "Maximum number of allowed tasks in a single job.")
    public int maxTasksPerJob = 4000;

    @Parameter(names = "-max_update_instance_failures",
        validateValueWith = PositiveNumber.class,
        description = "Upper limit on the number of "
            + "failures allowed during a job update. This helps cap potentially unbounded entries"
            + " into storage.")
    public int maxUpdateInstanceFailures = maxTasksPerJob * 5;

    // TODO(wfarner): From jcommander docs - "Also, note that only List<String> is allowed for
    // parameters that define an arity. You will have to convert these values yourself..."

    @Parameter(names = "-allowed_container_types",
        description = "Container types that are allowed to be used by jobs.")
    public List<_Fields> allowedContainerTypes = ImmutableList.of(Container._Fields.MESOS);

    @Parameter(names = "-allow_docker_parameters",
        description = "Allow to pass docker container parameters in the job.",
        arity = 1)
    public boolean enableDockerParameters = false;

    @Parameter(names = "-default_docker_parameters",
        description =
            "Default docker parameters for any job that does not explicitly declare parameters.")
    public List<DockerParameter> defaultDockerParameters = ImmutableList.of();

    @Parameter(names = "-require_docker_use_executor",
        description = "If false, Docker tasks may run without an executor (EXPERIMENTAL)",
        arity = 1)
    public boolean requireDockerUseExecutor = true;

    @Parameter(names = "-enable_mesos_fetcher", description = "Allow jobs to pass URIs "
        + "to the Mesos Fetcher. Note that enabling this feature could pose "
        + "a privilege escalation threat.",
        arity = 1)
    public boolean enableMesosFetcher = false;

    @Parameter(names = "-allow_container_volumes",
        description = "Allow passing in volumes in the job. Enabling this could pose a privilege "
            + "escalation threat.",
        arity = 1)
    public boolean allowContainerVolumes = false;

    @Parameter(names = "-allowed_job_environments", description = "Regular expression describing "
            + "the environments that are allowed to be used by jobs.")
    public String allowedJobEnvironments = ConfigurationManager.DEFAULT_ALLOWED_JOB_ENVIRONMENTS;
  }

  private final ConfigurationManagerSettings configurationManagerSettings;
  private final DriverKind kind;
  private final CliOptions options;

  @VisibleForTesting
  public AppModule(
      ConfigurationManagerSettings configurationManagerSettings,
      DriverKind kind,
      CliOptions options) {
    this.configurationManagerSettings = requireNonNull(configurationManagerSettings);
    this.kind = kind;
    this.options = options;
  }

  public AppModule(CliOptions opts) {
    this(new ConfigurationManagerSettings(
        ImmutableSet.copyOf(opts.app.allowedContainerTypes),
            opts.app.enableDockerParameters,
            opts.app.defaultDockerParameters,
            opts.app.requireDockerUseExecutor,
            opts.main.allowGpuResource,
            opts.app.enableMesosFetcher,
            opts.app.allowContainerVolumes,
            opts.app.allowedJobEnvironments),
        opts.main.driverImpl,
        opts);
  }

  @Override
  protected void configure() {
    bind(ConfigurationManagerSettings.class).toInstance(configurationManagerSettings);
    bind(Thresholds.class)
        .toInstance(
            new Thresholds(options.app.maxTasksPerJob,
            options.app.maxUpdateInstanceFailures));

    // Enable intercepted method timings and context classloader repair.
    TimedInterceptor.bind(binder());
    GuiceUtils.bindJNIContextClassLoader(binder(), Scheduler.class);
    GuiceUtils.bindExceptionTrap(binder(), Scheduler.class);

    bind(Clock.class).toInstance(Clock.SYSTEM_CLOCK);
    // Filter layering: notifier filter -> base impl
    bind(SchedulingFilter.class).to(NotifyingSchedulingFilter.class);
    bind(NotifyingSchedulingFilter.class).in(Singleton.class);
    bind(SchedulingFilter.class)
        .annotatedWith(NotifyingSchedulingFilter.NotifyDelegate.class)
        .to(SchedulingFilterImpl.class);
    bind(SchedulingFilterImpl.class).in(Singleton.class);

    install(new PubsubEventModule());
    install(new AsyncModule(options.async));
    install(new OfferManagerModule(options));
    install(new PruningModule(options.pruning));
    install(new ReconciliationModule(options.reconciliation));
    install(new SchedulingModule(options.scheduling));
    install(new AsyncStatsModule(options.asyncStats));
    install(new MetadataModule());
    install(new QuotaModule());
    install(new JettyServerModule(options));
    install(new PreemptorModule(options));
    install(new SchedulerDriverModule(kind));
    install(new SchedulerServicesModule());
    install(new SchedulerModule(options.scheduler));
    install(new StateModule(options));
    install(new SlaModule(options.sla));
    install(new UpdaterModule(options.updater));
    bind(StatsProvider.class).toInstance(Stats.STATS_PROVIDER);
  }
}

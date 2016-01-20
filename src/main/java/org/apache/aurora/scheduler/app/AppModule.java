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

import java.util.Set;

import javax.inject.Singleton;

import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;

import org.apache.aurora.GuiceUtils;
import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.common.args.constraints.Positive;
import org.apache.aurora.common.inject.TimedInterceptor;
import org.apache.aurora.common.stats.Stats;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.Container._Fields;
import org.apache.aurora.scheduler.SchedulerModule;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.async.AsyncModule;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.apache.aurora.scheduler.filter.SchedulingFilterImpl;
import org.apache.aurora.scheduler.http.JettyServerModule;
import org.apache.aurora.scheduler.mesos.SchedulerDriverModule;
import org.apache.aurora.scheduler.metadata.MetadataModule;
import org.apache.aurora.scheduler.offers.OffersModule;
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

/**
 * Binding module for the aurora scheduler application.
 */
public class AppModule extends AbstractModule {
  private static final int DEFAULT_MAX_TASKS_PER_JOB = 4000;

  @Positive
  @CmdLine(name = "max_tasks_per_job", help = "Maximum number of allowed tasks in a single job.")
  public static final Arg<Integer> MAX_TASKS_PER_JOB = Arg.create(DEFAULT_MAX_TASKS_PER_JOB);

  private static final int DEFAULT_MAX_UPDATE_INSTANCE_FAILURES =
      DEFAULT_MAX_TASKS_PER_JOB * 5;

  @Positive
  @CmdLine(name = "max_update_instance_failures", help = "Upper limit on the number of "
      + "failures allowed during a job update. This helps cap potentially unbounded entries into "
      + "storage.")
  public static final Arg<Integer> MAX_UPDATE_INSTANCE_FAILURES = Arg.create(
      DEFAULT_MAX_UPDATE_INSTANCE_FAILURES);

  @CmdLine(name = "allowed_container_types",
      help = "Container types that are allowed to be used by jobs.")
  private static final Arg<Set<_Fields>> ALLOWED_CONTAINER_TYPES =
      Arg.create(ImmutableSet.of(Container._Fields.MESOS));

  @CmdLine(name = "allow_docker_parameters",
      help = "Allow to pass docker container parameters in the job.")
  private static final Arg<Boolean> ENABLE_DOCKER_PARAMETERS = Arg.create(false);

  @Override
  protected void configure() {
    bind(ConfigurationManager.class).toInstance(
        new ConfigurationManager(
            ImmutableSet.copyOf(ALLOWED_CONTAINER_TYPES.get()),
            ENABLE_DOCKER_PARAMETERS.get()));
    bind(Thresholds.class)
        .toInstance(new Thresholds(MAX_TASKS_PER_JOB.get(), MAX_UPDATE_INSTANCE_FAILURES.get()));

    // Enable intercepted method timings and context classloader repair.
    TimedInterceptor.bind(binder());
    GuiceUtils.bindJNIContextClassLoader(binder(), Scheduler.class);
    GuiceUtils.bindExceptionTrap(binder(), Scheduler.class);

    bind(Clock.class).toInstance(Clock.SYSTEM_CLOCK);
    install(new PubsubEventModule());
    // Filter layering: notifier filter -> base impl
    PubsubEventModule.bindSchedulingFilterDelegate(binder()).to(SchedulingFilterImpl.class);
    bind(SchedulingFilterImpl.class).in(Singleton.class);

    install(new AsyncModule());
    install(new OffersModule());
    install(new PruningModule());
    install(new ReconciliationModule());
    install(new SchedulingModule());
    install(new AsyncStatsModule());
    install(new MetadataModule());
    install(new QuotaModule());
    install(new JettyServerModule());
    install(new PreemptorModule());
    install(new SchedulerDriverModule());
    install(new SchedulerServicesModule());
    install(new SchedulerModule());
    install(new StateModule());
    install(new SlaModule());
    install(new UpdaterModule());
    bind(StatsProvider.class).toInstance(Stats.STATS_PROVIDER);
  }
}

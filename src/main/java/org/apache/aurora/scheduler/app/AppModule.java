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

import javax.inject.Singleton;

import com.google.inject.AbstractModule;

import org.apache.aurora.GuiceUtils;
import org.apache.aurora.common.inject.TimedInterceptor;
import org.apache.aurora.common.stats.Stats;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.scheduler.SchedulerModule;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.async.AsyncModule;
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
import org.apache.aurora.scheduler.updater.UpdaterModule;
import org.apache.mesos.Scheduler;

/**
 * Binding module for the aurora scheduler application.
 */
public class AppModule extends AbstractModule {
  @Override
  protected void configure() {
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

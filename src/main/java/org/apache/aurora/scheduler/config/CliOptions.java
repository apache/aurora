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

package org.apache.aurora.scheduler.config;

import java.util.List;

import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

import org.apache.aurora.scheduler.SchedulerModule;
import org.apache.aurora.scheduler.TierModule;
import org.apache.aurora.scheduler.app.AppModule;
import org.apache.aurora.scheduler.app.SchedulerMain;
import org.apache.aurora.scheduler.async.AsyncModule;
import org.apache.aurora.scheduler.configuration.executor.ExecutorModule;
import org.apache.aurora.scheduler.cron.quartz.CronModule;
import org.apache.aurora.scheduler.discovery.FlaggedZooKeeperConfig;
import org.apache.aurora.scheduler.events.WebhookModule;
import org.apache.aurora.scheduler.http.JettyServerModule;
import org.apache.aurora.scheduler.http.api.ApiModule;
import org.apache.aurora.scheduler.http.api.security.HttpSecurityModule;
import org.apache.aurora.scheduler.http.api.security.IniShiroRealmModule;
import org.apache.aurora.scheduler.http.api.security.Kerberos5ShiroRealmModule;
import org.apache.aurora.scheduler.log.mesos.MesosLogStreamModule;
import org.apache.aurora.scheduler.mesos.CommandLineDriverSettingsModule;
import org.apache.aurora.scheduler.offers.OfferManagerModule;
import org.apache.aurora.scheduler.preemptor.PreemptorModule;
import org.apache.aurora.scheduler.pruning.PruningModule;
import org.apache.aurora.scheduler.reconciliation.ReconciliationModule;
import org.apache.aurora.scheduler.resources.ResourceSettings;
import org.apache.aurora.scheduler.scheduling.SchedulingModule;
import org.apache.aurora.scheduler.scheduling.TaskAssignerImplModule;
import org.apache.aurora.scheduler.sla.SlaModule;
import org.apache.aurora.scheduler.state.StateModule;
import org.apache.aurora.scheduler.stats.AsyncStatsModule;
import org.apache.aurora.scheduler.stats.StatsModule;
import org.apache.aurora.scheduler.storage.backup.BackupModule;
import org.apache.aurora.scheduler.storage.log.LogPersistenceModule;
import org.apache.aurora.scheduler.storage.log.SnapshotModule;
import org.apache.aurora.scheduler.thrift.aop.AopModule;
import org.apache.aurora.scheduler.updater.UpdaterModule;

public class CliOptions {
  public final ReconciliationModule.Options reconciliation =
      new ReconciliationModule.Options();
  public final OfferManagerModule.Options offer = new OfferManagerModule.Options();
  public final ExecutorModule.Options executor = new ExecutorModule.Options();
  public final AppModule.Options app = new AppModule.Options();
  public final SchedulerMain.Options main = new SchedulerMain.Options();
  public final SchedulingModule.Options scheduling = new SchedulingModule.Options();
  public final AsyncModule.Options async = new AsyncModule.Options();
  public final FlaggedZooKeeperConfig.Options zk = new FlaggedZooKeeperConfig.Options();
  public final UpdaterModule.Options updater = new UpdaterModule.Options();
  public final StateModule.Options state = new StateModule.Options();
  public final LogPersistenceModule.Options logPersistence = new LogPersistenceModule.Options();
  public final SnapshotModule.Options snapshot = new SnapshotModule.Options();
  public final BackupModule.Options backup = new BackupModule.Options();
  public final AopModule.Options aop = new AopModule.Options();
  public final PruningModule.Options pruning = new PruningModule.Options();
  public final CommandLineDriverSettingsModule.Options driver =
      new CommandLineDriverSettingsModule.Options();
  public final JettyServerModule.Options jetty = new JettyServerModule.Options();
  public final HttpSecurityModule.Options httpSecurity = new HttpSecurityModule.Options();
  public final Kerberos5ShiroRealmModule.Options kerberos = new Kerberos5ShiroRealmModule.Options();
  public final IniShiroRealmModule.Options iniShiroRealm = new IniShiroRealmModule.Options();
  public final ApiModule.Options api = new ApiModule.Options();
  public final PreemptorModule.Options preemptor = new PreemptorModule.Options();
  public final MesosLogStreamModule.Options mesosLog = new MesosLogStreamModule.Options();
  public final SlaModule.Options sla = new SlaModule.Options();
  public final WebhookModule.Options webhook = new WebhookModule.Options();
  public final SchedulerModule.Options scheduler = new SchedulerModule.Options();
  public final TierModule.Options tiers = new TierModule.Options();
  public final AsyncStatsModule.Options asyncStats = new AsyncStatsModule.Options();
  public final StatsModule.Options stats = new StatsModule.Options();
  public final CronModule.Options cron = new CronModule.Options();
  public final ResourceSettings resourceSettings = new ResourceSettings();
  public final TaskAssignerImplModule.Options taskAssigner = new TaskAssignerImplModule.Options();
  final List<Object> custom;

  public CliOptions() {
    this(ImmutableList.of());
  }

  public CliOptions(List<Object> custom) {
    this.custom = custom;
  }

  /**
   * Gets a custom options object of a particular type.
   *
   * @param customOptionType Custom option type class.
   * @param <T> Custom option type.
   * @return The matching custom option object.
   */
  @SuppressWarnings("unchecked")
  public <T> T getCustom(Class<T> customOptionType) {
    return (T) FluentIterable.from(custom)
        .firstMatch(Predicates.instanceOf(customOptionType))
        .get();
  }
}

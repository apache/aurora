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
package org.apache.aurora.scheduler.pruning;

import java.util.concurrent.ScheduledExecutorService;

import javax.inject.Singleton;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;

import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Binding module for background storage pruning.
 */
public class PruningModule extends AbstractModule {

  private static final Logger LOG = LoggerFactory.getLogger(PruningModule.class);

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-history_prune_threshold",
        description = "Time after which the scheduler will prune terminated task history.")
    public TimeAmount historyPruneThreshold = new TimeAmount(2, Time.DAYS);

    @Parameter(names = "-history_max_per_job_threshold",
        description = "Maximum number of terminated tasks to retain in a job history.")
    public int historyMaxPerJobThreshold = 100;

    @Parameter(names = "-history_min_retention_threshold",
        description =
            "Minimum guaranteed time for task history retention before any pruning is attempted.")
    public TimeAmount historyMinRetentionThreshold = new TimeAmount(1, Time.HOURS);

    @Parameter(names = "-job_update_history_per_job_threshold",
        description = "Maximum number of completed job updates to retain in a job update history.")
    public int jobUpdateHistoryPerJobThreshold = 10;

    @Parameter(names = "-job_update_history_pruning_interval",
        description = "Job update history pruning interval.")
    public TimeAmount jobUpdateHistoryPruningInterval = new TimeAmount(15, Time.MINUTES);

    @Parameter(names = "-job_update_history_pruning_threshold",
        description = "Time after which the scheduler will prune completed job update history.")
    public TimeAmount jobUpdateHistoryPruningThreshold = new TimeAmount(30, Time.DAYS);
  }

  private final Options options;

  public PruningModule(Options options) {
    this.options = options;
  }

  @Override
  protected void configure() {
    install(new PrivateModule() {
      @Override
      protected void configure() {
        // TODO(ksweeney): Create a configuration validator module so this can be injected.
        // TODO(William Farner): Revert this once large task counts is cheap ala hierarchichal store
        bind(TaskHistoryPruner.HistoryPrunerSettings.class).toInstance(
            new TaskHistoryPruner.HistoryPrunerSettings(
                options.historyPruneThreshold,
                options.historyMinRetentionThreshold,
                options.historyMaxPerJobThreshold));

        bind(TaskHistoryPruner.class).in(Singleton.class);
        expose(TaskHistoryPruner.class);
      }
    });
    PubsubEventModule.bindSubscriber(binder(), TaskHistoryPruner.class);

    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(JobUpdateHistoryPruner.HistoryPrunerSettings.class).toInstance(
            new JobUpdateHistoryPruner.HistoryPrunerSettings(
                options.jobUpdateHistoryPruningInterval,
                options.jobUpdateHistoryPruningThreshold,
                options.jobUpdateHistoryPerJobThreshold));

        bind(ScheduledExecutorService.class).toInstance(
            AsyncUtil.singleThreadLoggingScheduledExecutor("JobUpdatePruner-%d", LOG));

        bind(JobUpdateHistoryPruner.class).in(Singleton.class);
        expose(JobUpdateHistoryPruner.class);
      }
    });
    SchedulerServicesModule.addSchedulerActiveServiceBinding(binder())
        .to(JobUpdateHistoryPruner.class);
  }
}

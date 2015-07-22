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
import java.util.logging.Logger;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.apache.aurora.scheduler.pruning.TaskHistoryPruner.HistoryPrunnerSettings;

/**
 * Binding module for background storage pruning.
 */
public class PruningModule extends AbstractModule {

  private static final Logger LOG = Logger.getLogger(PruningModule.class.getName());

  @CmdLine(name = "history_prune_threshold",
      help = "Time after which the scheduler will prune terminated task history.")
  private static final Arg<Amount<Long, Time>> HISTORY_PRUNE_THRESHOLD =
      Arg.create(Amount.of(2L, Time.DAYS));

  @CmdLine(name = "history_max_per_job_threshold",
      help = "Maximum number of terminated tasks to retain in a job history.")
  private static final Arg<Integer> HISTORY_MAX_PER_JOB_THRESHOLD = Arg.create(100);

  @CmdLine(name = "history_min_retention_threshold",
      help = "Minimum guaranteed time for task history retention before any pruning is attempted.")
  private static final Arg<Amount<Long, Time>> HISTORY_MIN_RETENTION_THRESHOLD =
      Arg.create(Amount.of(1L, Time.HOURS));

  @CmdLine(name = "job_update_history_per_job_threshold",
      help = "Maximum number of completed job updates to retain in a job update history.")
  private static final Arg<Integer> JOB_UPDATE_HISTORY_PER_JOB_THRESHOLD = Arg.create(10);

  @CmdLine(name = "job_update_history_pruning_interval",
      help = "Job update history pruning interval.")
  private static final Arg<Amount<Long, Time>> JOB_UPDATE_HISTORY_PRUNING_INTERVAL =
      Arg.create(Amount.of(15L, Time.MINUTES));

  @CmdLine(name = "job_update_history_pruning_threshold",
      help = "Time after which the scheduler will prune completed job update history.")
  private static final Arg<Amount<Long, Time>> JOB_UPDATE_HISTORY_PRUNING_THRESHOLD =
      Arg.create(Amount.of(30L, Time.DAYS));

  @Override
  protected void configure() {
    install(new PrivateModule() {
      @Override
      protected void configure() {
        // TODO(ksweeney): Create a configuration validator module so this can be injected.
        // TODO(William Farner): Revert this once large task counts is cheap ala hierarchichal store
        bind(HistoryPrunnerSettings.class).toInstance(new HistoryPrunnerSettings(
            HISTORY_PRUNE_THRESHOLD.get(),
            HISTORY_MIN_RETENTION_THRESHOLD.get(),
            HISTORY_MAX_PER_JOB_THRESHOLD.get()
        ));

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
                JOB_UPDATE_HISTORY_PRUNING_INTERVAL.get(),
                JOB_UPDATE_HISTORY_PRUNING_THRESHOLD.get(),
                JOB_UPDATE_HISTORY_PER_JOB_THRESHOLD.get()));

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

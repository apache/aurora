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
package org.apache.aurora.scheduler.updater;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;

import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.util.BackoffStrategy;
import org.apache.aurora.common.util.TruncatedBinaryBackoff;
import org.apache.aurora.scheduler.BatchWorker;
import org.apache.aurora.scheduler.BatchWorker.NoResult;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.config.CliOptions;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.apache.aurora.scheduler.preemptor.BiCache;
import org.apache.aurora.scheduler.preemptor.BiCache.BiCacheSettings;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Binding module for scheduling logic and higher-level state management.
 */
public class UpdaterModule extends AbstractModule {
  private static final Logger LOG = LoggerFactory.getLogger(UpdaterModule.class);

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-enable_update_affinity",
        description = "Enable best-effort affinity of task updates.",
        arity = 1)
    public boolean enableAffinity = false;

    @Parameter(names = "-update_affinity_reservation_hold_time",
        description = "How long to wait for a reserved agent to reoffer freed up resources.")
    public TimeAmount affinityExpiration = new TimeAmount(3L, Time.MINUTES);

    @Parameter(names = "-sla_aware_action_max_batch_size",
        description = "The maximum number of sla aware update actions that can be processed"
            + " in a batch.")
    public int slaAwareActionMaxBatchSize = 300;

    @Parameter(names = "-sla_aware_kill_retry_min_delay",
        description = "Minimum amount of time to wait between attempting to perform an SLA-Aware"
            + " kill on a task.")
    public TimeAmount slaAwareKillRetryMinDelay = new TimeAmount(1L, Time.MINUTES);

    @Parameter(names = "-sla_aware_kill_retry_max_delay",
        description = "Maximum amount of time to wait between attempting to perform an SLA-Aware"
            + " kill on a task.")
    public TimeAmount slaAwareKillRetryMaxDelay = new TimeAmount(5L, Time.MINUTES);
  }

  private final ScheduledExecutorService executor;
  private final Optional<UpdateActionBatchWorker> batchWorker;
  private final Options options;

  public UpdaterModule(Options options) {
    this(
        AsyncUtil.singleThreadLoggingScheduledExecutor("updater-%d", LOG),
        Optional.empty(),
        options);
  }

  @VisibleForTesting
  UpdaterModule(ScheduledExecutorService executor,
                Optional<UpdateActionBatchWorker> batchWorker,
                Options options) {

    this.executor = Objects.requireNonNull(executor);
    this.batchWorker = batchWorker;
    this.options = options;
  }

  @Override
  protected void configure() {
    install(new PrivateModule() {
      @Override
      protected void configure() {
        if (options.enableAffinity) {
          bind(BiCacheSettings.class).toInstance(
              new BiCacheSettings(options.affinityExpiration, "update_affinity"));
          bind(new TypeLiteral<BiCache<IInstanceKey, TaskGroupKey>>() { }).in(Singleton.class);
          bind(UpdateAgentReserver.class).to(UpdateAgentReserver.UpdateAgentReserverImpl.class);
          bind(UpdateAgentReserver.UpdateAgentReserverImpl.class).in(Singleton.class);
        } else {
          bind(UpdateAgentReserver.class).to(UpdateAgentReserver.NullAgentReserver.class);
          bind(UpdateAgentReserver.NullAgentReserver.class).in(Singleton.class);
        }
        expose(UpdateAgentReserver.class);

        bind(BackoffStrategy.class).toInstance(new TruncatedBinaryBackoff(
            options.slaAwareKillRetryMinDelay,
            options.slaAwareKillRetryMaxDelay));
        bind(SlaKillController.class).in(Singleton.class);

        bind(ScheduledExecutorService.class).toInstance(executor);
        bind(UpdateFactory.class).to(UpdateFactory.UpdateFactoryImpl.class);
        bind(UpdateFactory.UpdateFactoryImpl.class).in(Singleton.class);
        bind(JobUpdateController.class).to(JobUpdateControllerImpl.class);
        bind(JobUpdateControllerImpl.class).in(Singleton.class);
        expose(JobUpdateController.class);
        bind(JobUpdateEventSubscriber.class);
        expose(JobUpdateEventSubscriber.class);
      }
    });

    if (batchWorker.isPresent()) {
      bind(UpdateActionBatchWorker.class).toInstance(batchWorker.get());
    } else {
      bind(UpdateActionBatchWorker.class).in(Singleton.class);
    }
    SchedulerServicesModule.addSchedulerActiveServiceBinding(binder())
        .to(UpdateActionBatchWorker.class);

    PubsubEventModule.bindSubscriber(binder(), JobUpdateEventSubscriber.class);
    SchedulerServicesModule.addSchedulerActiveServiceBinding(binder())
        .to(JobUpdateEventSubscriber.class);
  }

  public static class UpdateActionBatchWorker extends BatchWorker<NoResult> {

    @Inject
    UpdateActionBatchWorker(CliOptions options, Storage storage, StatsProvider statsProvider) {
      super(storage, statsProvider, options.updater.slaAwareActionMaxBatchSize);
    }

    @Override
    protected String serviceName() {
      return "UpdateActionBatchWorker";
    }
  }
}

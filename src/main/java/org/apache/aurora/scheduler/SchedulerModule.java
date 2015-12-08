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
package org.apache.aurora.scheduler;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;

import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.common.args.constraints.CanRead;
import org.apache.aurora.common.args.constraints.Positive;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.scheduler.SchedulerLifecycle.LeadingOptions;
import org.apache.aurora.scheduler.TaskIdGenerator.TaskIdGeneratorImpl;
import org.apache.aurora.scheduler.TierManager.TierManagerImpl;
import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.apache.mesos.Protos;
import org.codehaus.jackson.map.ObjectMapper;

import static org.apache.aurora.scheduler.SchedulerServicesModule.addSchedulerActiveServiceBinding;
import static org.apache.aurora.scheduler.TierManager.TierManagerImpl.TierConfig;

/**
 * Binding module for top-level scheduling logic.
 */
public class SchedulerModule extends AbstractModule {

  private static final Logger LOG = Logger.getLogger(SchedulerModule.class.getName());

  @CmdLine(name = "max_registration_delay",
      help = "Max allowable delay to allow the driver to register before aborting")
  private static final Arg<Amount<Long, Time>> MAX_REGISTRATION_DELAY =
      Arg.create(Amount.of(1L, Time.MINUTES));

  @CmdLine(name = "max_leading_duration",
      help = "After leading for this duration, the scheduler should commit suicide.")
  private static final Arg<Amount<Long, Time>> MAX_LEADING_DURATION =
      Arg.create(Amount.of(1L, Time.DAYS));

  @Positive
  @CmdLine(name = "max_status_update_batch_size",
      help = "The maximum number of status updates that can be processed in a batch.")
  private static final Arg<Integer> MAX_STATUS_UPDATE_BATCH_SIZE = Arg.create(1000);

  @CanRead
  @CmdLine(name = "tier_config",
      help = "Configuration file defining supported task tiers, task traits and behaviors.")
  private static final Arg<File> TIER_CONFIG_FILE = Arg.create();

  @Override
  protected void configure() {
    bind(TaskIdGenerator.class).to(TaskIdGeneratorImpl.class);

    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(LeadingOptions.class).toInstance(
            new LeadingOptions(MAX_REGISTRATION_DELAY.get(), MAX_LEADING_DURATION.get()));

        final ScheduledExecutorService executor =
            AsyncUtil.singleThreadLoggingScheduledExecutor("Lifecycle-%d", LOG);

        bind(ScheduledExecutorService.class).toInstance(executor);
        bind(SchedulerLifecycle.class).in(Singleton.class);
        expose(SchedulerLifecycle.class);
      }
    });

    PubsubEventModule.bindSubscriber(binder(), SchedulerLifecycle.class);
    bind(TaskVars.class).in(Singleton.class);
    PubsubEventModule.bindSubscriber(binder(), TaskVars.class);
    addSchedulerActiveServiceBinding(binder()).to(TaskVars.class);

    bind(new TypeLiteral<BlockingQueue<Protos.TaskStatus>>() { })
        .annotatedWith(TaskStatusHandlerImpl.StatusUpdateQueue.class)
        .toInstance(new LinkedBlockingQueue<>());
    bind(new TypeLiteral<Integer>() { })
        .annotatedWith(TaskStatusHandlerImpl.MaxBatchSize.class)
        .toInstance(MAX_STATUS_UPDATE_BATCH_SIZE.get());

    bind(TaskStatusHandler.class).to(TaskStatusHandlerImpl.class);
    bind(TaskStatusHandlerImpl.class).in(Singleton.class);

    bind(TierConfig.class).toInstance(parseTierConfig(readTierFile()));
    bind(TierManager.class).to(TierManagerImpl.class);
    bind(TierManagerImpl.class).in(Singleton.class);
    addSchedulerActiveServiceBinding(binder()).to(TaskStatusHandlerImpl.class);
  }

  private static Optional<String> readTierFile() {
    if (TIER_CONFIG_FILE.hasAppliedValue()) {
      try {
        return Optional.of(Files.toString(TIER_CONFIG_FILE.get(), StandardCharsets.UTF_8));
      } catch (IOException e) {
        LOG.severe("Error loading tier configuration file.");
        throw Throwables.propagate(e);
      }
    }

    return Optional.<String>absent();
  }

  @VisibleForTesting
  static TierConfig parseTierConfig(Optional<String> config) {
    Optional<TierConfig> map = config.transform(input -> {
      try {
        return new ObjectMapper().readValue(input, TierConfig.class);
      } catch (IOException e) {
        LOG.severe("Error parsing tier configuration file.");
        throw Throwables.propagate(e);
      }
    });
    return map.or(TierConfig.EMPTY);
  }
}

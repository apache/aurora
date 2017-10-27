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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;

import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.scheduler.BatchWorker.NoResult;
import org.apache.aurora.scheduler.SchedulerLifecycle.LeadingOptions;
import org.apache.aurora.scheduler.TaskIdGenerator.TaskIdGeneratorImpl;
import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.config.CliOptions;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.config.validators.PositiveNumber;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.mesos.v1.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.aurora.scheduler.SchedulerServicesModule.addSchedulerActiveServiceBinding;

/**
 * Binding module for top-level scheduling logic.
 */
public class SchedulerModule extends AbstractModule {

  private static final Logger LOG = LoggerFactory.getLogger(SchedulerModule.class);

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-max_registration_delay",
        description = "Max allowable delay to allow the driver to register before aborting")
    public TimeAmount maxRegistrationDelay = new TimeAmount(1, Time.MINUTES);

    @Parameter(names = "-max_leading_duration",
        description = "After leading for this duration, the scheduler should commit suicide.")
    public TimeAmount maxLeadingDuration = new TimeAmount(1, Time.DAYS);

    @Parameter(names = "-max_status_update_batch_size",
        validateValueWith = PositiveNumber.class,
        description = "The maximum number of status updates that can be processed in a batch.")
    public int maxStatusUpdateBatchSize = 1000;

    @Parameter(names = "-max_task_event_batch_size",
        validateValueWith = PositiveNumber.class,
        description =
            "The maximum number of task state change events that can be processed in a batch.")
    public int maxTaskEventBatchSize = 300;
  }

  private final Options options;

  public SchedulerModule(Options options) {
    this.options = options;
  }

  @Override
  protected void configure() {
    bind(TaskIdGenerator.class).to(TaskIdGeneratorImpl.class);

    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(LeadingOptions.class).toInstance(
            new LeadingOptions(options.maxRegistrationDelay, options.maxLeadingDuration));

        final ScheduledExecutorService executor =
            AsyncUtil.singleThreadLoggingScheduledExecutor("Lifecycle-%d", LOG);

        bind(ScheduledExecutorService.class).toInstance(executor);
        bind(SchedulerLifecycle.class).in(Singleton.class);
        expose(SchedulerLifecycle.class);
      }
    });

    PubsubEventModule.bindRegisteredSubscriber(binder(), SchedulerLifecycle.class);
    bind(TaskVars.class).in(Singleton.class);
    PubsubEventModule.bindSubscriber(binder(), TaskVars.class);
    addSchedulerActiveServiceBinding(binder()).to(TaskVars.class);

    bind(new TypeLiteral<BlockingQueue<Protos.TaskStatus>>() { })
        .annotatedWith(TaskStatusHandlerImpl.StatusUpdateQueue.class)
        .toInstance(new LinkedBlockingQueue<>());
    bind(new TypeLiteral<Integer>() { })
        .annotatedWith(TaskStatusHandlerImpl.MaxBatchSize.class)
        .toInstance(options.maxStatusUpdateBatchSize);

    bind(TaskStatusHandler.class).to(TaskStatusHandlerImpl.class);
    bind(TaskStatusHandlerImpl.class).in(Singleton.class);
    addSchedulerActiveServiceBinding(binder()).to(TaskStatusHandlerImpl.class);

    bind(TaskEventBatchWorker.class).in(Singleton.class);
    addSchedulerActiveServiceBinding(binder()).to(TaskEventBatchWorker.class);
  }

  public static class TaskEventBatchWorker extends BatchWorker<NoResult> {
    @Inject
    TaskEventBatchWorker(CliOptions options, Storage storage, StatsProvider statsProvider) {
      super(storage, statsProvider, options.scheduler.maxTaskEventBatchSize);
    }

    @Override
    protected String serviceName() {
      return "TaskEventBatchWorker";
    }
  }
}

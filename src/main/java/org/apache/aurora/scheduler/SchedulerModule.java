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

import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

import javax.inject.Singleton;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.binder.LinkedBindingBuilder;
import com.google.inject.multibindings.Multibinder;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

import org.apache.aurora.GuavaUtils;
import org.apache.aurora.GuavaUtils.ServiceManagerIface;
import org.apache.aurora.scheduler.SchedulerLifecycle.LeadingOptions;
import org.apache.aurora.scheduler.SchedulerLifecycle.SchedulerActive;
import org.apache.aurora.scheduler.TaskIdGenerator.TaskIdGeneratorImpl;
import org.apache.aurora.scheduler.async.GcExecutorLauncher;
import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.events.PubsubEventModule;

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

  @Override
  protected void configure() {
    bind(TaskIdGenerator.class).to(TaskIdGeneratorImpl.class);
    bind(UserTaskLauncher.class).in(Singleton.class);

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
  }

  @Provides
  @Singleton
  List<TaskLauncher> provideTaskLaunchers(
      GcExecutorLauncher gcLauncher,
      UserTaskLauncher userTaskLauncher) {

    return ImmutableList.of(gcLauncher, userTaskLauncher);
  }

  /**
   * Register a Service to run after storage is ready, but before the scheduler has announced
   * leadership. If this service fails to startup the scheduler will abort.
   *
   * Usage: {@code addSchedulerActiveServiceBinding(binder()).to(YourService.class)}.
   *
   * @param binder Binder for the current non-private module.
   * @return a linked binding builder with the normal Guice EDSL methods.
   */
  public static LinkedBindingBuilder<Service> addSchedulerActiveServiceBinding(Binder binder) {
    return Multibinder.newSetBinder(binder, Service.class, SchedulerActive.class).addBinding();
  }

  @Provides
  @Singleton
  @SchedulerActive
  ServiceManagerIface provideSchedulerActiveServiceManager(@SchedulerActive Set<Service> services) {
    return GuavaUtils.serviceManager(new ServiceManager(services));
  }
}

/*
 * Copyright 2013 Twitter, Inc.
 *
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
package com.twitter.aurora.scheduler;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.inject.Singleton;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;

import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import com.twitter.aurora.scheduler.Driver.DriverImpl;
import com.twitter.aurora.scheduler.SchedulerLifecycle.DriverReference;
import com.twitter.aurora.scheduler.SchedulerLifecycle.LeadingOptions;
import com.twitter.aurora.scheduler.TaskIdGenerator.TaskIdGeneratorImpl;
import com.twitter.aurora.scheduler.events.PubsubEventModule;
import com.twitter.aurora.scheduler.periodic.GcExecutorLauncher;
import com.twitter.aurora.scheduler.periodic.GcExecutorLauncher.GcExecutorSettings;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

/**
 * Binding module for top-level scheduling logic.
 */
public class SchedulerModule extends AbstractModule {

  @CmdLine(name = "executor_gc_interval",
      help = "Max interval on which to run the GC executor on a host to clean up dead tasks.")
  private static final Arg<Amount<Long, Time>> EXECUTOR_GC_INTERVAL =
      Arg.create(Amount.of(1L, Time.HOURS));

  @CmdLine(name = "gc_executor_path", help = "Path to the gc executor launch script.")
  private static final Arg<String> GC_EXECUTOR_PATH = Arg.create(null);

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
    bind(Driver.class).to(DriverImpl.class);
    bind(DriverImpl.class).in(Singleton.class);
    bind(new TypeLiteral<Supplier<Optional<SchedulerDriver>>>() { }).to(DriverReference.class);
    bind(DriverReference.class).in(Singleton.class);

    bind(Scheduler.class).to(MesosSchedulerImpl.class);
    bind(MesosSchedulerImpl.class).in(Singleton.class);

    bind(TaskIdGenerator.class).to(TaskIdGeneratorImpl.class);

    bind(GcExecutorSettings.class).toInstance(new GcExecutorSettings(
        EXECUTOR_GC_INTERVAL.get(),
        Optional.fromNullable(GC_EXECUTOR_PATH.get())));

    bind(GcExecutorLauncher.class).in(Singleton.class);
    bind(UserTaskLauncher.class).in(Singleton.class);

    install(new PrivateModule() {
      @Override protected void configure() {
        bind(LeadingOptions.class).toInstance(
            new LeadingOptions(MAX_REGISTRATION_DELAY.get(), MAX_LEADING_DURATION.get()));
          final ScheduledExecutorService executor = Executors.newScheduledThreadPool(
              1,
              new ThreadFactoryBuilder().setNameFormat("Lifecycle-%d").setDaemon(true).build());
        bind(ScheduledExecutorService.class).toInstance(executor);
        bind(SchedulerLifecycle.class).in(Singleton.class);
        expose(SchedulerLifecycle.class);
      }
    });

    PubsubEventModule.bindSubscriber(binder(), SchedulerLifecycle.class);
    PubsubEventModule.bindSubscriber(binder(), TaskVars.class);
  }

  @Provides
  @Singleton
  List<TaskLauncher> provideTaskLaunchers(
      GcExecutorLauncher gcLauncher,
      UserTaskLauncher userTaskLauncher) {

    return ImmutableList.of(gcLauncher, userTaskLauncher);
  }
}

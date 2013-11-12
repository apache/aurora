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
package com.twitter.aurora.scheduler.periodic;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;

import com.twitter.aurora.scheduler.async.Preemptor;
import com.twitter.aurora.scheduler.async.Preemptor.PreemptionDelay;
import com.twitter.aurora.scheduler.periodic.PeriodicTaskLauncher.PeriodicTaskInterval;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

/**
 * Binding module that configures periodic scheduler tasks.
 */
public class PeriodicTaskModule extends AbstractModule {

  @CmdLine(name = "periodic_task_interval",
      help = "Interval on which to run task garbage collection tasks.")
  private static final Arg<Amount<Long, Time>> PERIODIC_TASK_INTERVAL =
      Arg.create(Amount.of(2L, Time.MINUTES));

  @CmdLine(name = "preemption_delay",
      help = "Time interval after which a pending task becomes eligible to preempt other tasks")
  private static final Arg<Amount<Long, Time>> PREEMPTION_DELAY =
      Arg.create(Amount.of(10L, Time.MINUTES));

  @Override
  protected void configure() {

    bind(new TypeLiteral<Amount<Long, Time>>() { }).annotatedWith(PeriodicTaskInterval.class)
        .toInstance(PERIODIC_TASK_INTERVAL.get());

    bind(new TypeLiteral<Amount<Long, Time>>() { }).annotatedWith(PreemptionDelay.class)
        .toInstance(PREEMPTION_DELAY.get());
    bind(Preemptor.class).in(Singleton.class);

    LifecycleModule.bindStartupAction(binder(), PeriodicTaskLauncher.class);
  }
}

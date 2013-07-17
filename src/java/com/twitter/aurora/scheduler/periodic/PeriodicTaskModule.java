package com.twitter.aurora.scheduler.periodic;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import com.twitter.aurora.scheduler.periodic.PeriodicTaskLauncher.PeriodicTaskInterval;
import com.twitter.aurora.scheduler.periodic.Preempter.PreemptionDelay;
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
      Arg.create(Amount.of(2L, Time.MINUTES));

  @Override
  protected void configure() {

    bind(new TypeLiteral<Amount<Long, Time>>() { }).annotatedWith(PeriodicTaskInterval.class)
        .toInstance(PERIODIC_TASK_INTERVAL.get());

    bind(new TypeLiteral<Amount<Long, Time>>() { }).annotatedWith(PreemptionDelay.class)
        .toInstance(PREEMPTION_DELAY.get());
    bind(Preempter.class).in(Singleton.class);

    LifecycleModule.bindStartupAction(binder(), PeriodicTaskLauncher.class);
  }
}

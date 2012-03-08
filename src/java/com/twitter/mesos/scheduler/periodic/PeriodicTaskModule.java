package com.twitter.mesos.scheduler.periodic;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.mesos.scheduler.periodic.HistoryPruner.HistoryPrunerImpl;
import com.twitter.mesos.scheduler.periodic.HistoryPruner.HistoryPrunerImpl.PruneThreshold;
import com.twitter.mesos.scheduler.periodic.PeriodicTaskLauncher.PeriodicTaskInterval;

/**
 * Binding module that configures periodic scheduler tasks.
 *
 * @author William Farner
 */
public class PeriodicTaskModule extends AbstractModule {

  @CmdLine(name = "periodic_task_interval",
      help = "Interval on which to run task garbage collection tasks.")
  private static final Arg<Amount<Long, Time>> PERIODIC_TASK_INTERVAL =
      Arg.create(Amount.of(5L, Time.MINUTES));

  @CmdLine(name = "history_prune_threshold",
      help = "Time after which the scheduler will prune terminated task history.")
  private static final Arg<Amount<Long, Time>> HISTORY_PRUNE_THRESHOLD =
      Arg.create(Amount.of(2L, Time.DAYS));

  @CmdLine(name = "per_job_task_history_goal",
      help = "Per-job task history that the scheduler attempts to retain.")
  private static final Arg<Integer> PER_JOB_TASK_HISTORY_GOAL = Arg.create(300);

  @Override
  protected void configure() {
    bind(Integer.class).annotatedWith(PruneThreshold.class)
        .toInstance(PER_JOB_TASK_HISTORY_GOAL.get());
    bind(new TypeLiteral<Amount<Long, Time>>() { }).annotatedWith(PruneThreshold.class)
        .toInstance(HISTORY_PRUNE_THRESHOLD.get());
    bind(HistoryPruner.class).to(HistoryPrunerImpl.class);
    bind(HistoryPrunerImpl.class).in(Singleton.class);

    bind(new TypeLiteral<Amount<Long, Time>>() { }).annotatedWith(PeriodicTaskInterval.class)
        .toInstance(PERIODIC_TASK_INTERVAL.get());
    bind(HistoryPruneRunner.class).in(Singleton.class);

    LifecycleModule.bindStartupAction(binder(), PeriodicTaskLauncher.class);
  }

  /**
   * Binds the periodic task module.
   *
   * @param binder a guice binder to bind with.
   */
  public static void bind(Binder binder) {
    binder.install(new PeriodicTaskModule());
  }
}

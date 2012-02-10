package com.twitter.mesos.scheduler.periodic;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;

import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.concurrent.ExecutorServiceShutdown;
import com.twitter.mesos.scheduler.StateManager;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Launcher responsible for scheduling and cleaning up after periodic tasks in the scheduler.
 *
 * @author William Farner
 */
public class PeriodicTaskLauncher implements Command, Runnable {

  private static final Logger LOG = Logger.getLogger(PeriodicTaskLauncher.class.getName());

  /**
   * Binding annotation for the periodic task execution interval.
   *
   * @author William Farner
   */
  @BindingAnnotation
  @Target({FIELD, PARAMETER, METHOD}) @Retention(RUNTIME)
  public @interface PeriodicTaskInterval {}

  private final HistoryPruneRunner pruneRunner;
  private final StateManager stateManager;
  private final ShutdownRegistry shutdownRegistry;
  private final ScheduledExecutorService executor;
  private final Amount<Long, Time> taskInterval;

  @Inject
  PeriodicTaskLauncher(
      HistoryPruneRunner pruneRunner,
      StateManager stateManager,
      ShutdownRegistry shutdownRegistry,
      @PeriodicTaskInterval Amount<Long, Time> taskInterval) {

    this.pruneRunner = checkNotNull(pruneRunner);
    this.stateManager = checkNotNull(stateManager);
    this.shutdownRegistry = checkNotNull(shutdownRegistry);
    this.taskInterval = checkNotNull(taskInterval);

    executor = Executors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder()
            .setNameFormat("Scheduler-Periodic-Task-%d")
            .setDaemon(true)
            .build());
  }

  @Override
  public void execute() {
    shutdownRegistry.addAction(new Command() {
      @Override public void execute() {
        new ExecutorServiceShutdown(executor, Amount.of(1L, Time.SECONDS)).execute();
      }
    });

    executor.scheduleAtFixedRate(this,
        taskInterval.getValue(),
        taskInterval.getValue(),
        taskInterval.getUnit().getTimeUnit());
  }

  @Override
  public void run() {
    try {
      if (stateManager.isStarted()) {
        pruneRunner.run();
        stateManager.scanOutstandingTasks();
      } else {
        LOG.fine("Skipping periodic task run since state manager is not started.");
      }
    } catch (RuntimeException e) {
      LOG.log(Level.WARNING, "Periodic task failed.", e);
    }
  }
}

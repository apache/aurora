package com.twitter.mesos.executor;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.google.inject.Module;

import com.twitter.common.application.AbstractApplication;
import com.twitter.common.application.AppLauncher;
import com.twitter.common.application.Lifecycle;
import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.application.modules.HttpModule;
import com.twitter.common.application.modules.LogModule;
import com.twitter.common.application.modules.StatsExportModule;
import com.twitter.common.application.modules.StatsModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.ArgFilters;
import com.twitter.common.args.CmdLine;
import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

/**
 * ExecutorMain
 *
 * @author Florian Leibert
 */
public class ExecutorMain extends AbstractApplication {

  @CmdLine(name = "executor_initialization_timeout",
      help = "Specifies the maximum amount of time to wait for the executor driver to initialize "
             + "before comitting suicide.")
  private static final Arg<Amount<Long, Time>> driverInitializationTimeout =
      Arg.create(Amount.of(5L, Time.SECONDS));

  private static final Logger LOG = Logger.getLogger(ExecutorMain.class.getName());

  @Inject @ExecutorRootDir private File executorRootDir;
  @Inject private DriverRunner driverRunner;
  @Inject private ExecutorCore executorCore;
  @Inject private Supplier<Iterable<Task>> deadTaskLoader;
  @Inject private ShutdownRegistry shutdownRegistry;
  @Inject private Lifecycle lifecycle;

  @Override
  public void run() {
    shutdownRegistry.addAction(new Command() {
      @Override public void execute() {
        LOG.info("Shutting down the executor.");
        executorCore.shutdownCore();
      }
    });

    if (!executorRootDir.exists()) {
      Preconditions.checkState(executorRootDir.mkdirs(), "Failed to create executor root dir.");
    }

    executorCore.addDeadTasks(deadTaskLoader.get());

    LOG.info("Starting executor driver");
    try {
      Amount<Long, Time> timeout = driverInitializationTimeout.get();
      if (!driverRunner.run(timeout)) {
        LOG.severe("Commiting suicide - executor driver did not initialize within " + timeout);
        return;
      }
    } catch (InterruptedException e) {
      LOG.info("Interrupted waiting for driver to initialize - shutting down");
      return;
    }

    LOG.info("Executor driver initialized - starting periodic tasks");
    executorCore.startPeriodicTasks(shutdownRegistry);

    lifecycle.awaitShutdown();
  }

  @Override
  public Iterable<? extends Module> getModules() {
    return Arrays.asList(
        new LogModule(),
        new HttpModule(),
        new StatsModule(),
        new StatsExportModule(),
        new ExecutorModule()
    );
  }

  public static void main(String[] args) {
    AppLauncher.launch(new ExecutorMain(), ArgFilters.SELECT_ALL, args);
  }
}

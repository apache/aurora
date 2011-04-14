package com.twitter.mesos.executor;

import java.io.File;
import java.util.Arrays;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.google.inject.Module;

import org.apache.mesos.Executor;
import org.apache.mesos.MesosExecutorDriver;

import com.twitter.common.application.AbstractApplication;
import com.twitter.common.application.ActionRegistry;
import com.twitter.common.application.ShutdownStage;
import com.twitter.common.application.modules.HttpModule;
import com.twitter.common.application.modules.LogModule;
import com.twitter.common.application.modules.StatsExportModule;
import com.twitter.common.application.modules.StatsModule;
import com.twitter.common.base.Command;

/**
 * ExecutorMain
 *
 * @author Florian Leibert
 */
public class ExecutorMain extends AbstractApplication {

  private static final Logger LOG = Logger.getLogger(ExecutorMain.class.getName());

  @Inject @ExecutorRootDir private File executorRootDir;
  @Inject private Executor mesosExecutor;
  @Inject private ExecutorCore executorCore;
  @Inject private Supplier<Iterable<Task>> deadTaskLoader;
  @Inject @ShutdownStage private ActionRegistry shutdownRegistry;

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
    executorCore.startPeriodicTasks();

    new MesosExecutorDriver(mesosExecutor).run();
  }

  @Override
  public Iterable<Module> getModules() {
    return Arrays.<Module>asList(
        new LogModule(),
        new HttpModule(),
        new StatsModule(),
        new StatsExportModule(),
        new ExecutorModule()
    );
  }
}

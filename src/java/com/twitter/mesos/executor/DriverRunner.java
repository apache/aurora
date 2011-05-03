package com.twitter.mesos.executor;

import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import org.apache.mesos.MesosExecutorDriver;

import com.twitter.common.application.ActionRegistry;
import com.twitter.common.application.ShutdownStage;
import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

/**
 * Allows clients to spawn a {@link MesosExecutorDriver} in its own thread with assurance that
 * initialization of the driver has been completed.
 *
 * @author John Sirois
 */
class DriverRunner {

  private static final Logger LOG = Logger.getLogger(DriverRunner.class.getName());

  private final ActionRegistry shutdownRegistry;
  private final MesosExecutorImpl mesosExecutor;

  @Inject
  DriverRunner(@ShutdownStage ActionRegistry shutdownRegistry,
      MesosExecutorImpl mesosExecutor) {
    this.shutdownRegistry = Preconditions.checkNotNull(shutdownRegistry);
    this.mesosExecutor = Preconditions.checkNotNull(mesosExecutor);
  }

  /**
   * Starts the {@link MesosExecutorDriver} in the background, returning only after the the driver
   * finishes initialization or the specified {@code timeout} expires.
   *
   * @param timeout the maximum amount of time to wait for driver initialization
   * @return {@code true} if the driver initialized within the specified {@code timeout}
   * @throws InterruptedException if interrupted awaiting driver initialization
   */
  boolean run(Amount<Long, Time> timeout) throws InterruptedException {
    final MesosExecutorDriver executorDriver = new MesosExecutorDriver(mesosExecutor);
    Thread driverMain = new Thread(new Runnable() {
      @Override public void run() {
        executorDriver.run();
        shutdownRegistry.addAction(new Command() {
          @Override public void execute() {
            LOG.info("Shutting down excutor driver.");
            executorDriver.stop();
          }
        });
      }
    }, "MesosExecutorDriver-main");
    driverMain.setDaemon(true);
    driverMain.start();

    return mesosExecutor.awaitInit(timeout);
  }
}

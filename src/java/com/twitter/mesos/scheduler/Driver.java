package com.twitter.mesos.scheduler;

import javax.annotation.Nullable;

import com.google.common.base.Supplier;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

import com.twitter.common.util.StateMachine;

/**
 * Wraps the mesos core Scheduler driver to ensure its used in a valid lifecycle; namely:
 * <pre>
 *   (run -> kill*)? -> stop*
 * </pre>
 *
 * Also ensures the driver is only asked for when needed.
 *
 * @author John Sirois
 */
class Driver {
  enum State {
    INIT,
    RUNNING,
    STOPPED
  }

  private final StateMachine<State> stateMachine;
  private final Supplier<SchedulerDriver> driverSupplier;
  @Nullable private SchedulerDriver schedulerDriver;

  /**
   * Creates a driver manager that will only ask for the underlying mesos driver when actually
   * needed.
   *
   * @param driverSupplier A factory for the underlying driver.
   */
  Driver(Supplier<SchedulerDriver> driverSupplier) {
    this.driverSupplier = driverSupplier;
    this.stateMachine =
        StateMachine.<State>builder("scheduler_driver")
            .initialState(State.INIT)
            .addState(State.INIT, State.RUNNING, State.STOPPED)
            .addState(State.RUNNING, State.STOPPED)
            .logTransitions()
            .throwOnBadTransition(true)
            .build();
  }

  private synchronized SchedulerDriver get(State expected) {
    stateMachine.checkState(expected);
    if (schedulerDriver == null) {
      schedulerDriver = driverSupplier.get();
    }
    return schedulerDriver;
  }

  /**
   * Runs the underlying driver.  Can only be called once.
   *
   * @return The status of the underlying driver run request.
   */
  Protos.Status run() {
    SchedulerDriver driver = get(State.INIT);
    stateMachine.transition(State.RUNNING);
    return driver.run();
  }

  /**
   * Stops the underlying driver if it is running, otherwise does nothing.
   */
  synchronized void stop() {
    if (schedulerDriver != null) {
      schedulerDriver.stop(true /* failover */);
      schedulerDriver = null;
      stateMachine.transition(State.STOPPED);
    }
  }

  /**
   * Uses the underlying driver to send a kill task request for the given {@code taskId} to the
   * mesos master.
   *
   * @param taskId The id of the task to kill.
   * @return The status of the underlying driver kill request.
   */
  Protos.Status killTask(Protos.TaskID taskId) {
    SchedulerDriver driver = get(State.RUNNING);
    return driver.killTask(taskId);
  }
}

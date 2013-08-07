package com.twitter.aurora.scheduler;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.SchedulerDriver;

import com.twitter.common.stats.Stats;
import com.twitter.common.util.StateMachine;

import static org.apache.mesos.Protos.Status.DRIVER_RUNNING;

/**
 * Wraps the mesos Scheduler driver to ensure its used in a valid lifecycle; namely:
 * <pre>
 *   (run -> kill*)? -> stop*
 * </pre>
 *
 * Also ensures the driver is only asked for when needed.
 */
public interface Driver {

  /**
   * Launches a task.
   *
   * @param offerId ID of the resource offer to accept with the task.
   * @param task Task to launch.
   */
  void launchTask(OfferID offerId, TaskInfo task);

  /**
   * Declines a resource offer.
   *
   * @param offerId ID of the offer to decline.
   */
  void declineOffer(OfferID offerId);

  /**
   * Sends a kill task request for the given {@code taskId} to the mesos master.
   *
   * @param taskId The id of the task to kill.
   */
  void killTask(String taskId);

  /**
   * Stops the underlying driver if it is running, otherwise does nothing.
   */
  void stop();

  /**
   * Runs the underlying driver.  Can only be called once.
   *
   * @return The status of the underlying driver run request.
   */
  Protos.Status run();

  /**
   * Mesos driver implementation.
   */
  static class DriverImpl implements Driver {
    private static final Logger LOG = Logger.getLogger(Driver.class.getName());

    /**
     * Driver states.
     */
    enum State {
      INIT,
      RUNNING,
      STOPPED
    }

    private final StateMachine<State> stateMachine;
    private final Supplier<Optional<SchedulerDriver>> driverSupplier;
    private final AtomicLong killFailures = Stats.exportLong("scheduler_driver_kill_failures");

    /**
     * Creates a driver manager that will only ask for the underlying mesos driver when actually
     * needed.
     *
     * @param driverSupplier A factory for the underlying driver.
     */
    @Inject
    DriverImpl(Supplier<Optional<SchedulerDriver>> driverSupplier) {
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
      // TODO(William Farner): Formalize the failure case here by throwing a checked exception.
      stateMachine.checkState(expected);
      // This will and should fail if the driver is not present.
      return driverSupplier.get().get();
    }

    @Override
    public void launchTask(OfferID offerId, TaskInfo task) {
      get(State.RUNNING).launchTasks(offerId, ImmutableList.of(task));
    }

    @Override
    public void declineOffer(OfferID offerId) {
      get(State.RUNNING).declineOffer(offerId);
    }

    @Override
    public Protos.Status run() {
      SchedulerDriver driver = get(State.INIT);
      stateMachine.transition(State.RUNNING);
      return driver.run();
    }

    @Override
    public synchronized void stop() {
      if (stateMachine.getState() == State.RUNNING) {
        SchedulerDriver driver = get(State.RUNNING);
        driver.stop(true /* failover */);
        stateMachine.transition(State.STOPPED);
      }
    }

    @Override
    public void killTask(String taskId) {
      SchedulerDriver driver = get(State.RUNNING);
      Protos.Status status = driver.killTask(Protos.TaskID.newBuilder().setValue(taskId).build());

      if (status != DRIVER_RUNNING) {
        LOG.severe(String.format("Attempt to kill task %s failed with code %s",
            taskId, status));
        killFailures.incrementAndGet();
      }
    }
  }
}

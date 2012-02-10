package com.twitter.mesos.scheduler;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.inject.Inject;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.SchedulerDriver;

import com.twitter.common.stats.Stats;
import com.twitter.common.util.StateMachine;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.gen.comm.ExecutorMessage;

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
public interface Driver {

  /**
   * Sends a kill task request for the given {@code taskId} to the mesos master.
   *
   * @param taskId The id of the task to kill.
   */
  void killTask(String taskId);

  /**
   * Sends a message to an executor.
   *
   * @param message Message to send.
   * @param slave Slave to route the message to.
   * @param executor Executor to route to within the {@code slave}.
   */
  void sendMessage(ExecutorMessage message, SlaveID slave, ExecutorID executor);

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

  static class DriverImpl implements Driver {
    private static final Logger LOG = Logger.getLogger(Driver.class.getName());

    enum State {
      INIT,
      RUNNING,
      STOPPED
    }

    private final StateMachine<State> stateMachine;
    private final Supplier<Optional<SchedulerDriver>> driverSupplier;
    private final AtomicLong killFailures = Stats.exportLong("scheduler_driver_kill_failures");
    private final AtomicLong messageFailures =
        Stats.exportLong("scheduler_driver_message_failures");

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
      stateMachine.checkState(expected);
      // This will and should fail if the driver is not present.
      return driverSupplier.get().get();
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

      if (status != Protos.Status.OK) {
        LOG.severe(String.format("Attempt to kill task %s failed with code %s",
            taskId, status));
        killFailures.incrementAndGet();
      }
    }

    @Override
    public void sendMessage(ExecutorMessage message, SlaveID slave, ExecutorID executor) {
      SchedulerDriver driver = get(State.RUNNING);

      Preconditions.checkNotNull(message);
      Preconditions.checkNotNull(slave);
      Preconditions.checkNotNull(executor);

      byte[] data;
      try {
        data = ThriftBinaryCodec.encode(message);
      } catch (CodingException e) {
        LOG.log(Level.SEVERE, "Failed to send restart request.", e);
        return;
      }

      LOG.info(String.format("Attempting to send message to %s/%s - %s",
          slave.getValue(), executor.getValue(), message));
      Status status = driver.sendFrameworkMessage(slave, executor, data);
      if (status != Status.OK) {
        LOG.severe(
            String.format("Attempt to send message failed with code %s [%s]", status, message));
        messageFailures.incrementAndGet();
      } else {
        LOG.info("Message successfully sent");
      }
    }
  }
}

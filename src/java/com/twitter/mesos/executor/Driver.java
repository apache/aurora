package com.twitter.mesos.executor;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Atomics;
import com.google.inject.Inject;
import com.google.protobuf.ByteString;

import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos.ExecutorArgs;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskStatus;

import com.twitter.common.application.Lifecycle;
import com.twitter.common.stats.Stats;
import com.twitter.mesos.StateTranslator;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.gen.ScheduleStatus;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Abstraction away from the mesos executor driver.
 *
 * @author William Farner
 */
public interface Driver extends Function<Message, Integer> {

  /**
   * Sends an update on the status of a task.
   *
   * @param taskId Task to update the status for.
   * @param status New status of thet task.
   * @param reason The optional reason for the state change.
   * @return zero if the status was successfully sent (but not necessarily received), or non-zero
   *    if the status could not be sent.
   */
  int sendStatusUpdate(String taskId, ScheduleStatus status, Optional<String> reason);

  /**
   * Sets the underlying driver.
   *
   * @param driver Real driver.
   * @param executorArgs executor args.
   */
  void init(ExecutorDriver driver, ExecutorArgs executorArgs);

  /**
   * Stops the driver immediately.
   */
  void stop();


  public static class DriverImpl implements Driver {

    private static final Logger LOG = Logger.getLogger(DriverImpl.class.getName());

    private final AtomicReference<ExecutorDriver> driverRef = Atomics.newReference();

    private final AtomicLong statusUpdatesSent = Stats.exportLong("executor_status_updates_sent");
    private final AtomicLong statusUpdatesFailed =
        Stats.exportLong("executor_status_updates_failed");
    private final AtomicLong messagesSent = Stats.exportLong("executor_framework_messages_sent");
    private final AtomicLong messagesFailed =
        Stats.exportLong("executor_framework_messages_failed");

    private final Lifecycle lifecycle;

    @Inject
    public DriverImpl(Lifecycle lifecycle) {
      this.lifecycle = checkNotNull(lifecycle);
    }

    @Override
    public void init(ExecutorDriver driver, ExecutorArgs executorArgs) {
      LOG.info("Driver assigned " + driver + ", and args " + executorArgs);
      this.driverRef.set(driver);
    }

    /**
     * Convenience wrapper to do work if a driver reference is available.
     *
     * @param work Work to execute with the driver.
     * @return Return code from driver operation.
     */
    private int doWorkWithDriver(Function<ExecutorDriver, Integer> work) {
      ExecutorDriver driver = driverRef.get();

      if (driver == null) {
        LOG.warning("Driver not available, message could not be sent.");
        return -1;
      }

      return work.apply(driver);
    }

    @Override public Integer apply(final Message message) {
      Preconditions.checkNotNull(message);

      int result = doWorkWithDriver(new Function<ExecutorDriver, Integer>() {
        @Override public Integer apply(ExecutorDriver driver) {
          byte[] data;
          try {
            data = ThriftBinaryCodec.encode(message.getMessage());
          } catch (CodingException e) {
            LOG.log(Level.SEVERE, "Failed to encode message: " + message.getMessage()
                                  + " intended for slave " + message.getSlaveId());
            return -1;
          }

          LOG.info("Sending message to scheduler.");
          Status status = driver.sendFrameworkMessage(data);
          if (status != Status.OK) {
            LOG.warning(String.format("Attempt to send executor message returned code %s: %s",
                status, message));
            messagesFailed.incrementAndGet();
          } else {
            messagesSent.incrementAndGet();
          }

          return status.getNumber();
        }
      });

      if (result != 0) {
        LOG.severe("Attempt to send message failed with code " + result + ", committing suicide.");
        lifecycle.shutdown();
      }

      return result;
    }

    @Override public int sendStatusUpdate(final String taskId, final ScheduleStatus status,
        final Optional<String> reason) {
      Preconditions.checkNotNull(status);

      return doWorkWithDriver(new Function<ExecutorDriver, Integer>() {
        @Override public Integer apply(ExecutorDriver driver) {
          LOG.info("Notifying task " + taskId + " in state " + status);
          TaskStatus.Builder msg = TaskStatus.newBuilder()
              .setTaskId(TaskID.newBuilder().setValue(taskId))
              .setState(StateTranslator.get(status));
          if (reason.isPresent()) {
            msg.setData(ByteString.copyFromUtf8(reason.get()));
          }

          Status s = driver.sendStatusUpdate(msg.build());
          if (s != Status.OK) {
            LOG.warning("Status update failed with " + s + ", committing suicide.");
            lifecycle.shutdown();
          } else {
            statusUpdatesSent.incrementAndGet();
          }
          return s.getNumber();
        }
      });
    }

    @Override
    public void stop() {
      LOG.info("Stopping executor driver.");
      driverRef.get().stop();
    }
  }
}

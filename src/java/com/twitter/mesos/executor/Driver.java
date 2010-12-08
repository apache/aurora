package com.twitter.mesos.executor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.twitter.mesos.Message;
import com.twitter.mesos.StateTranslator;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.gen.ScheduleStatus;
import mesos.ExecutorDriver;
import mesos.FrameworkMessage;
import mesos.TaskStatus;

import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Abstraction away from the mesos executor driver.
 *
 * @author wfarner
 */
public interface Driver extends Function<Message, Integer> {

  /**
   * Sends an update on the status of a task.
   *
   * @param taskId Task to update the status for.
   * @param status New status of thet task.
   * @return zero if the status was successfully sent (but not necessarily received), or non-zero
   *    if the status could not be sent.
   */
  public int sendStatusUpdate(int taskId, ScheduleStatus status);

  /**
   * A driver with a settable reference for the mesos executor driver to use.
   */
  public static interface MesosDriver extends Driver {
    public void setDriver(ExecutorDriver driver);
  }

  public static class MesosDriverImpl implements MesosDriver {

    private static final Logger LOG = Logger.getLogger(MesosDriverImpl.class.getName());

    private final AtomicReference<ExecutorDriver> driverRef = new AtomicReference<ExecutorDriver>();

    private final static byte[] EMPTY_MSG = new byte[0];

    @Override
    public void setDriver(ExecutorDriver driver) {
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

      return doWorkWithDriver(new Function<ExecutorDriver, Integer>() {
        @Override public Integer apply(ExecutorDriver driver) {
          FrameworkMessage frameworkMessage = new FrameworkMessage();
          frameworkMessage.setSlaveId(message.getSlaveId());
          try {
            frameworkMessage.setData(ThriftBinaryCodec.encode(message.getMessage()));
          } catch (CodingException e) {
            LOG.log(Level.SEVERE, "Failed to encode message: " + message.getMessage()
                                  + " intended for slave " + message.getSlaveId());
            return -1;
          }

          int result = driver.sendFrameworkMessage(frameworkMessage);
          if (result != 0) {
            LOG.warning(String.format("Attempt to send executor message returned code %d: %s",
                result, message));
          }

          return result;
        }
      });
    }

    @Override public int sendStatusUpdate(final int taskId, final ScheduleStatus status) {
      Preconditions.checkNotNull(status);

      return doWorkWithDriver(new Function<ExecutorDriver, Integer>() {
        @Override public Integer apply(ExecutorDriver driver) {
          LOG.info("Notifying task " + taskId + " in state " + status);
          return driver.sendStatusUpdate(
              new TaskStatus(taskId, StateTranslator.get(status), EMPTY_MSG));
        }
      });
    }
  }
}

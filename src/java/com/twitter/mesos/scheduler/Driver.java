package com.twitter.mesos.scheduler;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.twitter.mesos.Message;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import mesos.FrameworkMessage;
import mesos.SchedulerDriver;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Defines an interface that is compatible with the mesos SchedulerDriver.
 *
 * @author wfarner
 */
public interface Driver {

  public int sendMessage(Message message);

  public int killTask(int taskId);

  public static class MesosDriverImpl implements Driver {

    private static final Logger LOG = Logger.getLogger(MesosDriverImpl.class.getName());

    private final SchedulerDriver driver;

    @Inject
    public MesosDriverImpl(SchedulerDriver driver) {
      this.driver = Preconditions.checkNotNull(driver);
    }

    @Override public int sendMessage(final Message message) {
      FrameworkMessage frameworkMessage = new FrameworkMessage();
      frameworkMessage.setSlaveId(message.getSlaveId());
      try {
        frameworkMessage.setData(ThriftBinaryCodec.encode(message.getMessage()));
      } catch (ThriftBinaryCodec.CodingException e) {
        LOG.log(Level.SEVERE, "Failed to encode message: " + message.getMessage()
                              + " intended for slave " + message.getSlaveId());
        return -1;
      }

      int result = driver.sendFrameworkMessage(frameworkMessage);
      if (result != 0) {
        LOG.severe(String.format("Attempt to send message failed with code %d [%s]",
            result, message));
      }
      return result;
    }

    @Override public int killTask(final int taskId) {
      int result = driver.killTask(taskId);
      if (result != 0) {
        LOG.severe(String.format("Attempt to kill task %d failed with code %d",
            taskId, result));
      }
      return result;
    }
  }
}

package com.twitter.mesos.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.twitter.mesos.Message;
import com.twitter.mesos.StateTranslator;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.SchedulerMessage;
import mesos.ExecutorInfo;
import mesos.FrameworkMessage;
import mesos.Scheduler;
import mesos.SchedulerDriver;
import mesos.SlaveOffer;
import mesos.TaskDescription;
import mesos.TaskStatus;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Location for communication with the mesos core.
 *
 * @author wfarner
 */
class MesosSchedulerImpl extends Scheduler {
  private static Logger LOG = Logger.getLogger(MesosSchedulerImpl.class.getName());

  static {
    System.loadLibrary("mesos");
  }

  // Stores scheduler state and handles actual scheduling decisions.
  private final SchedulerMain.TwitterSchedulerOptions options;
  private final SchedulerCore schedulerCore;
  private final ExecutorTracker executorTracker;

  @Inject
  public MesosSchedulerImpl(SchedulerMain.TwitterSchedulerOptions options,
      SchedulerCore schedulerCore, ExecutorTracker executorTracker) {
    this.options = Preconditions.checkNotNull(options);
    this.schedulerCore = Preconditions.checkNotNull(schedulerCore);
    this.executorTracker = Preconditions.checkNotNull(executorTracker);
  }

  @Override
  public String getFrameworkName(SchedulerDriver driver) {
    return "TwitterScheduler";
  }

  @Override
  public void slaveLost(SchedulerDriver schedulerDriver, String slaveId) {
    LOG.info("Received notification of lost slave: " + slaveId);
  }

  @Override
  public ExecutorInfo getExecutorInfo(SchedulerDriver driver) {
    return new ExecutorInfo(options.executorPath, new byte[0]);
  }

  @Override
  public void registered(final SchedulerDriver driver, String s) {
    LOG.info("Registered with ID " + s);

    schedulerCore.registered(new Driver() {
      @Override public int sendMessage(Message message) {
        FrameworkMessage frameworkMessage = new FrameworkMessage();
        frameworkMessage.setSlaveId(message.getSlaveId());
        try {
          frameworkMessage.setData(ThriftBinaryCodec.encode(message.getMessage()));
        } catch (ThriftBinaryCodec.CodingException e) {
          LOG.log(Level.SEVERE, "Failed to encode message: " + message.getMessage()
                                + " intended for slave " + message.getSlaveId());
          return -1;
        }

        return driver.sendFrameworkMessage(frameworkMessage);
      }

      @Override public int killTask(int taskId) {
        return driver.killTask(taskId);
      }
    }, s);
  }

  @Override
  public void resourceOffer(SchedulerDriver driver, String offerId, List<SlaveOffer> slaveOffers) {
    List<TaskDescription> scheduledTasks = Lists.newLinkedList();

    try {
      for (SlaveOffer offer : slaveOffers) {
        SchedulerCore.TwitterTask task = schedulerCore.offer(
            offer.getSlaveId(), offer.getHost(), offer.getParams());

        if (task != null) {
          byte[] taskInBytes;
          try {
            taskInBytes = ThriftBinaryCodec.encode(task.task);
          } catch (ThriftBinaryCodec.CodingException e) {
            LOG.log(Level.SEVERE, "Unable to serialize task.", e);
            throw new ScheduleException("Internal error.", e);
          }

          scheduledTasks.add(
              new TaskDescription(task.taskId, task.slaveId, task.taskName, task.params,
                  taskInBytes));
        }
      }
    } catch (ScheduleException e) {
      LOG.log(Level.SEVERE, "Failed to schedule offer.", e);
      return;
    }

    driver.replyToOffer(offerId, scheduledTasks, Maps.<String, String>newHashMap());
  }

  @Override
  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    LOG.info("Received status update for task " + status.getTaskId()
        + " in state " + status.getState());

    Query query = Query.byId(status.getTaskId());

    if (schedulerCore.getTasks(query).isEmpty()) {
      LOG.severe("Failed to find task id " + status.getTaskId());
    } else {
      ScheduleStatus translatedState = StateTranslator.get(status.getState());
      if (translatedState == null) {
        LOG.log(Level.SEVERE, "Failed to look up task state translation for: " + status.getState());
        return;
      }

      schedulerCore.setTaskStatus(query, translatedState);
    }
  }

  @Override
  public void error(SchedulerDriver driver, int code, String message) {
    LOG.severe("Received error message: " + message + " with code " + code);
  }

  @Override
  public void frameworkMessage(SchedulerDriver driver, FrameworkMessage message) {
    if (message.getData() == null) {
      LOG.info("Received empty framework message.");
      return;
    }

    try {
      SchedulerMessage schedulerMsg = ThriftBinaryCodec.decode(SchedulerMessage.class,
          message.getData());
      if (schedulerMsg == null || !schedulerMsg.isSet()) {
        LOG.warning("Received empty scheduler message.");
        return;
      }

      switch (schedulerMsg.getSetField()) {
        case TASK_UPDATE:
          schedulerCore.updateRegisteredTasks(schedulerMsg.getTaskUpdate());
          break;
        case EXECUTOR_STATUS:
          LOG.info("Received executor status update: " + schedulerMsg.getExecutorStatus());
          executorTracker.addStatus(schedulerMsg.getExecutorStatus());
          break;
        default:
          LOG.warning("Received unhandled scheduler message type: " + schedulerMsg.getSetField());
      }
    } catch (ThriftBinaryCodec.CodingException e) {
      LOG.log(Level.SEVERE, "Failed to decode framework message.", e);
    }
  }
}

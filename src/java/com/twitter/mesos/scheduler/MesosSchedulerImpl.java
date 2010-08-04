package com.twitter.mesos.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.twitter.common.base.Closure;
import com.twitter.mesos.FrameworkMessageCodec;
import com.twitter.mesos.SchedulerMessageMux;
import com.twitter.mesos.StateTranslator;
import com.twitter.mesos.codec.Codec;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.gen.ExecutorStatus;
import com.twitter.mesos.gen.RegisteredTaskUpdate;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.SchedulerMessage;
import com.twitter.mesos.gen.SchedulerMessageType;
import com.twitter.mesos.gen.TaskQuery;
import mesos.ExecutorInfo;
import mesos.FrameworkMessage;
import mesos.Scheduler;
import mesos.SchedulerDriver;
import mesos.SlaveOfferVector;
import mesos.StringMap;
import mesos.TaskDescription;
import mesos.TaskDescriptionVector;
import mesos.TaskStatus;
import org.apache.thrift.TBase;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Location for communication with the mesos core.
 *
 * @author wfarner
 */
class MesosSchedulerImpl extends Scheduler {
  private static Logger LOG = Logger.getLogger(MesosSchedulerImpl.class.getName());

  private final Codec<SchedulerMessage, byte[]> schedulerMessageCodec =
      new ThriftBinaryCodec<SchedulerMessage>(SchedulerMessage.class);
  private final Codec<RegisteredTaskUpdate, byte[]> registeredTaskUpdateCodec =
      new ThriftBinaryCodec<RegisteredTaskUpdate>(RegisteredTaskUpdate.class);

  static {
    System.loadLibrary("nexus");
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
  public ExecutorInfo getExecutorInfo(SchedulerDriver driver) {
    return new ExecutorInfo(options.executorPath, new byte[0]);
  }

  @Override
  public void registered(SchedulerDriver driver, String s) {
    LOG.info("Registered with ID " + s);
    schedulerCore.registered(driver, s);
  }

  @Override
  public void resourceOffer(SchedulerDriver driver, String offerId, SlaveOfferVector offers) {
    TaskDescriptionVector newlyScheduledTasks = new TaskDescriptionVector();

    try {
      for (int i = 0; i < offers.size(); i++) {
        TaskDescription taskToSchedule = schedulerCore.offer(offers.get(i));
        if (taskToSchedule != null) {
          newlyScheduledTasks.add(taskToSchedule);
        }
      }
    } catch (ScheduleException e) {
      LOG.log(Level.SEVERE, "Failed to schedule offer.", e);
      return;
    }

    driver.replyToOffer(offerId, newlyScheduledTasks, new StringMap());
  }

  @Override
  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    LOG.info("Received status update for task " + status.getTaskId()
        + " in state " + status.getState());

    TaskQuery query = new TaskQuery();
    query.addToTaskIds(status.getTaskId());

    if (Iterables.isEmpty(schedulerCore.getTasks(query))) {
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

  private final Map<SchedulerMessageType, Closure<TBase>> messageCallbacks = ImmutableMap.of(
      SchedulerMessageType.EXECUTOR_STATUS, new Closure<TBase>() {
        @Override public void execute(TBase item) throws RuntimeException {
          ExecutorStatus status = (ExecutorStatus) item;
          LOG.info("Received executor status update: " + status);
          executorTracker.addStatus(status);
        }
      },
      SchedulerMessageType.REGISTERED_TASK_UPDATE, new Closure<TBase>() {
        @Override public void execute(TBase item) throws RuntimeException {
          schedulerCore.updateRegisteredTasks((RegisteredTaskUpdate) item);
        }
      }
  );

  @Override
  public void frameworkMessage(SchedulerDriver driver, FrameworkMessage message) {
    if (message.getData() == null) {
      LOG.info("Received empty framework message.");
      return;
    }

    try {
      SchedulerMessageMux.demux(
          new FrameworkMessageCodec<SchedulerMessage>(SchedulerMessage.class).decode(message),
          messageCallbacks);
    } catch (Codec.CodingException e) {
      LOG.log(Level.SEVERE, "Failed to decode framework message.", e);
    }
  }
}

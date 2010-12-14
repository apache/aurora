package com.twitter.mesos.scheduler;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import com.twitter.common.stats.Stats;
import com.twitter.mesos.StateTranslator;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.gen.RegisteredTaskUpdate;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.SchedulerMessage;
import mesos.Protos.ExecutorInfo;
import mesos.Protos.FrameworkID;
import mesos.Protos.FrameworkMessage;
import mesos.Protos.OfferID;
import mesos.Protos.Param;
import mesos.Protos.Params;
import mesos.Protos.SlaveID;
import mesos.Protos.SlaveOffer;
import mesos.Protos.TaskDescription;
import mesos.Protos.TaskID;
import mesos.Protos.TaskStatus;
import mesos.Scheduler;
import mesos.SchedulerDriver;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Location for communication with the mesos core.
 *
 * @author wfarner
 */
class MesosSchedulerImpl implements Scheduler {
  private static Logger LOG = Logger.getLogger(MesosSchedulerImpl.class.getName());

  private final SchedulerMain.TwitterSchedulerOptions options;

  // Stores scheduler state and handles actual scheduling decisions.
  private final SchedulerCore schedulerCore;
  private final ExecutorTracker executorTracker;
  private FrameworkID frameworkID;

  @Inject
  public MesosSchedulerImpl(SchedulerMain.TwitterSchedulerOptions options,
      SchedulerCore schedulerCore, ExecutorTracker executorTracker) {
    this.options = checkNotNull(options);
    this.schedulerCore = checkNotNull(schedulerCore);
    this.executorTracker = checkNotNull(executorTracker);
  }

  @Override
  public String getFrameworkName(SchedulerDriver driver) {
    return "TwitterScheduler";
  }

  @Override
  public void slaveLost(SchedulerDriver schedulerDriver, SlaveID slaveId) {
    LOG.info("Received notification of lost slave: " + slaveId);
  }

  @Override
  public ExecutorInfo getExecutorInfo(SchedulerDriver driver) {
    return ExecutorInfo.newBuilder().setUri(options.executorPath).build();
  }

  private static final Function<Entry<String, String>, Param> MAP_TO_PARAM =
      new Function<Entry<String, String>, Param>() {
    @Override public Param apply(Entry<String, String> param) {
      return Param.newBuilder().setKey(param.getKey()).setValue(param.getValue()).build();
    }
  };

  private static Map<String, String> asMap(Params params) {
    Map<String, String> map = Maps.newHashMap();
    for (Param param : params.getParamList()) {
      String existing = map.put(param.getKey(), param.getValue());
      if (existing != null) {
        LOG.severe("Params key collision on " + param.getKey() + ", lost value " + existing);
      }
    }

    return map;
  }

  @Override public void registered(SchedulerDriver schedulerDriver, FrameworkID frameworkID) {
    LOG.info("Registered with ID " + frameworkID);
    this.frameworkID = frameworkID;
    schedulerCore.registered(frameworkID.getValue());
  }

  @Override
  public void resourceOffer(SchedulerDriver driver, OfferID offerId, List<SlaveOffer> slaveOffers) {
    Preconditions.checkState(frameworkID != null, "Must be registered before receiving offers.");

    List<TaskDescription> scheduledTasks = Lists.newLinkedList();

    try {
      for (SlaveOffer offer : slaveOffers) {
        SchedulerCore.TwitterTask task = schedulerCore.offer(
            offer.getSlaveId().getValue(), offer.getHostname(), asMap(offer.getParams()));

        if (task != null) {
          byte[] taskInBytes;
          try {
            taskInBytes = ThriftBinaryCodec.encode(task.task);
          } catch (ThriftBinaryCodec.CodingException e) {
            LOG.log(Level.SEVERE, "Unable to serialize task.", e);
            throw new ScheduleException("Internal error.", e);
          }

          List<Param> params = ImmutableList.copyOf(Iterables.transform(task.params.entrySet(),
              MAP_TO_PARAM));

          scheduledTasks.add(TaskDescription.newBuilder()
              .setName(task.taskName)
              .setTaskId(TaskID.newBuilder().setValue(task.taskId))
              .setSlaveId(SlaveID.newBuilder().setValue(task.slaveId))
              .setParams(Params.newBuilder().addAllParam(params).build())
              .setData(ByteString.copyFrom(taskInBytes))
              .build());
        }
      }
    } catch (ScheduleException e) {
      LOG.log(Level.SEVERE, "Failed to schedule offer.", e);
      return;
    }

    driver.replyToOffer(offerId, scheduledTasks);
  }

  @Override
  public void offerRescinded(SchedulerDriver schedulerDriver, OfferID offerID) {
    LOG.info("Offer rescinded but we don't care " + offerID);
  }

  @Override
  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    LOG.info("Received status update for task " + status.getTaskId()
        + " in state " + status.getState());

    Query query = Query.byId(status.getTaskId().getValue());

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
          message.getData().toByteArray());
      if (schedulerMsg == null || !schedulerMsg.isSet()) {
        LOG.warning("Received empty scheduler message.");
        return;
      }

      switch (schedulerMsg.getSetField()) {
        case TASK_UPDATE:
          RegisteredTaskUpdate update = schedulerMsg.getTaskUpdate();
          vars.registeredTaskUpdates.incrementAndGet();
          LOG.info("Received registered task update from " + update.getSlaveHost());
          schedulerCore.updateRegisteredTasks(update);
          break;
        case EXECUTOR_STATUS:
          LOG.info("Received executor status update: " + schedulerMsg.getExecutorStatus());
          vars.executorStatusUpdates.incrementAndGet();
          executorTracker.addStatus(schedulerMsg.getExecutorStatus());
          break;
        default:
          LOG.warning("Received unhandled scheduler message type: " + schedulerMsg.getSetField());
      }
    } catch (ThriftBinaryCodec.CodingException e) {
      LOG.log(Level.SEVERE, "Failed to decode framework message.", e);
    }
  }

  private class Vars {
    final AtomicLong executorStatusUpdates = Stats.exportLong("executor_status_updates");
    final AtomicLong registeredTaskUpdates = Stats.exportLong("executor_registered_task_updates");
  }
  private Vars vars = new Vars();
}

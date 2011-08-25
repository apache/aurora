package com.twitter.mesos.scheduler;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.protobuf.ByteString;

import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.SlaveOffer;
import org.apache.mesos.Protos.TaskDescription;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.thrift.TBase;

import com.twitter.common.application.Lifecycle;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.base.Closure;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.mesos.ExecutorKey;
import com.twitter.mesos.StateTranslator;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.comm.ExecutorMessage;
import com.twitter.mesos.gen.comm.RegisteredTaskUpdate;
import com.twitter.mesos.gen.comm.RestartExecutor;
import com.twitter.mesos.gen.comm.SchedulerMessage;
import com.twitter.mesos.gen.comm.StateUpdateResponse;
import com.twitter.mesos.scheduler.sync.ExecutorWatchdog;
import com.twitter.mesos.scheduler.sync.ExecutorWatchdog.UpdateRequest;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Location for communication with the mesos core.
 *
 * @author William Farner
 */
class MesosSchedulerImpl implements Scheduler {
  private static final Logger LOG = Logger.getLogger(MesosSchedulerImpl.class.getName());

  private static final Amount<Long, Time> MAX_REGISTRATION_DELAY = Amount.of(1L, Time.MINUTES);

  @CmdLine(name = "executor_poll_interval", help = "Interval between executor update requests.")
  private static final Arg<Amount<Long, Time>> EXECUTOR_POLL_INTERVAL =
      Arg.create(Amount.of(10L, Time.SECONDS));

  // Stores scheduler state and handles actual scheduling decisions.
  private final SchedulerCore schedulerCore;

  private final ExecutorTracker executorTracker;
  private volatile FrameworkID frameworkID = null;
  private final ExecutorInfo executorInfo;
  private final ExecutorWatchdog executorWatchdog;

  @Inject
  public MesosSchedulerImpl(SchedulerCore schedulerCore, ExecutorTracker executorTracker,
      ExecutorInfo executorInfo, final Lifecycle lifecycle, ExecutorWatchdog executorWatchdog) {
    this.schedulerCore = checkNotNull(schedulerCore);
    this.executorTracker = checkNotNull(executorTracker);
    this.executorInfo = checkNotNull(executorInfo);
    this.executorWatchdog = checkNotNull(executorWatchdog);

    // TODO(William Farner): Clean this up.
    Thread registrationChecker = new Thread() {
      @Override public void run() {
        try {
          Thread.sleep(MAX_REGISTRATION_DELAY.as(Time.MILLISECONDS));
        } catch (InterruptedException e) {
          LOG.log(Level.WARNING, "Delayed registration check interrupted.", e);
          Thread.currentThread().interrupt();
        }

        if (frameworkID == null) {
          LOG.severe("Framework has not been registered within the tolerated delay, quitting.");
          lifecycle.shutdown();
        }
      }
    };
    registrationChecker.setDaemon(true);
    registrationChecker.start();
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
    return executorInfo;
  }

  private static void sendMessage(SchedulerDriver driver, ExecutorMessage message, SlaveID slave,
      ExecutorID executor) {
    byte[] data;
    try {
      data = ThriftBinaryCodec.encode(message);
    } catch (CodingException e) {
      LOG.log(Level.SEVERE, "Failed to send restart request.", e);
      return;
    }

    LOG.info(String.format("Attempting to send message to %s/%s - %s",
        slave.getValue(), executor.getValue(), message));
    int result = driver.sendFrameworkMessage(slave, executor, data);
    if (result != 0) {
      LOG.severe(String.format("Attempt to send message failed with code %d [%s]",
          result, message));
    } else {
      LOG.info("Message successfully sent");
    }
  }

  @Override
  public void registered(final SchedulerDriver driver, FrameworkID frameworkID) {
    LOG.info("Registered with ID " + frameworkID);
    this.frameworkID = frameworkID;
    schedulerCore.registered(frameworkID.getValue());

    // TODO(wfarner): Build this into ExecutorWatchdog.
    executorTracker.start(new Closure<String>() {
      @Override public void execute(String slaveId) {
        LOG.info("Sending restart request to executor " + slaveId);
        ExecutorMessage message = new ExecutorMessage();
        message.setRestartExecutor(new RestartExecutor());

        sendMessage(driver, message, SlaveID.newBuilder().setValue(slaveId).build(),
            executorInfo.getExecutorId());
      }
    });

    executorWatchdog.startRequestLoop(EXECUTOR_POLL_INTERVAL.get(),
        new Closure<UpdateRequest>() {
          @Override public void execute(UpdateRequest request) {
            ExecutorMessage message = new ExecutorMessage();
            message.setStateUpdateRequest(request.request);
            sendMessage(driver, message, request.executor.slave, request.executor.executor);
          }
        });
  }

  @Override
  public void resourceOffer(SchedulerDriver driver, OfferID offerId, List<SlaveOffer> slaveOffers) {
    Preconditions.checkState(frameworkID != null, "Must be registered before receiving offers.");

    List<TaskDescription> scheduledTasks = Lists.newLinkedList();

    try {
      for (SlaveOffer offer : slaveOffers) {
        log(Level.FINE, "Received offer: %s", offer);
        SchedulerCore.TwitterTask task = schedulerCore.offer(offer, executorInfo.getExecutorId());

        if (task != null) {
          byte[] taskInBytes;
          try {
            taskInBytes = ThriftBinaryCodec.encode(task.task);
          } catch (ThriftBinaryCodec.CodingException e) {
            LOG.log(Level.SEVERE, "Unable to serialize task.", e);
            throw new ScheduleException("Internal error.", e);
          }

          TaskDescription assignedTask =
              TaskDescription.newBuilder().setName(task.taskName)
                  .setTaskId(TaskID.newBuilder().setValue(task.taskId))
                  .setSlaveId(SlaveID.newBuilder().setValue(task.slaveId))
                  .addAllResources(task.resources)
                  .setData(ByteString.copyFrom(taskInBytes))
                  .build();
          log(Level.FINE, "Accepted offer: ", assignedTask);
          scheduledTasks.add(assignedTask);
        }
      }
    } catch (ScheduleException e) {
      LOG.log(Level.SEVERE, "Failed to schedule offer.", e);
      return;
    }

    if (!scheduledTasks.isEmpty()) {
      LOG.info(String.format("Accepting offer %s, to launch tasks %s", offerId.getValue(),
          ImmutableSet.copyOf(Iterables.transform(scheduledTasks, TO_STRING))));
    }

    driver.replyToOffer(offerId, scheduledTasks);
  }

  private static final Function<TaskDescription, String> TO_STRING =
      new Function<TaskDescription, String>() {
        @Override public String apply(TaskDescription task) {
          return task.getTaskId().getValue() + " on " + task.getSlaveId().getValue();
        }
      };

  @Override
  public void offerRescinded(SchedulerDriver schedulerDriver, OfferID offerID) {
    LOG.info("Offer rescinded but we don't care " + offerID);
  }

  @Override
  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    String info = status.hasData() ? status.getData().toStringUtf8() : null;
    String infoMsg = info != null ? " with info " + info : "";
    LOG.info("Received status update for task " + status.getTaskId().getValue()
        + " in state " + status.getState() + infoMsg);

    Query query = Query.byId(status.getTaskId().getValue());

    if (schedulerCore.getTasks(query).isEmpty()) {
      LOG.severe("Failed to find task id " + status.getTaskId());
    } else {
      ScheduleStatus translatedState = StateTranslator.get(status.getState());
      if (translatedState == null) {
        LOG.log(Level.SEVERE, "Failed to look up task state translation for: " + status.getState());
        return;
      }

      schedulerCore.setTaskStatus(query, translatedState, info);
    }
  }

  @Override
  public void error(SchedulerDriver driver, int code, String message) {
    LOG.severe("Received error message: " + message + " with code " + code);
    // TODO(William Farner): Exit cleanly.
    System.exit(1);
  }

  @Override
  public void frameworkMessage(SchedulerDriver driver, SlaveID slave, ExecutorID executor,
      byte[] data) {

    if (data == null) {
      LOG.info("Received empty framework message.");
      return;
    }

    try {
      SchedulerMessage schedulerMsg = ThriftBinaryCodec.decode(SchedulerMessage.class, data);
      if (schedulerMsg == null || !schedulerMsg.isSet()) {
        LOG.warning("Received empty scheduler message.");
        return;
      }

      switch (schedulerMsg.getSetField()) {
        case TASK_UPDATE:
          RegisteredTaskUpdate update = schedulerMsg.getTaskUpdate();
          LOG.info("Ignoring registered task from " + update.getSlaveHost());
          break;

        case EXECUTOR_STATUS:
          vars.executorStatusUpdates.incrementAndGet();
          executorTracker.addStatus(schedulerMsg.getExecutorStatus());
          break;

        case STATE_UPDATE_RESPONSE:
          StateUpdateResponse stateUpdate = schedulerMsg.getStateUpdateResponse();
          ExecutorKey executorKey = new ExecutorKey(slave, executor, stateUpdate.getSlaveHost());
          LOG.info("Applying state update " + stateUpdate);
          schedulerCore.stateUpdate(executorKey, stateUpdate);
          executorWatchdog.stateUpdated(executorKey,
              stateUpdate.getExecutorUUID(), stateUpdate.getPosition());
          break;

        default:
        LOG.warning("Received unhandled scheduler message type: " + schedulerMsg.getSetField());
      }
    } catch (ThriftBinaryCodec.CodingException e) {
      LOG.log(Level.SEVERE, "Failed to decode framework message.", e);
    }
  }

  private static void log(Level level, String message, Object... args) {
    if (LOG.isLoggable(level)) {
      LOG.log(level, String.format(message, args));
    }
  }

  private static class Vars {
    final AtomicLong executorStatusUpdates = Stats.exportLong("executor_status_updates");
  }
  private final Vars vars = new Vars();
}

package com.twitter.mesos.scheduler;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import com.twitter.common.application.Lifecycle;
import com.twitter.common.base.Closure;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.mesos.StateTranslator;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.gen.ExecutorMessage;
import com.twitter.mesos.gen.RegisteredTaskUpdate;
import com.twitter.mesos.gen.RestartExecutor;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.SchedulerMessage;
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

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Location for communication with the mesos core.
 *
 * @author William Farner
 */
class MesosSchedulerImpl implements Scheduler {
  private static final Logger LOG = Logger.getLogger(MesosSchedulerImpl.class.getName());

  private static final String TWITTER_EXECUTOR_ID = "twitter";

  private static final Amount<Long, Time> MAX_REGISTRATION_DELAY = Amount.of(1L, Time.MINUTES);

  /**
   * Binding annotation for the path to the executor binary.
   */
  @BindingAnnotation
  @Target({FIELD, PARAMETER, METHOD}) @Retention(RUNTIME)
  public @interface ExecutorPath {}

  // Stores scheduler state and handles actual scheduling decisions.
  private final SchedulerCore schedulerCore;

  private final ExecutorTracker executorTracker;
  private volatile FrameworkID frameworkID = null;
  private final String executorPath;

  @Inject
  public MesosSchedulerImpl(SchedulerCore schedulerCore, ExecutorTracker executorTracker,
      @ExecutorPath String executorPath, final Lifecycle lifecycle) {
    this.schedulerCore = checkNotNull(schedulerCore);
    this.executorTracker = checkNotNull(executorTracker);
    this.executorPath = checkNotBlank(executorPath);

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

    return ExecutorInfo.newBuilder().setUri(executorPath)
        .setExecutorId(ExecutorID.newBuilder().setValue(TWITTER_EXECUTOR_ID))
        .build();
  }

  @Override
  public void registered(final SchedulerDriver driver, FrameworkID frameworkID) {
    LOG.info("Registered with ID " + frameworkID);
    this.frameworkID = frameworkID;
    schedulerCore.registered(frameworkID.getValue());

    executorTracker.start(new Closure<String>() {
      @Override public void execute(String slaveId) {
        LOG.info("Sending restart request to executor " + slaveId);
        ExecutorMessage message = new ExecutorMessage();
        message.setRestartExecutor(new RestartExecutor());

        byte[] data;
        try {
          data = ThriftBinaryCodec.encode(message);
        } catch (CodingException e) {
          LOG.log(Level.SEVERE, "Failed to send restart request.", e);
          return;
        }

        LOG.info("Attempting to send message from scheduler to " + slaveId + " - " + message);
        int result = driver.sendFrameworkMessage(SlaveID.newBuilder().setValue(slaveId).build(),
            ExecutorID.newBuilder().setValue(TWITTER_EXECUTOR_ID).build(), data);
        if (result != 0) {
          LOG.severe(String.format("Attempt to send message failed with code %d [%s]",
              result, message));
        } else {
          LOG.info("Message successfully sent");
        }
      }
    });
  }

  @Override
  public void resourceOffer(SchedulerDriver driver, OfferID offerId, List<SlaveOffer> slaveOffers) {
    Preconditions.checkState(frameworkID != null, "Must be registered before receiving offers.");

    List<TaskDescription> scheduledTasks = Lists.newLinkedList();

    try {
      for (SlaveOffer offer : slaveOffers) {
        SchedulerCore.TwitterTask task = schedulerCore.offer(offer);

        if (task != null) {
          byte[] taskInBytes;
          try {
            taskInBytes = ThriftBinaryCodec.encode(task.task);
          } catch (ThriftBinaryCodec.CodingException e) {
            LOG.log(Level.SEVERE, "Unable to serialize task.", e);
            throw new ScheduleException("Internal error.", e);
          }

          scheduledTasks.add(TaskDescription.newBuilder()
              .setName(task.taskName)
              .setTaskId(TaskID.newBuilder().setValue(task.taskId))
              .setSlaveId(SlaveID.newBuilder().setValue(task.slaveId))
              .addAllResources(task.resources)
              .setData(ByteString.copyFrom(taskInBytes))
              .build());
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
    LOG.info("Received status update for task " + status.getTaskId()
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
          vars.registeredTaskUpdates.incrementAndGet();
          schedulerCore.updateRegisteredTasks(update);
          break;
        case EXECUTOR_STATUS:
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

  private static class Vars {
    final AtomicLong executorStatusUpdates = Stats.exportLong("executor_status_updates");
    final AtomicLong registeredTaskUpdates = Stats.exportLong("executor_registered_task_updates");
  }
  private final Vars vars = new Vars();
}

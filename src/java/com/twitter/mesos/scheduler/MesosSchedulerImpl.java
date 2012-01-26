package com.twitter.mesos.scheduler;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.protobuf.ByteString;

import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskDescription;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import com.twitter.common.application.Lifecycle;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.mesos.ExecutorKey;
import com.twitter.mesos.StateTranslator;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.comm.SchedulerMessage;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Location for communication with the mesos core.
 *
 * @author William Farner
 */
public class MesosSchedulerImpl implements Scheduler {
  private static final Logger LOG = Logger.getLogger(MesosSchedulerImpl.class.getName());

  private static final Amount<Long, Time> MAX_REGISTRATION_DELAY = Amount.of(1L, Time.MINUTES);

  // TODO(wickman):  This belongs in SchedulerModule eventually.
  @NotNull
  @CmdLine(name = "thermos_executor_path", help = "Path to the thermos executor launch script.")
  private static final Arg<String> THERMOS_EXECUTOR_PATH = Arg.create();

  private final SlaveMapper slaveMapper;

  // Stores scheduler state and handles actual scheduling decisions.
  private final SchedulerCore schedulerCore;

  private volatile FrameworkID frameworkID = null;
  private final ExecutorInfo executorInfo;

  private final AtomicInteger registeredFlag = Stats.exportInt("framework_registered");

  @Inject
  public MesosSchedulerImpl(SchedulerCore schedulerCore,
      ExecutorInfo executorInfo,
      final Lifecycle lifecycle,
      SlaveMapper slaveMapper) {
    this.schedulerCore = checkNotNull(schedulerCore);
    this.executorInfo = checkNotNull(executorInfo);
    this.slaveMapper = checkNotNull(slaveMapper);

    // TODO(William Farner): Clean this up.
    LOG.info(String.format("Waiting up to %s for scheduler registration.", MAX_REGISTRATION_DELAY));
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
  public void slaveLost(SchedulerDriver schedulerDriver, SlaveID slaveId) {
    LOG.info("Received notification of lost slave: " + slaveId);
  }

  @Override
  public void registered(final SchedulerDriver driver, FrameworkID frameworkID) {
    LOG.info("Registered with ID " + frameworkID);
    registeredFlag.set(1);
    this.frameworkID = frameworkID;
    try {
      schedulerCore.registered(frameworkID.getValue());
    } catch (SchedulerException e) {
      LOG.log(Level.SEVERE, "Problem registering", e);
      driver.abort();
    }
  }

  TaskDescription twitterTaskToMesosTask(SchedulerCore.TwitterTask twitterTask)
      throws SchedulerException {

    checkNotNull(twitterTask);
    byte[] taskInBytes;
    try {
      taskInBytes = ThriftBinaryCodec.encode(twitterTask.task);
    } catch (ThriftBinaryCodec.CodingException e) {
      LOG.log(Level.SEVERE, "Unable to serialize task.", e);
      throw new SchedulerException("Internal error.", e);
    }

    log(Level.INFO, "Setting task resources to %s", twitterTask.resources);
    TaskDescription.Builder assignedTaskBuilder =
        TaskDescription.newBuilder().setName(twitterTask.taskName)
            .setTaskId(TaskID.newBuilder().setValue(twitterTask.taskId))
            .setSlaveId(SlaveID.newBuilder().setValue(twitterTask.slaveId))
            .addAllResources(twitterTask.resources)
            .setData(ByteString.copyFrom(taskInBytes));
    if (twitterTask.isThermosTask()) {
      assignedTaskBuilder.setExecutor(ExecutorInfo.newBuilder()
          .setExecutorId(ExecutorID.newBuilder().setValue(String.format("%s%s",
              ExecutorKey.THERMOS_EXECUTOR_ID_PREFIX,
              twitterTask.taskId)))
          .setUri(THERMOS_EXECUTOR_PATH.get()));
    }
    return assignedTaskBuilder.build();
  }

  @Timed("scheduler_resource_offers")
  @Override
  public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
    Preconditions.checkState(frameworkID != null, "Must be registered before receiving offers.");
    for (Offer offer : offers) {
      log(Level.FINE, "Received offer: %s", offer);
      slaveMapper.addSlave(offer.getHostname(), offer.getSlaveId());

      List<TaskDescription> scheduledTasks = Collections.emptyList();
      try {
        SchedulerCore.TwitterTask task = schedulerCore.offer(offer, executorInfo.getExecutorId());
        if (task != null) {
          TaskDescription assignedTask = twitterTaskToMesosTask(task);
          scheduledTasks = ImmutableList.of(assignedTask);
        }

        if (!scheduledTasks.isEmpty()) {
          LOG.info(String.format("Accepting offer %s, to launch tasks %s", offer.getId().getValue(),
              ImmutableSet.copyOf(Iterables.transform(scheduledTasks, TO_STRING))));
        }
      } catch (SchedulerException e) {
        LOG.log(Level.WARNING, "Failed to schedule offers.", e);
        vars.failedOffers.incrementAndGet();
      } catch (ScheduleException e) {
        LOG.log(Level.WARNING, "Failed to schedule offers.", e);
        vars.failedOffers.incrementAndGet();
      }

      // For a given offer, if we fail to process it we can always launch with an empty set of tasks
      // to signal the core we have processed the offer and just not used any of it.
      driver.launchTasks(offer.getId(), scheduledTasks);
    }
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

  @Timed("scheduler_status_update")
  @Override
  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    String info = status.hasData() ? status.getData().toStringUtf8() : null;
    String infoMsg = info != null ? " with info " + info : "";
    String coreMsg = status.hasMessage() ? " with core message " + status.getMessage() : "";
    LOG.info("Received status update for task " + status.getTaskId().getValue()
        + " in state " + status.getState() + infoMsg + coreMsg);

    Query query = Query.byId(status.getTaskId().getValue());

    try {
      if (schedulerCore.getTasks(query).isEmpty()) {
        LOG.severe("Failed to find task id " + status.getTaskId());
      } else {
        ScheduleStatus translatedState = StateTranslator.get(status.getState());
        if (translatedState == null) {
          LOG.log(Level.SEVERE,
              "Failed to look up task state translation for: " + status.getState());
          return;
        }

        schedulerCore.setTaskStatus(query, translatedState, info);
      }
    } catch (SchedulerException e) {
      // We assume here that a subsequent RegisteredTaskUpdate will inform us of this tasks status.
      LOG.log(Level.WARNING, "Failed to update status for: " + status, e);
      vars.failedStatusUpdates.incrementAndGet();
    }
  }

  @Override
  public void error(SchedulerDriver driver, int code, String message) {
    LOG.severe("Received error message: " + message + " with code " + code);
    // TODO(William Farner): Exit cleanly.
    System.exit(1);
  }

  @Timed("scheduler_framework_message")
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
        case DELETED_TASKS:
          schedulerCore.tasksDeleted(schedulerMsg.getDeletedTasks().getTaskIds());
          break;

        default:
          LOG.warning("Received unhandled scheduler message type: " + schedulerMsg.getSetField());
          break;
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
    final AtomicLong failedExecutorStatusUpdates =
        Stats.exportLong("executor_status_updates_failed");
    final AtomicLong failedOffers = Stats.exportLong("scheduler_failed_offers");
    final AtomicLong failedStatusUpdates = Stats.exportLong("scheduler_failed_status_updates");
  }
  private final Vars vars = new Vars();

  /**
   * Maintains a mapping between hosts and slave ids.
   */
  public static interface SlaveHosts {

    /**
     * Gets the slave ID associated with a host name.
     *
     * @param host The host to look up.
     * @return The host's slave ID, or {@code null} if the host was not found.
     */
    SlaveID getSlave(String host);

    /**
     * Gets all slave ID mappings.
     *
     * @return all string to slave ID mappings.
     */
    Map<String, SlaveID> getSlaves();
  }

  /**
   * Records slave host names and their associated slave IDs.
   *
   * We accept a trivial memory "leak" by not removing when a slave machine is decommissioned.
   */
  public static interface SlaveMapper {

    /**
     * Records a host to slave ID mapping.
     *
     * @param host Host name.
     * @param slaveId Slave ID.
     */
    void addSlave(String host, SlaveID slaveId);
  }

  static class SlaveHostsImpl implements SlaveHosts, SlaveMapper {
    private final Map<String, SlaveID> executorSlaves = Maps.newConcurrentMap();

    @Override
    public SlaveID getSlave(String host) {
      return executorSlaves.get(host);
    }

    @Override
    public void addSlave(String host, SlaveID slaveId) {
      executorSlaves.put(host, slaveId);
    }

    @Override
    public Map<String, SlaveID> getSlaves() {
      return ImmutableMap.copyOf(executorSlaves);
    }
  }
}

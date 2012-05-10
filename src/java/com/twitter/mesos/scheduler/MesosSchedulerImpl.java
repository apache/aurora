package com.twitter.mesos.scheduler;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskDescription;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import com.twitter.common.application.Lifecycle;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.mesos.JNICallback;
import com.twitter.mesos.codec.ThriftBinaryCodec;
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

  private final AtomicLong resourceOffers = Stats.exportLong("scheduler_resource_offers");
  private final AtomicLong failedOffers = Stats.exportLong("scheduler_failed_offers");
  private final AtomicLong failedStatusUpdates = Stats.exportLong("scheduler_status_updates");

  private final List<TaskLauncher> taskLaunchers;
  private final SlaveMapper slaveMapper;

  // Stores scheduler state and handles actual scheduling decisions.
  private final SchedulerCore schedulerCore;
  private final Lifecycle lifecycle;
  private volatile FrameworkID frameworkID = null;
  private final AtomicInteger registeredFlag = Stats.exportInt("framework_registered");

  /**
   * Creates a new mesos scheduler.
   *
   * @param schedulerCore Core scheduler.
   * @param lifecycle Application lifecycle manager.
   * @param taskLaunchers Task launchers.
   * @param slaveMapper Slave information accumulator.
   */
  @Inject
  public MesosSchedulerImpl(SchedulerCore schedulerCore,
      final Lifecycle lifecycle,
      List<TaskLauncher> taskLaunchers,
      SlaveMapper slaveMapper) {
    this.schedulerCore = checkNotNull(schedulerCore);
    this.lifecycle = checkNotNull(lifecycle);
    this.taskLaunchers = checkNotNull(taskLaunchers);
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

  @JNICallback
  @Override
  public void slaveLost(SchedulerDriver schedulerDriver, SlaveID slaveId) {
    LOG.info("Received notification of lost slave: " + slaveId);
  }

  @JNICallback
  @Override
  public void registered(final SchedulerDriver driver, FrameworkID fId) {
    LOG.info("Registered with ID " + fId);
    registeredFlag.set(1);
    this.frameworkID = fId;
    try {
      schedulerCore.registered(frameworkID.getValue());
    } catch (SchedulerException e) {
      LOG.log(Level.SEVERE, "Problem registering", e);
      driver.abort();
    }
  }

  private static boolean fitsInOffer(TaskDescription task, Offer offer) {
    return Resources.from(offer).greaterThanOrEqual(Resources.from(task.getResourcesList()));
  }

  @JNICallback
  @Timed("scheduler_resource_offers")
  @Override
  public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
    Preconditions.checkState(frameworkID != null, "Must be registered before receiving offers.");
    for (Offer offer : offers) {
      log(Level.FINE, "Received offer: %s", offer);
      resourceOffers.incrementAndGet();

      slaveMapper.addSlave(offer.getHostname(), offer.getSlaveId());

      Optional<TaskDescription> task = Optional.absent();

      for (TaskLauncher launcher : taskLaunchers) {
        try {
          task = launcher.createTask(offer);
        } catch (SchedulerException e) {
          LOG.log(Level.WARNING, "Failed to schedule offers.", e);
          failedOffers.incrementAndGet();
        }

        if (task.isPresent()) {
          if (fitsInOffer(task.get(), offer)) {
            driver.launchTasks(offer.getId(), ImmutableList.of(task.get()));
            break;
          } else {
            LOG.warning("Insufficient resources to launch task " + task);
            task = Optional.absent();
          }
        }
      }

      if (!task.isPresent()) {
        // For a given offer, if we fail to process it we can always launch with an empty set of
        // tasks to signal the core we have processed the offer and just not used any of it.
        driver.launchTasks(offer.getId(), ImmutableList.<TaskDescription>of());
      }
    }
  }

  @JNICallback
  @Override
  public void offerRescinded(SchedulerDriver schedulerDriver, OfferID offerID) {
    LOG.info("Offer rescinded but we don't care " + offerID);
  }

  @JNICallback
  @Timed("scheduler_status_update")
  @Override
  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    String info = status.hasData() ? status.getData().toStringUtf8() : null;
    String infoMsg = info != null ? " with info " + info : "";
    String coreMsg = status.hasMessage() ? " with core message " + status.getMessage() : "";
    LOG.info("Received status update for task " + status.getTaskId().getValue()
        + " in state " + status.getState() + infoMsg + coreMsg);

    for (TaskLauncher launcher : taskLaunchers) {
      if (launcher.statusUpdate(status)) {
        return;
      }
    }

    LOG.warning("Unhandled status update " + status);
    failedStatusUpdates.incrementAndGet();
  }

  @JNICallback
  @Override
  public void error(SchedulerDriver driver, int code, String message) {
    LOG.severe("Received error message: " + message + " with code " + code);
    lifecycle.shutdown();
  }

  @JNICallback
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

  /**
   * Maintains a mapping between hosts and slave ids.
   */
  public interface SlaveHosts {

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
  public interface SlaveMapper {

    /**
     * Records a host to slave ID mapping.
     *
     * @param host Host name.
     * @param slaveId Slave ID.
     */
    void addSlave(String host, SlaveID slaveId);
  }

  /**
   * In-memory slave host mapper.
   */
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

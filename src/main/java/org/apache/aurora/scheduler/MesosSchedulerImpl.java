/**
 * Copyright 2013 Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.twitter.common.application.Lifecycle;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.stats.Stats;

import org.apache.aurora.GuiceUtils.AllowUnchecked;
import org.apache.aurora.codec.ThriftBinaryCodec;
import org.apache.aurora.gen.comm.SchedulerMessage;
import org.apache.aurora.scheduler.base.Conversions;
import org.apache.aurora.scheduler.base.SchedulerException;
import org.apache.aurora.scheduler.configuration.Resources;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent.DriverDisconnected;
import org.apache.aurora.scheduler.events.PubsubEvent.DriverRegistered;
import org.apache.aurora.scheduler.state.SchedulerCore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Location for communication with mesos.
 */
class MesosSchedulerImpl implements Scheduler {
  private static final Logger LOG = Logger.getLogger(MesosSchedulerImpl.class.getName());

  private final AtomicLong resourceOffers = Stats.exportLong("scheduler_resource_offers");
  private final AtomicLong failedOffers = Stats.exportLong("scheduler_failed_offers");
  private final AtomicLong failedStatusUpdates = Stats.exportLong("scheduler_status_updates");
  private final AtomicLong frameworkDisconnects =
      Stats.exportLong("scheduler_framework_disconnects");
  private final AtomicLong frameworkReregisters =
      Stats.exportLong("scheduler_framework_reregisters");
  private final AtomicLong lostExecutors = Stats.exportLong("scheduler_lost_executors");

  private final List<TaskLauncher> taskLaunchers;

  private final Storage storage;
  private final SchedulerCore schedulerCore;
  private final Lifecycle lifecycle;
  private final EventSink eventSink;
  private volatile boolean registered = false;

  /**
   * Creates a new scheduler.
   *
   * @param schedulerCore Core scheduler.
   * @param lifecycle Application lifecycle manager.
   * @param taskLaunchers Task launchers.
   */
  @Inject
  public MesosSchedulerImpl(
      Storage storage,
      SchedulerCore schedulerCore,
      final Lifecycle lifecycle,
      List<TaskLauncher> taskLaunchers,
      EventSink eventSink) {

    this.storage = checkNotNull(storage);
    this.schedulerCore = checkNotNull(schedulerCore);
    this.lifecycle = checkNotNull(lifecycle);
    this.taskLaunchers = checkNotNull(taskLaunchers);
    this.eventSink = checkNotNull(eventSink);
  }

  @Override
  public void slaveLost(SchedulerDriver schedulerDriver, SlaveID slaveId) {
    LOG.info("Received notification of lost slave: " + slaveId);
  }

  @Override
  public void registered(
      SchedulerDriver driver,
      final FrameworkID frameworkId,
      MasterInfo masterInfo) {

    LOG.info("Registered with ID " + frameworkId + ", master: " + masterInfo);

    storage.write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getSchedulerStore().saveFrameworkId(frameworkId.getValue());
      }
    });
    registered = true;
    eventSink.post(new DriverRegistered());
  }

  @Override
  public void disconnected(SchedulerDriver schedulerDriver) {
    LOG.warning("Framework disconnected.");
    frameworkDisconnects.incrementAndGet();
    eventSink.post(new DriverDisconnected());
  }

  @Override
  public void reregistered(SchedulerDriver schedulerDriver, MasterInfo masterInfo) {
    LOG.info("Framework re-registered with master " + masterInfo);
    frameworkReregisters.incrementAndGet();
  }

  private static boolean fitsInOffer(TaskInfo task, Offer offer) {
    return Resources.from(offer).greaterThanOrEqual(Resources.from(task.getResourcesList()));
  }

  @Timed("scheduler_resource_offers")
  @Override
  public void resourceOffers(SchedulerDriver driver, final List<Offer> offers) {
    Preconditions.checkState(registered, "Must be registered before receiving offers.");

    // Store all host attributes in a single write operation to prevent other threads from
    // securing the storage lock between saves.  We also save the host attributes before passing
    // offers elsewhere to ensure that host attributes are available before attempting to
    // schedule tasks associated with offers.
    // TODO(wfarner): Reconsider the requirements here, we might be able to save host offers
    //                asynchronously and augment the task scheduler to skip over offers when the
    //                host attributes cannot be found. (AURORA-116)
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        for (final Offer offer : offers) {
          storeProvider.getAttributeStore().saveHostAttributes(Conversions.getAttributes(offer));
        }
      }
    });

    for (Offer offer : offers) {
      log(Level.FINE, "Received offer: %s", offer);
      resourceOffers.incrementAndGet();

      // Ordering of task launchers is important here, since offers are consumed greedily.
      // TODO(William Farner): Refactor this area of code now that the primary task launcher
      // is asynchronous.
      for (TaskLauncher launcher : taskLaunchers) {
        Optional<TaskInfo> task = Optional.absent();
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
          }
        }
      }
    }
  }

  @Override
  public void offerRescinded(SchedulerDriver schedulerDriver, OfferID offerId) {
    LOG.info("Offer rescinded: " + offerId);
    for (TaskLauncher launcher : taskLaunchers) {
      launcher.cancelOffer(offerId);
    }
  }

  @AllowUnchecked
  @Timed("scheduler_status_update")
  @Override
  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    String info = status.hasData() ? status.getData().toStringUtf8() : null;
    String infoMsg = info != null ? " with info " + info : "";
    String coreMsg = status.hasMessage() ? " with core message " + status.getMessage() : "";
    LOG.info("Received status update for task " + status.getTaskId().getValue()
        + " in state " + status.getState() + infoMsg + coreMsg);

    try {
      for (TaskLauncher launcher : taskLaunchers) {
        if (launcher.statusUpdate(status)) {
          return;
        }
      }
    } catch (SchedulerException e) {
      LOG.log(Level.SEVERE, "Status update failed due to scheduler exception: " + e, e);
      // We re-throw the exception here in an effort to NACK the status update and trigger an
      // abort of the driver.  Previously we directly issued driver.abort(), but the re-entrancy
      // guarantees of that are uncertain (and we believe it was not working).  However, this
      // was difficult to discern since logging is unreliable during JVM shutdown and we would not
      // see the above log message.
      throw e;
    }

    LOG.warning("Unhandled status update " + status);
    failedStatusUpdates.incrementAndGet();
  }

  @Override
  public void error(SchedulerDriver driver, String message) {
    LOG.severe("Received error message: " + message);
    lifecycle.shutdown();
  }

  @Override
  public void executorLost(SchedulerDriver schedulerDriver, ExecutorID executorID, SlaveID slaveID,
      int status) {

    LOG.info("Lost executor " + executorID);
    lostExecutors.incrementAndGet();
  }

  @Timed("scheduler_framework_message")
  @Override
  public void frameworkMessage(SchedulerDriver driver, ExecutorID executor, SlaveID slave,
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
          // TODO(William Farner): Refactor this to use a thinner interface here.  As it stands
          // it is odd that we route the registered() call to schedulerCore via the
          // registeredListener and call the schedulerCore directly here.
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
}

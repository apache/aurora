/**
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
package org.apache.aurora.scheduler.mesos;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Qualifier;

import com.google.common.base.Preconditions;
import com.twitter.common.application.Lifecycle;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.stats.Stats;

import org.apache.aurora.GuiceUtils.AllowUnchecked;
import org.apache.aurora.scheduler.TaskLauncher;
import org.apache.aurora.scheduler.base.SchedulerException;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent.DriverDisconnected;
import org.apache.aurora.scheduler.events.PubsubEvent.DriverRegistered;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

/**
 * Location for communication with mesos.
 */
class MesosSchedulerImpl implements Scheduler {
  private static final Logger LOG = Logger.getLogger(MesosSchedulerImpl.class.getName());

  private final AtomicLong totalResourceOffers = Stats.exportLong("scheduler_resource_offers");
  private final AtomicLong totalFailedStatusUpdates = Stats.exportLong("scheduler_status_updates");
  private final AtomicLong totalFrameworkDisconnects =
      Stats.exportLong("scheduler_framework_disconnects");
  private final AtomicLong totalFrameworkReregisters =
      Stats.exportLong("scheduler_framework_reregisters");
  private final AtomicLong totalLostExecutors = Stats.exportLong("scheduler_lost_executors");

  private final List<TaskLauncher> taskLaunchers;

  private final Storage storage;
  private final Lifecycle lifecycle;
  private final EventSink eventSink;
  private final Executor executor;
  private volatile boolean isRegistered = false;

  /**
   * Binding annotation for the executor the incoming Mesos message handler uses.
   */
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  @interface SchedulerExecutor { }

  /**
   * Creates a new scheduler.
   *
   * @param storage Store to save host attributes into.
   * @param lifecycle Application lifecycle manager.
   * @param taskLaunchers Task launchers, which will be used in order.  Calls to
   *                      {@link TaskLauncher#willUse(Offer)} and
   *                      {@link TaskLauncher#statusUpdate(TaskStatus)} are propagated to provided
   *                      launchers, ceasing after the first match (based on a return value of
   *                      {@code true}.
   * @param eventSink Pubsub sink to send driver status changes to.
   * @param executor Executor for async work
   */
  @Inject
  public MesosSchedulerImpl(
      Storage storage,
      final Lifecycle lifecycle,
      List<TaskLauncher> taskLaunchers,
      EventSink eventSink,
      @SchedulerExecutor Executor executor) {

    this.storage = requireNonNull(storage);
    this.lifecycle = requireNonNull(lifecycle);
    this.taskLaunchers = requireNonNull(taskLaunchers);
    this.eventSink = requireNonNull(eventSink);
    this.executor = requireNonNull(executor);
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
      @Override
      protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getSchedulerStore().saveFrameworkId(frameworkId.getValue());
      }
    });
    isRegistered = true;
    eventSink.post(new DriverRegistered());
  }

  @Override
  public void disconnected(SchedulerDriver schedulerDriver) {
    LOG.warning("Framework disconnected.");
    totalFrameworkDisconnects.incrementAndGet();
    eventSink.post(new DriverDisconnected());
  }

  @Override
  public void reregistered(SchedulerDriver schedulerDriver, MasterInfo masterInfo) {
    LOG.info("Framework re-registered with master " + masterInfo);
    totalFrameworkReregisters.incrementAndGet();
  }

  @Timed("scheduler_resource_offers")
  @Override
  public void resourceOffers(SchedulerDriver driver, final List<Offer> offers) {
    Preconditions.checkState(isRegistered, "Must be registered before receiving offers.");

    // Store all host attributes in a single write operation to prevent other threads from
    // securing the storage lock between saves.  We also save the host attributes before passing
    // offers elsewhere to ensure that host attributes are available before attempting to
    // schedule tasks associated with offers.
    // TODO(wfarner): Reconsider the requirements here, augment the task scheduler to skip over
    //                offers when the host attributes cannot be found. (AURORA-137)

    executor.execute(new Runnable() {
      @Override
      public void run() {
        storage.write(new MutateWork.NoResult.Quiet() {
          @Override
          protected void execute(MutableStoreProvider storeProvider) {
            for (final Offer offer : offers) {
              storeProvider.getAttributeStore().saveHostAttributes(
                  AttributeStore.Util.mergeOffer(storeProvider.getAttributeStore(), offer));
            }
          }
        });

        for (Offer offer : offers) {
          if (LOG.isLoggable(Level.FINE)) {
            LOG.log(Level.FINE, String.format("Received offer: %s", offer));
          }
          totalResourceOffers.incrementAndGet();
          for (TaskLauncher launcher : taskLaunchers) {
            if (launcher.willUse(offer)) {
              break;
            }
          }
        }
      }
    });
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
    String infoMsg = info == null ? "" : " with info " + info;
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
    totalFailedStatusUpdates.incrementAndGet();
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
    totalLostExecutors.incrementAndGet();
  }

  @Timed("scheduler_framework_message")
  @Override
  public void frameworkMessage(
      SchedulerDriver driver,
      ExecutorID executorID,
      SlaveID slave,
      byte[] data) {

    LOG.warning("Ignoring framework message.");
  }
}

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

import javax.inject.Inject;
import javax.inject.Qualifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import org.apache.aurora.GuiceUtils.AllowUnchecked;
import org.apache.aurora.common.application.Lifecycle;
import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.TaskStatusHandler;
import org.apache.aurora.scheduler.base.SchedulerException;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent.DriverDisconnected;
import org.apache.aurora.scheduler.events.PubsubEvent.DriverRegistered;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStatusReceived;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.stats.CachedCounters;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

import static org.apache.mesos.Protos.Offer;
import static org.apache.mesos.Protos.TaskStatus.Reason.REASON_RECONCILIATION;

/**
 * Location for communication with mesos.
 */
@VisibleForTesting
public class MesosSchedulerImpl implements Scheduler {
  private final TaskStatusHandler taskStatusHandler;
  private final OfferManager offerManager;
  private final Storage storage;
  private final Lifecycle lifecycle;
  private final EventSink eventSink;
  private final Executor executor;
  private final Logger log;
  private final CachedCounters counters;
  private volatile boolean isRegistered = false;

  /**
   * Binding annotation for the executor the incoming Mesos message handler uses.
   */
  @VisibleForTesting
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  public @interface SchedulerExecutor { }

  /**
   * Creates a new scheduler.
   *
   * @param storage Store to save host attributes into.
   * @param lifecycle Application lifecycle manager.
   * @param taskStatusHandler Task status update manager.
   * @param offerManager Offer manager.
   * @param eventSink Pubsub sink to send driver status changes to.
   * @param executor Executor for async work
   */
  @Inject
  public MesosSchedulerImpl(
      Storage storage,
      Lifecycle lifecycle,
      TaskStatusHandler taskStatusHandler,
      OfferManager offerManager,
      EventSink eventSink,
      @SchedulerExecutor Executor executor,
      CachedCounters counters) {

    this(
        storage,
        lifecycle,
        taskStatusHandler,
        offerManager,
        eventSink,
        executor,
        counters,
        LoggerFactory.getLogger(MesosSchedulerImpl.class));
  }

  @VisibleForTesting
  MesosSchedulerImpl(
      Storage storage,
      Lifecycle lifecycle,
      TaskStatusHandler taskStatusHandler,
      OfferManager offerManager,
      EventSink eventSink,
      Executor executor,
      CachedCounters counters,
      Logger log) {

    this.storage = requireNonNull(storage);
    this.lifecycle = requireNonNull(lifecycle);
    this.taskStatusHandler = requireNonNull(taskStatusHandler);
    this.offerManager = requireNonNull(offerManager);
    this.eventSink = requireNonNull(eventSink);
    this.executor = requireNonNull(executor);
    this.counters = requireNonNull(counters);
    this.log = requireNonNull(log);
  }

  @Override
  public void slaveLost(SchedulerDriver schedulerDriver, SlaveID slaveId) {
    log.info("Received notification of lost slave: " + slaveId);
  }

  @Override
  public void registered(
      SchedulerDriver driver,
      final FrameworkID frameworkId,
      MasterInfo masterInfo) {

    log.info("Registered with ID " + frameworkId + ", master: " + masterInfo);

    storage.write(
        (NoResult.Quiet) storeProvider ->
            storeProvider.getSchedulerStore().saveFrameworkId(frameworkId.getValue()));
    isRegistered = true;
    eventSink.post(new DriverRegistered());
  }

  @Override
  public void disconnected(SchedulerDriver schedulerDriver) {
    log.warn("Framework disconnected.");
    counters.get("scheduler_framework_disconnects").get();
    eventSink.post(new DriverDisconnected());
  }

  @Override
  public void reregistered(SchedulerDriver schedulerDriver, MasterInfo masterInfo) {
    log.info("Framework re-registered with master " + masterInfo);
    counters.get("scheduler_framework_reregisters").incrementAndGet();
  }

  @Timed("scheduler_resource_offers")
  @Override
  public void resourceOffers(SchedulerDriver driver, final List<Offer> offers) {
    Preconditions.checkState(isRegistered, "Must be registered before receiving offers.");

    executor.execute(() -> {
      // TODO(wfarner): Reconsider the requirements here, augment the task scheduler to skip over
      //                offers when the host attributes cannot be found. (AURORA-137)
      storage.write((NoResult.Quiet) storeProvider -> {
        for (Offer offer : offers) {
          IHostAttributes attributes =
              AttributeStore.Util.mergeOffer(storeProvider.getAttributeStore(), offer);
          storeProvider.getAttributeStore().saveHostAttributes(attributes);
          log.debug("Received offer: {}", offer);
          counters.get("scheduler_resource_offers").incrementAndGet();
          offerManager.addOffer(new HostOffer(offer, attributes));
        }
      });
    });
  }

  @Override
  public void offerRescinded(SchedulerDriver schedulerDriver, OfferID offerId) {
    log.info("Offer rescinded: " + offerId);
    offerManager.cancelOffer(offerId);
  }

  private static void logStatusUpdate(Logger logger, TaskStatus status) {
    // Periodic task reconciliation runs generate a large amount of no-op messages.
    // Suppress logging for reconciliation status updates by default.
    boolean debugLevel = status.hasReason() && status.getReason() == REASON_RECONCILIATION;

    StringBuilder message = new StringBuilder("Received status update for task ")
        .append(status.getTaskId().getValue())
        .append(" in state ")
        .append(status.getState());
    if (status.hasSource()) {
      message.append(" from ").append(status.getSource());
    }
    if (status.hasReason()) {
      message.append(" with ").append(status.getReason());
    }
    if (status.hasMessage()) {
      message.append(": ").append(status.getMessage());
    }
    if (debugLevel) {
      logger.debug(message.toString());
    } else {
      logger.info(message.toString());
    }
  }

  private static final Function<Double, Long> SECONDS_TO_MICROS = seconds -> (long) (seconds * 1E6);

  @AllowUnchecked
  @Timed("scheduler_status_update")
  @Override
  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    logStatusUpdate(log, status);
    eventSink.post(new TaskStatusReceived(
        status.getState(),
        Optional.fromNullable(status.getSource()),
        status.hasReason() ? Optional.of(status.getReason()) : Optional.absent(),
        Optional.fromNullable(status.getTimestamp()).transform(SECONDS_TO_MICROS)));

    try {
      // The status handler is responsible for acknowledging the update.
      taskStatusHandler.statusUpdate(status);
    } catch (SchedulerException e) {
      log.error("Status update failed due to scheduler exception: " + e, e);
      // We re-throw the exception here to trigger an abort of the driver.
      throw e;
    }
  }

  @Override
  public void error(SchedulerDriver driver, String message) {
    log.error("Received error message: " + message);
    lifecycle.shutdown();
  }

  @Override
  public void executorLost(SchedulerDriver schedulerDriver, ExecutorID executorID, SlaveID slaveID,
      int status) {

    log.warn("Lost executor " + executorID);
    counters.get("scheduler_lost_executors").incrementAndGet();
  }

  @Timed("scheduler_framework_message")
  @Override
  public void frameworkMessage(
      SchedulerDriver driver,
      ExecutorID executorID,
      SlaveID slave,
      byte[] data) {

    log.warn("Ignoring framework message.");
  }
}

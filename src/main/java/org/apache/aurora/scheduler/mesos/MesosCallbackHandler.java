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
import javax.inject.Inject;
import javax.inject.Qualifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;

import org.apache.aurora.common.application.Lifecycle;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.TaskStatusHandler;
import org.apache.aurora.scheduler.base.SchedulerException;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.mesos.v1.Protos.AgentID;
import org.apache.mesos.v1.Protos.ExecutorID;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.Protos.MasterInfo;
import org.apache.mesos.v1.Protos.Offer;
import org.apache.mesos.v1.Protos.OfferID;
import org.apache.mesos.v1.Protos.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

import static org.apache.mesos.v1.Protos.TaskStatus.Reason.REASON_RECONCILIATION;

/**
 * Abstracts the logic of handling scheduler events/callbacks from Mesos.
 * This interface allows the two different Mesos Scheduler Callback classes
 * to share logic and to simplify testing.
 */
public interface MesosCallbackHandler {
  void handleRegistration(FrameworkID frameworkId, MasterInfo masterInfo);
  void handleReregistration(MasterInfo masterInfo);
  void handleOffers(List<Offer> offers);
  void handleDisconnection();
  void handleRescind(OfferID offerId);
  void handleMessage(ExecutorID executor, AgentID agent);
  void handleError(String message);
  void handleUpdate(TaskStatus status);
  void handleLostAgent(AgentID agentId);
  void handleLostExecutor(ExecutorID executorID, AgentID slaveID, int status);

  class MesosCallbackHandlerImpl implements MesosCallbackHandler {

    private final TaskStatusHandler taskStatusHandler;
    private final OfferManager offerManager;
    private final Storage storage;
    private final Lifecycle lifecycle;
    private final EventSink eventSink;
    private final Executor executor;
    private final Logger log;

    private final AtomicLong offersRescinded;
    private final AtomicLong slavesLost;
    private final AtomicLong reRegisters;
    private final AtomicLong offersRecieved;
    private final AtomicLong disconnects;
    private final AtomicLong executorsLost;

    /**
     * Binding annotation for the executor the incoming Mesos message handler uses.
     */
    @VisibleForTesting
    @Qualifier
    @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
    public @interface SchedulerExecutor { }

    /**
     * Creates a new handler for callbacks.
     *
     * @param storage Store to save host attributes into.
     * @param lifecycle Application lifecycle manager.
     * @param taskStatusHandler Task status update manager.
     * @param offerManager Offer manager.
     * @param eventSink Pubsub sink to send driver status changes to.
     * @param executor Executor for async work
     */
    @Inject
    public MesosCallbackHandlerImpl(
        Storage storage,
        Lifecycle lifecycle,
        TaskStatusHandler taskStatusHandler,
        OfferManager offerManager,
        EventSink eventSink,
        @SchedulerExecutor Executor executor,
        StatsProvider statsProvider) {

      this(
          storage,
          lifecycle,
          taskStatusHandler,
          offerManager,
          eventSink,
          executor,
          LoggerFactory.getLogger(MesosCallbackHandlerImpl.class),
          statsProvider);
    }

    @VisibleForTesting
    MesosCallbackHandlerImpl(
        Storage storage,
        Lifecycle lifecycle,
        TaskStatusHandler taskStatusHandler,
        OfferManager offerManager,
        EventSink eventSink,
        Executor executor,
        Logger log,
        StatsProvider statsProvider) {

      this.storage = requireNonNull(storage);
      this.lifecycle = requireNonNull(lifecycle);
      this.taskStatusHandler = requireNonNull(taskStatusHandler);
      this.offerManager = requireNonNull(offerManager);
      this.eventSink = requireNonNull(eventSink);
      this.executor = requireNonNull(executor);
      this.log = requireNonNull(log);

      this.offersRescinded = statsProvider.makeCounter("offers_rescinded");
      this.slavesLost = statsProvider.makeCounter("slaves_lost");
      this.reRegisters = statsProvider.makeCounter("scheduler_framework_reregisters");
      this.offersRecieved = statsProvider.makeCounter("scheduler_resource_offers");
      this.disconnects = statsProvider.makeCounter("scheduler_framework_disconnects");
      this.executorsLost = statsProvider.makeCounter("scheduler_lost_executors");
    }

    @Override
    public void handleRegistration(FrameworkID frameworkId, MasterInfo masterInfo) {
      log.info("Registered with ID " + frameworkId + ", master: " + masterInfo);

      storage.write(
          (Storage.MutateWork.NoResult.Quiet) storeProvider ->
              storeProvider.getSchedulerStore().saveFrameworkId(frameworkId.getValue()));
      eventSink.post(new PubsubEvent.DriverRegistered());
    }

    @Override
    public void handleReregistration(MasterInfo masterInfo) {
      log.info("Framework re-registered with master " + masterInfo);
      reRegisters.incrementAndGet();
    }

    @Override
    public void handleOffers(List<Offer> offers) {
      // Don't invoke the executor or storage lock if the list of offers is empty.
      if (offers.isEmpty()) {
        return;
      }

      executor.execute(() -> {
        // TODO(wfarner): Reconsider the requirements here, augment the task scheduler to skip over
        //                offers when the host attributes cannot be found. (AURORA-137)
        storage.write((Storage.MutateWork.NoResult.Quiet) storeProvider -> {
          for (Offer offer : offers) {
            IHostAttributes attributes =
                AttributeStore.Util.mergeOffer(storeProvider.getAttributeStore(), offer);
            storeProvider.getAttributeStore().saveHostAttributes(attributes);
            log.debug("Received offer: {}", offer);
            offersRecieved.incrementAndGet();
            offerManager.addOffer(new HostOffer(offer, attributes));
          }
        });
      });
    }

    @Override
    public void handleDisconnection() {
      log.warn("Framework disconnected.");
      this.disconnects.incrementAndGet();
      eventSink.post(new PubsubEvent.DriverDisconnected());
    }

    @Override
    public void handleRescind(OfferID offerId) {
      log.info("Offer rescinded: " + offerId);
      offerManager.cancelOffer(offerId);
      offersRescinded.incrementAndGet();
    }

    @Override
    public void handleMessage(ExecutorID executorID, AgentID agentID) {
      log.warn(
          "Ignoring framework message from {} on {}.",
          executorID.getValue(),
          agentID.getValue());
    }

    @Override
    public void handleError(String message) {
      log.error("Received error message: " + message);
      lifecycle.shutdown();
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

    private static final Function<Double, Long> SECONDS_TO_MICROS =
        seconds -> (long) (seconds * 1E6);

    @Override
    public void handleUpdate(TaskStatus status) {
      logStatusUpdate(log, status);
      eventSink.post(new PubsubEvent.TaskStatusReceived(
          status.getState(),
          // Source and Reason are enums. They cannot be null so we we need to use `hasXXX`.
          status.hasSource() ? Optional.of(status.getSource()) : Optional.absent(),
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
    public void handleLostAgent(AgentID agentId) {
      log.info("Received notification of lost agent: " + agentId);
      slavesLost.incrementAndGet();
    }

    @Override
    public void handleLostExecutor(ExecutorID executorID, AgentID slaveID, int status) {
      // With the current implementation of MESOS-313, Mesos is also reporting clean terminations of
      // custom executors via the executorLost callback.
      if (status != 0) {
        log.warn("Lost executor " + executorID + " on slave " + slaveID + " with status " + status);
        executorsLost.incrementAndGet();
      }
    }
  }
}

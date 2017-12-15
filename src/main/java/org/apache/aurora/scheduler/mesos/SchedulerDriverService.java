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

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;

import org.apache.aurora.common.stats.Stats;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.mesos.Protos.Filters;
import org.apache.mesos.Protos.Offer.Operation;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.v1.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkState;

import static org.apache.mesos.Protos.Status.DRIVER_RUNNING;

/**
 * Manages the lifecycle of the scheduler driver, and provides a more constrained API to use it.
 */
class SchedulerDriverService extends AbstractIdleService implements Driver {
  private static final Logger LOG = LoggerFactory.getLogger(SchedulerDriverService.class);

  private final AtomicLong killFailures = Stats.exportLong("scheduler_driver_kill_failures");
  private final DriverFactory driverFactory;
  private final FrameworkInfoFactory infoFactory;

  private final Scheduler scheduler;
  private final Storage storage;
  private final DriverSettings driverSettings;
  private final SettableFuture<SchedulerDriver> driverFuture = SettableFuture.create();

  @Inject
  SchedulerDriverService(
      Scheduler scheduler,
      Storage storage,
      DriverSettings driverSettings,
      DriverFactory driverFactory,
      FrameworkInfoFactory infoFactory) {

    this.scheduler = requireNonNull(scheduler);
    this.storage = requireNonNull(storage);
    this.driverSettings = requireNonNull(driverSettings);
    this.driverFactory = requireNonNull(driverFactory);
    this.infoFactory = requireNonNull(infoFactory);
  }

  @Override
  protected void startUp() {
    Optional<String> frameworkId = storage.read(
        storeProvider -> storeProvider.getSchedulerStore().fetchFrameworkId());

    LOG.info("Connecting to mesos master: " + driverSettings.getMasterUri());
    if (!driverSettings.getCredentials().isPresent()) {
      LOG.warn("Connecting to master without authentication!");
    }

    Protos.FrameworkInfo.Builder frameworkBuilder = infoFactory.getFrameworkInfo().toBuilder();

    if (frameworkId.isPresent()) {
      LOG.info("Found persisted framework ID: " + frameworkId);
      frameworkBuilder.setId(Protos.FrameworkID.newBuilder().setValue(frameworkId.get()));
    } else {
      LOG.warn("Did not find a persisted framework ID, connecting as a new framework.");
    }

    SchedulerDriver schedulerDriver = driverFactory.create(
        scheduler,
        driverSettings.getCredentials(),
        frameworkBuilder.build(),
        driverSettings.getMasterUri());
    Status status = schedulerDriver.start();
    LOG.info("Driver started with code " + status);

    driverFuture.set(schedulerDriver);
  }

  @Override
  public void blockUntilStopped() {
    Futures.getUnchecked(driverFuture).join();
  }

  @Override
  protected void shutDown() throws ExecutionException, InterruptedException {
    // WARNING: stop() and stop(false) are dangerous, avoid at all costs. See the docs for
    // SchedulerDriver for more details.
    driverFuture.get().stop(true /* failover */);
  }

  @Override
  public void abort() {
    Futures.getUnchecked(driverFuture).abort();
  }

  @Override
  public void acceptOffers(
      Protos.OfferID offerId,
      Collection<Protos.Offer.Operation> operations,
      Protos.Filters filter) {
    ensureRunning();

    OfferID convertedOfferId = ProtosConversion.convert(offerId);
    Collection<Operation> convertedOperations =
        Collections2.transform(operations, ProtosConversion::convert);
    Filters convertedFilter = ProtosConversion.convert(filter);

    Futures.getUnchecked(driverFuture)
        .acceptOffers(ImmutableList.of(convertedOfferId), convertedOperations, convertedFilter);
  }

  @Override
  public void acceptInverseOffer(Protos.OfferID offerID, Protos.Filters filter) {
    throw new UnsupportedOperationException("SchedulerDriver does not support inverse offers");
  }

  @Override
  public void declineOffer(Protos.OfferID offerId, Protos.Filters filter) {
    ensureRunning();

    OfferID convertedOfferId = ProtosConversion.convert(offerId);
    Filters convertedFilter = ProtosConversion.convert(filter);

    Futures.getUnchecked(driverFuture).declineOffer(convertedOfferId, convertedFilter);
  }

  @Override
  public void killTask(String taskId) {
    ensureRunning();
    Status status = Futures.getUnchecked(driverFuture).killTask(
        TaskID.newBuilder().setValue(taskId).build());

    if (status != DRIVER_RUNNING) {
      LOG.error("Attempt to kill task {} failed with code {}", taskId, status);
      killFailures.incrementAndGet();
    }
  }

  @Override
  public void acknowledgeStatusUpdate(Protos.TaskStatus status) {
    ensureRunning();

    TaskStatus convertedStatus = ProtosConversion.convert(status);
    Futures.getUnchecked(driverFuture).acknowledgeStatusUpdate(convertedStatus);
  }

  @Override
  public void reconcileTasks(Collection<Protos.TaskStatus> statuses) {
    ensureRunning();

    Collection<TaskStatus> convertedStatuses =
        Collections2.transform(statuses, ProtosConversion::convert);
    Futures.getUnchecked(driverFuture).reconcileTasks(convertedStatuses);
  }

  private void ensureRunning() {
    checkState(isRunning(), "Driver is not running.");
  }
}

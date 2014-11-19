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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.twitter.common.stats.Stats;

import org.apache.aurora.scheduler.storage.Storage;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkState;

import static org.apache.mesos.Protos.Status.DRIVER_RUNNING;

/**
 * Manages the lifecycle of the scheduler driver, and provides a more constrained API to use it.
 */
class SchedulerDriverService extends AbstractIdleService implements Driver {
  private static final Logger LOG = Logger.getLogger(SchedulerDriverService.class.getName());

  private final AtomicLong killFailures = Stats.exportLong("scheduler_driver_kill_failures");
  private final DriverFactory driverFactory;

  private final Scheduler scheduler;
  private final Storage storage;
  private final DriverSettings driverSettings;
  private final SettableFuture<SchedulerDriver> driverFuture = SettableFuture.create();

  @Inject
  SchedulerDriverService(
      Scheduler scheduler,
      Storage storage,
      DriverSettings driverSettings,
      DriverFactory driverFactory) {

    this.scheduler = requireNonNull(scheduler);
    this.storage = requireNonNull(storage);
    this.driverSettings = requireNonNull(driverSettings);
    this.driverFactory = requireNonNull(driverFactory);
  }

  @Override
  protected void startUp() {
    Optional<String> frameworkId = storage.read(
        new Storage.Work.Quiet<Optional<String>>() {
          @Override
          public Optional<String> apply(Storage.StoreProvider storeProvider) {
            return storeProvider.getSchedulerStore().fetchFrameworkId();
          }
        });

    LOG.info("Connecting to mesos master: " + driverSettings.getMasterUri());
    if (!driverSettings.getCredentials().isPresent()) {
      LOG.warning("Connecting to master without authentication!");
    }

    FrameworkInfo.Builder frameworkBuilder = driverSettings.getFrameworkInfo().toBuilder();

    if (frameworkId.isPresent()) {
      LOG.info("Found persisted framework ID: " + frameworkId);
      frameworkBuilder.setId(FrameworkID.newBuilder().setValue(frameworkId.get()));
    } else {
      LOG.warning("Did not find a persisted framework ID, connecting as a new framework.");
    }

    SchedulerDriver schedulerDriver = driverFactory.create(
        scheduler,
        driverSettings.getCredentials(),
        frameworkBuilder.build(),
        driverSettings.getMasterUri());
    Protos.Status status = schedulerDriver.start();
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
  public void launchTask(Protos.OfferID offerId, Protos.TaskInfo task) {
    checkState(isRunning(), "Driver is not running.");
    Futures.getUnchecked(driverFuture)
        .launchTasks(ImmutableList.of(offerId), ImmutableList.of(task));
  }

  @Override
  public void declineOffer(Protos.OfferID offerId) {
    checkState(isRunning(), "Driver is not running.");
    Futures.getUnchecked(driverFuture).declineOffer(offerId);
  }

  @Override
  public void killTask(String taskId) {
    checkState(isRunning(), "Driver is not running.");
    Protos.Status status = Futures.getUnchecked(driverFuture).killTask(
        Protos.TaskID.newBuilder().setValue(taskId).build());

    if (status != DRIVER_RUNNING) {
      LOG.severe(String.format("Attempt to kill task %s failed with code %s",
          taskId, status));
      killFailures.incrementAndGet();
    }
  }
}

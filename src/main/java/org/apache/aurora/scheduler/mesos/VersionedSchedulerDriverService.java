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
import java.util.concurrent.CountDownLatch;

import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;

import org.apache.aurora.common.base.Command;
import org.apache.aurora.scheduler.events.PubsubEvent.DriverRegistered;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.mesos.v1.Protos.Credential;
import org.apache.mesos.v1.Protos.Filters;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.Protos.FrameworkInfo;
import org.apache.mesos.v1.Protos.Offer.Operation;
import org.apache.mesos.v1.Protos.OfferID;
import org.apache.mesos.v1.Protos.TaskID;
import org.apache.mesos.v1.Protos.TaskStatus;
import org.apache.mesos.v1.scheduler.Mesos;
import org.apache.mesos.v1.scheduler.Protos.Call;
import org.apache.mesos.v1.scheduler.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkState;

/**
 * A driver implementation that uses the V1 API drivers from libmesos.
 */
class VersionedSchedulerDriverService extends AbstractIdleService
    implements Driver, EventSubscriber {
  private static final Logger LOG = LoggerFactory.getLogger(VersionedSchedulerDriverService.class);

  private final Storage storage;
  private final DriverSettings driverSettings;
  private final Scheduler scheduler;
  private final VersionedDriverFactory factory;
  private final FrameworkInfoFactory infoFactory;
  private final SettableFuture<Mesos> mesosFuture = SettableFuture.create();
  private final CountDownLatch terminationLatch = new CountDownLatch(1);
  private final CountDownLatch registrationLatch = new CountDownLatch(1);

  @Inject
  VersionedSchedulerDriverService(
      Storage storage,
      DriverSettings settings,
      Scheduler scheduler,
      VersionedDriverFactory factory,
      FrameworkInfoFactory infoFactory) {
    this.storage = requireNonNull(storage);
    this.driverSettings = requireNonNull(settings);
    this.scheduler = requireNonNull(scheduler);
    this.factory = requireNonNull(factory);
    this.infoFactory = requireNonNull(infoFactory);
  }

  private FrameworkID getFrameworkId() {
    String id = storage.read(storeProvider ->
        storeProvider.getSchedulerStore().fetchFrameworkId().get());
    return FrameworkID.newBuilder().setValue(id).build();
  }

  @Override
  protected void startUp() throws Exception {
    Optional<String> frameworkId = storage.read(
        storeProvider -> storeProvider.getSchedulerStore().fetchFrameworkId());

    LOG.info("Connecting to mesos master: " + driverSettings.getMasterUri());
    if (!driverSettings.getCredentials().isPresent()) {
      LOG.warn("Connecting to master without authentication!");
    }

    FrameworkInfo.Builder frameworkBuilder = infoFactory.getFrameworkInfo().toBuilder();

    if (frameworkId.isPresent()) {
      LOG.info("Found persisted framework ID: " + frameworkId);
      frameworkBuilder.setId(FrameworkID.newBuilder().setValue(frameworkId.get()));
    } else {
      LOG.warn("Did not find a persisted framework ID, connecting as a new framework.");
    }

    Credential credential = driverSettings.getCredentials().orNull();
    Mesos mesos = factory.create(
        scheduler,
        frameworkBuilder.build(),
        driverSettings.getMasterUri(),
        Optional.fromNullable(credential));

    mesosFuture.set(mesos);
  }

  @Override
  protected void shutDown() throws Exception {
    terminationLatch.countDown();
  }

  @Override
  public void acceptOffers(OfferID offerId, Collection<Operation> operations, Filters filter) {
    whenRegistered(() -> {
      Collection<Operation.Type> opTypes = Collections2.transform(operations, Operation::getType);
      LOG.info("Accepting offer {} with ops {}", offerId, opTypes);

      Futures.getUnchecked(mesosFuture).send(
          Call.newBuilder()
              .setFrameworkId(getFrameworkId())
              .setType(Call.Type.ACCEPT)
              .setAccept(
                  Call.Accept.newBuilder()
                      .addOfferIds(offerId)
                      .addAllOperations(operations)
                      .setFilters(filter))
              .build());
    });

  }

  @Override
  public void declineOffer(OfferID offerId, Filters filter) {
    whenRegistered(() -> {
      LOG.info("Declining offer {}", offerId.getValue());

      Futures.getUnchecked(mesosFuture).send(
          Call.newBuilder().setType(Call.Type.DECLINE)
              .setFrameworkId(getFrameworkId())
              .setDecline(
                  Call.Decline.newBuilder()
                      .setFilters(filter)
                      .addOfferIds(offerId))
              .build()
      );
    });
  }

  @Override
  public void killTask(String taskId) {
    whenRegistered(() -> {
      LOG.info("Killing task {}", taskId);

      Futures.getUnchecked(mesosFuture).send(
          Call.newBuilder().setType(Call.Type.KILL)
              .setFrameworkId(getFrameworkId())
              .setKill(
                  Call.Kill.newBuilder()
                      .setTaskId(TaskID.newBuilder().setValue(taskId)))
              .build()
      );

    });
  }

  @Override
  public void acknowledgeStatusUpdate(TaskStatus status) {
    // The Mesos API says frameworks are only supposed to acknowledge status updates
    // with a UUID. The V0Driver accepts them just fine but the V1Driver logs every time
    // a status update is given without a uuid. To silence logs, we drop them here.

    whenRegistered(() -> {
      if (!status.hasUuid()) {
        return;
      }

      LOG.info("Acking status update for {} with uuid: {}",
          status.getTaskId().getValue(),
          status.getUuid());

      Futures.getUnchecked(mesosFuture).send(
          Call.newBuilder().setType(Call.Type.ACKNOWLEDGE)
              .setFrameworkId(getFrameworkId())
              .setAcknowledge(
                  Call.Acknowledge.newBuilder()
                      .setAgentId(status.getAgentId())
                      .setTaskId(status.getTaskId())
                      .setUuid(status.getUuid()))
              .build()
      );
    });

  }

  @Override
  public void reconcileTasks(Collection<TaskStatus> statuses) {
    whenRegistered(() -> {
      Collection<Call.Reconcile.Task> tasks = Collections2.transform(statuses, taskStatus ->
          Call.Reconcile.Task.newBuilder()
              .setTaskId(taskStatus.getTaskId())
              .build());

      Futures.getUnchecked(mesosFuture).send(
          Call.newBuilder()
              .setType(Call.Type.RECONCILE)
              .setFrameworkId(getFrameworkId())
              .setReconcile(
                  Call.Reconcile.newBuilder()
                      .addAllTasks(tasks))
              .build()
      );
    });
  }

  @Override
  public void blockUntilStopped() {
    ensureRunning();
    try {
      terminationLatch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void abort() {
    terminationLatch.countDown();
    stopAsync();
  }

  @Subscribe
  public void registered(DriverRegistered event) {
    registrationLatch.countDown();
  }

  private void whenRegistered(Command c) {
    ensureRunning();
    // We need to block until registered because thats when we are guaranteed to have our
    // framework id. Without it, we cannot construct any Call objects.
    try {
      registrationLatch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    c.execute();
  }

  private void ensureRunning() {
    checkState(isRunning(), "Driver is not running.");
  }
}

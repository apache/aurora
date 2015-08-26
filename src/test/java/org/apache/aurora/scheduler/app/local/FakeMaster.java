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
package org.apache.aurora.scheduler.app.local;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.util.concurrent.ExecutorServiceShutdown;
import org.apache.aurora.scheduler.app.local.simulator.Events.Started;
import org.apache.aurora.scheduler.mesos.DriverFactory;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.Filters;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Request;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import static java.util.Objects.requireNonNull;

import static org.apache.mesos.Protos.FrameworkInfo;

/**
 * A simulated master for use in scheduler testing.
 */
@SuppressWarnings("deprecation")
public class FakeMaster implements SchedulerDriver, DriverFactory {

  private static final Logger LOG = Logger.getLogger(FakeMaster.class.getName());

  private final Map<TaskID, Task> activeTasks = Collections.synchronizedMap(Maps.newHashMap());
  private final Map<OfferID, Offer> idleOffers = Collections.synchronizedMap(Maps.newHashMap());
  private final Map<OfferID, Offer> sentOffers = Collections.synchronizedMap(Maps.newHashMap());

  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
  private final CountDownLatch stopped = new CountDownLatch(1);

  private final SettableFuture<Scheduler> schedulerFuture = SettableFuture.create();
  private final EventBus eventBus;

  @Inject
  FakeMaster(EventBus eventBus) {
    this.eventBus = requireNonNull(eventBus);
  }

  public void addResources(Iterable<Offer> offers) {
    assertNotStopped();

    synchronized (idleOffers) {
      for (Offer offer : offers) {
        checkState(!idleOffers.containsKey(offer.getId()), "Duplicate offer id " + offer.getId());
        idleOffers.put(offer.getId(), offer);
      }
    }
  }

  public void changeState(TaskID task, TaskState state) {
    assertNotStopped();

    checkState(activeTasks.containsKey(task), "Task " + task + " does not exist.");
    Futures.getUnchecked(schedulerFuture).statusUpdate(this, TaskStatus.newBuilder()
        .setTaskId(task)
        .setState(state)
        .build());
  }

  @Override
  public SchedulerDriver create(
      Scheduler scheduler,
      Optional<Protos.Credential> credentials,
      FrameworkInfo frameworkInfo,
      String master) {

    schedulerFuture.set(scheduler);
    return this;
  }

  @Override
  public Status start() {
    assertNotStopped();

    Futures.getUnchecked(schedulerFuture).registered(this,
        FrameworkID.newBuilder().setValue("local").build(),
        MasterInfo.getDefaultInstance());

    eventBus.post(new Started());

    executor.scheduleAtFixedRate(
        new Runnable() {
          @Override
          public void run() {
            List<Offer> allOffers;
            synchronized (sentOffers) {
              synchronized (idleOffers) {
                sentOffers.putAll(idleOffers);
                allOffers = ImmutableList.copyOf(idleOffers.values());
                idleOffers.clear();
              }
            }

            if (allOffers.isEmpty()) {
              LOG.info("All offers consumed, suppressing offer cycle.");
            } else {
              Futures.getUnchecked(schedulerFuture).resourceOffers(FakeMaster.this, allOffers);
            }
          }
        },
        1,
        5,
        TimeUnit.SECONDS);

    return Status.DRIVER_RUNNING;
  }

  @Override
  public Status stop(boolean failover) {
    return stop();
  }

  @Override
  public Status stop() {
    stopped.countDown();
    new ExecutorServiceShutdown(executor, Amount.of(1L, Time.SECONDS)).execute();
    return Status.DRIVER_RUNNING;
  }

  @Override
  public Status abort() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Status join() {
    try {
      stopped.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return Status.DRIVER_RUNNING;
  }

  @Override
  public Status run() {
    assertNotStopped();
    return Status.DRIVER_RUNNING;
  }

  @Override
  public Status requestResources(Collection<Request> requests) {
    throw new UnsupportedOperationException();
  }

  private void assertNotStopped() {
    Preconditions.checkState(stopped.getCount() == 1, "Driver is stopped.");
  }

  private void checkState(boolean assertion, String failureMessage) {
    if (!assertion) {
      Futures.getUnchecked(schedulerFuture).error(this, failureMessage);
      stop();
      throw new IllegalStateException(failureMessage);
    }
  }

  @Override
  public Status launchTasks(
      Collection<OfferID> offerIds,
      Collection<TaskInfo> tasks,
      Filters filters) {

    throw new UnsupportedOperationException();
  }

  @Override
  public Status launchTasks(Collection<OfferID> offerIds, Collection<TaskInfo> tasks) {
    assertNotStopped();

    OfferID id = Iterables.getOnlyElement(offerIds);
    Offer offer = sentOffers.remove(id);
    checkState(offer != null, "Offer " + id + " is invalid.");

    final TaskInfo task = Iterables.getOnlyElement(tasks);
    synchronized (activeTasks) {
      checkState(
          !activeTasks.containsKey(task.getTaskId()),
          "Task " + task.getTaskId() + " already exists.");
      activeTasks.put(task.getTaskId(), new Task(offer, task));
    }

    executor.schedule(
        new Runnable() {
          @Override
          public void run() {
            Futures.getUnchecked(schedulerFuture).statusUpdate(
                FakeMaster.this,
                TaskStatus.newBuilder()
                    .setTaskId(task.getTaskId())
                    .setState(TaskState.TASK_RUNNING)
                    .build());
          }
        },
        1,
        TimeUnit.SECONDS);

    return Status.DRIVER_RUNNING;
  }

  @Override
  public Status launchTasks(OfferID offerId, Collection<TaskInfo> tasks, Filters filters) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Status launchTasks(OfferID offerId, Collection<TaskInfo> tasks) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Status killTask(TaskID taskId) {
    assertNotStopped();

    Task task = activeTasks.remove(taskId);
    checkState(task != null, "Task " + taskId + " not found.");
    idleOffers.put(task.getOffer().getId(), task.getOffer());

    Futures.getUnchecked(schedulerFuture).statusUpdate(this, TaskStatus.newBuilder()
        .setTaskId(taskId)
        .setState(TaskState.TASK_FINISHED)
        .build());

    return Status.DRIVER_RUNNING;
  }

  @Override
  public Status declineOffer(OfferID offerId, Filters filters) {
    assertNotStopped();

    throw new UnsupportedOperationException();
  }

  @Override
  public Status declineOffer(OfferID offerId) {
    assertNotStopped();
    return null;
  }

  @Override
  public Status reviveOffers() {
    assertNotStopped();
    throw new UnsupportedOperationException();
  }

  @Override
  public Status acknowledgeStatusUpdate(TaskStatus status) {
    assertNotStopped();
    throw new UnsupportedOperationException();
  }

  @Override
  public Status sendFrameworkMessage(ExecutorID executorId, SlaveID slaveId, byte[] data) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Status reconcileTasks(Collection<TaskStatus> statuses) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Status acceptOffers(
      Collection<OfferID> offerIds,
      Collection<Offer.Operation> operations,
      Filters filters) {
    throw new UnsupportedOperationException();
  }

  private static final class Task {
    private final Offer offer;
    private final TaskInfo task;

    private Task(Offer offer, TaskInfo task) {
      this.offer = offer;
      this.task = task;
    }

    Offer getOffer() {
      return offer;
    }

    TaskInfo getTask() {
      return task;
    }
  }
}

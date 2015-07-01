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
package org.apache.aurora.scheduler.async;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.RateLimiter;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.BackoffStrategy;

import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.async.OfferManager.OfferManagerImpl;
import org.apache.aurora.scheduler.async.OfferManager.OfferReturnDelay;
import org.apache.aurora.scheduler.async.TaskScheduler.TaskSchedulerImpl;
import org.apache.aurora.scheduler.async.preemptor.BiCache;
import org.apache.aurora.scheduler.async.preemptor.Preemptor;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.events.PubsubEvent.TasksDeleted;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.mesos.Driver;
import org.apache.aurora.scheduler.state.MaintenanceController;
import org.apache.aurora.scheduler.state.StateChangeResult;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.state.TaskAssigner;
import org.apache.aurora.scheduler.state.TaskAssigner.Assignment;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.db.DbUtil;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.MaintenanceMode.DRAINED;
import static org.apache.aurora.gen.MaintenanceMode.DRAINING;
import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.apache.aurora.gen.MaintenanceMode.SCHEDULED;
import static org.apache.aurora.gen.ScheduleStatus.FINISHED;
import static org.apache.aurora.gen.ScheduleStatus.INIT;
import static org.apache.aurora.gen.ScheduleStatus.KILLED;
import static org.apache.aurora.gen.ScheduleStatus.LOST;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.scheduler.base.TaskTestUtil.makeTask;
import static org.apache.aurora.scheduler.filter.AttributeAggregate.EMPTY;
import static org.apache.mesos.Protos.Offer;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertEquals;

/**
 * TODO(wfarner): Break this test up to independently test TaskSchedulerImpl and OfferQueueImpl.
 */
public class TaskSchedulerTest extends EasyMockTest {

  private static final long FIRST_SCHEDULE_DELAY_MS = 1L;

  private static final HostOffer OFFER_A =  makeOffer("OFFER_A", "HOST_A", NONE);
  private static final HostOffer OFFER_B = makeOffer("OFFER_B", "HOST_B", SCHEDULED);
  private static final HostOffer OFFER_C = makeOffer("OFFER_C", "HOST_C", DRAINING);
  private static final HostOffer OFFER_D = makeOffer("OFFER_D", "HOST_D", DRAINED);
  private static final String SLAVE_A = OFFER_A.getOffer().getSlaveId().getValue();
  private static final String SLAVE_B = OFFER_B.getOffer().getSlaveId().getValue();
  private static final String SLAVE_C = OFFER_C.getOffer().getSlaveId().getValue();

  private Storage storage;

  private MaintenanceController maintenance;
  private StateManager stateManager;
  private TaskAssigner assigner;
  private BackoffStrategy retryStrategy;
  private Driver driver;
  private ScheduledExecutorService executor;
  private ScheduledFuture<?> future;
  private OfferReturnDelay returnDelay;
  private OfferManager offerManager;
  private TaskGroups taskGroups;
  private RescheduleCalculator rescheduleCalculator;
  private Preemptor preemptor;
  private BiCache<String, TaskGroupKey> reservations;

  @Before
  public void setUp() {
    storage = DbUtil.createStorage();
    maintenance = createMock(MaintenanceController.class);
    stateManager = createMock(StateManager.class);
    assigner = createMock(TaskAssigner.class);
    retryStrategy = createMock(BackoffStrategy.class);
    driver = createMock(Driver.class);
    executor = createMock(ScheduledExecutorService.class);
    future = createMock(ScheduledFuture.class);
    returnDelay = createMock(OfferReturnDelay.class);
    rescheduleCalculator = createMock(RescheduleCalculator.class);
    preemptor = createMock(Preemptor.class);
    reservations = createMock(new Clazz<BiCache<String, TaskGroupKey>>() { });
  }

  private void replayAndCreateScheduler() {
    control.replay();
    offerManager = new OfferManagerImpl(driver, returnDelay, executor);
    TaskScheduler scheduler = new TaskSchedulerImpl(storage,
        stateManager,
        assigner,
        offerManager,
        preemptor,
        reservations);
    taskGroups = new TaskGroups(
        executor,
        Amount.of(FIRST_SCHEDULE_DELAY_MS, Time.MILLISECONDS),
        retryStrategy,
        RateLimiter.create(100),
        scheduler,
        rescheduleCalculator);
  }

  private Capture<Runnable> expectOffer() {
    return expectOfferDeclineIn(10);
  }

  private Capture<Runnable> expectOfferDeclineIn(long delayMillis) {
    expect(returnDelay.get()).andReturn(Amount.of(delayMillis, Time.MILLISECONDS));
    Capture<Runnable> runnable = createCapture();
    executor.schedule(capture(runnable), eq(delayMillis), eq(TimeUnit.MILLISECONDS));
    expectLastCall().andReturn(createMock(ScheduledFuture.class));
    return runnable;
  }

  private void changeState(
      IScheduledTask task,
      ScheduleStatus oldState,
      ScheduleStatus newState) {

    final IScheduledTask copy = IScheduledTask.build(task.newBuilder().setStatus(newState));
    // Insert the task if it doesn't already exist.
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(MutableStoreProvider storeProvider) {
        TaskStore.Mutable taskStore = storeProvider.getUnsafeTaskStore();
        if (Iterables.isEmpty(taskStore.fetchTasks(Query.taskScoped(Tasks.id(copy))))) {
          taskStore.saveTasks(ImmutableSet.of(copy));
        }
      }
    });
    taskGroups.taskChangedState(TaskStateChange.transition(copy, oldState));
  }

  private Capture<Runnable> expectTaskRetryIn(long penaltyMs) {
    Capture<Runnable> capture = createCapture();
    executor.schedule(
        capture(capture),
        eq(penaltyMs),
        eq(TimeUnit.MILLISECONDS));
    expectLastCall().andReturn(future);
    return capture;
  }

  private Capture<Runnable> expectTaskGroupBackoff(long previousPenaltyMs, long nextPenaltyMs) {
    expect(retryStrategy.calculateBackoffMs(previousPenaltyMs)).andReturn(nextPenaltyMs);
    return expectTaskRetryIn(nextPenaltyMs);
  }

  @Test
  public void testNoTasks() {
    expectAnyMaintenanceCalls();
    expectOfferDeclineIn(10);
    expectOfferDeclineIn(10);

    replayAndCreateScheduler();

    offerManager.addOffer(OFFER_A);
    offerManager.addOffer(OFFER_B);
  }

  @Test
  public void testNoOffers() {
    Capture<Runnable> timeoutCapture = expectTaskRetryIn(FIRST_SCHEDULE_DELAY_MS);
    expectTaskGroupBackoff(FIRST_SCHEDULE_DELAY_MS, 10);
    IScheduledTask task = createTask("a");
    expectPreemptorCall(task.getAssignedTask());
    expectReservationCheck(task);

    replayAndCreateScheduler();

    changeState(task, INIT, PENDING);
    timeoutCapture.getValue().run();
  }

  private IScheduledTask createTask(String taskId) {
    return createTask(taskId, null);
  }

  private IScheduledTask createTask(String taskId, @Nullable ScheduleStatus status) {
    return setStatus(makeTask(taskId, TaskTestUtil.JOB), status);
  }

  private IScheduledTask setStatus(IScheduledTask task, @Nullable ScheduleStatus status) {
    return IScheduledTask.build(task.newBuilder().setStatus(status));
  }

  @Test
  public void testLoadFromStorage() {
    final IScheduledTask a = createTask("a", KILLED);
    final IScheduledTask b = createTask("b", PENDING);
    final IScheduledTask c = createTask("c", RUNNING);

    expect(rescheduleCalculator.getStartupScheduleDelayMs(b)).andReturn(10L);
    expectTaskRetryIn(10);

    replayAndCreateScheduler();

    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(MutableStoreProvider store) {
        store.getUnsafeTaskStore().saveTasks(ImmutableSet.of(a, b, c));
      }
    });
    for (IScheduledTask task : ImmutableList.of(a, b, c)) {
      taskGroups.taskChangedState(TaskStateChange.initialized(task));
    }
    changeState(c, RUNNING, FINISHED);
  }

  @Test
  public void testTaskMissing() {
    Capture<Runnable> timeoutCapture = expectTaskRetryIn(FIRST_SCHEDULE_DELAY_MS);

    replayAndCreateScheduler();

    taskGroups.taskChangedState(TaskStateChange.transition(createTask("a", PENDING), INIT));
    timeoutCapture.getValue().run();
  }

  private IExpectationSetters<Assignment> expectMaybeAssign(
      HostOffer offer,
      IScheduledTask task,
      AttributeAggregate jobAggregate) {

    return expect(assigner.maybeAssign(
        EasyMock.anyObject(),
        eq(offer),
        eq(new ResourceRequest(task.getAssignedTask().getTask(), jobAggregate)),
        eq(Tasks.id(task))));
  }

  private IExpectationSetters<?> expectNoReservation(String slaveId) {
    return expect(reservations.get(slaveId)).andReturn(Optional.absent());
  }

  private IExpectationSetters<?> expectReservationCheck(IScheduledTask task) {
    return expect(reservations.getByValue(TaskGroupKey.from(task.getAssignedTask().getTask())))
        .andReturn(ImmutableSet.of());
  }

  @Test
  public void testTaskAssigned() {
    expectAnyMaintenanceCalls();
    expectOfferDeclineIn(10);

    IScheduledTask taskA = createTask("a", PENDING);
    TaskInfo mesosTask = makeTaskInfo(taskA);

    Capture<Runnable> timeoutCapture = expectTaskRetryIn(FIRST_SCHEDULE_DELAY_MS);
    expectNoReservation(SLAVE_A).times(2);
    expectReservationCheck(taskA);
    expectMaybeAssign(OFFER_A, taskA, EMPTY).andReturn(Assignment.failure());
    expectPreemptorCall(taskA.getAssignedTask());

    Capture<Runnable> timeoutCapture2 = expectTaskGroupBackoff(FIRST_SCHEDULE_DELAY_MS, 10);
    expectMaybeAssign(OFFER_A, taskA, EMPTY).andReturn(Assignment.success(mesosTask));
    driver.launchTask(OFFER_A.getOffer().getId(), mesosTask);

    Capture<Runnable> timeoutCapture3 = expectTaskRetryIn(FIRST_SCHEDULE_DELAY_MS);
    expectTaskGroupBackoff(FIRST_SCHEDULE_DELAY_MS, 10);
    IScheduledTask taskB = createTask("b");
    expectReservationCheck(taskB);
    expectPreemptorCall(taskB.getAssignedTask());

    replayAndCreateScheduler();

    offerManager.addOffer(OFFER_A);
    changeState(taskA, INIT, PENDING);
    timeoutCapture.getValue().run();
    timeoutCapture2.getValue().run();

    // Ensure the offer was consumed.
    changeState(taskB, INIT, PENDING);
    timeoutCapture3.getValue().run();
  }

  @Test
  public void testDriverNotReady() {
    IScheduledTask task = createTask("a", PENDING);
    TaskInfo mesosTask = TaskInfo.newBuilder()
        .setName(Tasks.id(task))
        .setTaskId(TaskID.newBuilder().setValue(Tasks.id(task)))
        .setSlaveId(SlaveID.newBuilder().setValue("slaveId"))
        .build();

    Capture<Runnable> timeoutCapture = expectTaskRetryIn(FIRST_SCHEDULE_DELAY_MS);
    expectAnyMaintenanceCalls();
    expectOfferDeclineIn(10);
    expectNoReservation(SLAVE_A);
    expectMaybeAssign(OFFER_A, task, EMPTY).andReturn(Assignment.success(mesosTask));
    driver.launchTask(OFFER_A.getOffer().getId(), mesosTask);
    expectLastCall().andThrow(new IllegalStateException("Driver not ready."));
    expect(stateManager.changeState(
        EasyMock.anyObject(),
        eq("a"),
        eq(Optional.of(PENDING)),
        eq(LOST),
        eq(TaskSchedulerImpl.LAUNCH_FAILED_MSG)))
        .andReturn(StateChangeResult.SUCCESS);

    replayAndCreateScheduler();

    changeState(task, INIT, PENDING);
    offerManager.addOffer(OFFER_A);
    timeoutCapture.getValue().run();
  }

  @Test
  public void testStorageException() {
    IScheduledTask task = createTask("a", PENDING);
    TaskInfo mesosTask = TaskInfo.newBuilder()
        .setName(Tasks.id(task))
        .setTaskId(TaskID.newBuilder().setValue(Tasks.id(task)))
        .setSlaveId(SlaveID.newBuilder().setValue("slaveId"))
        .build();

    Capture<Runnable> timeoutCapture = expectTaskRetryIn(FIRST_SCHEDULE_DELAY_MS);
    expectAnyMaintenanceCalls();
    expectOfferDeclineIn(10);
    expectNoReservation(SLAVE_A).times(2);
    expectMaybeAssign(OFFER_A, task, EMPTY).andThrow(new StorageException("Injected failure."));

    Capture<Runnable> timeoutCapture2 = expectTaskGroupBackoff(FIRST_SCHEDULE_DELAY_MS, 10);
    expectMaybeAssign(OFFER_A, task, EMPTY).andReturn(Assignment.success(mesosTask));
    driver.launchTask(OFFER_A.getOffer().getId(), mesosTask);
    expectLastCall();

    replayAndCreateScheduler();

    changeState(task, INIT, PENDING);
    offerManager.addOffer(OFFER_A);
    timeoutCapture.getValue().run();
    timeoutCapture2.getValue().run();
  }

  @Test
  public void testExpiration() {
    IScheduledTask task = createTask("a", PENDING);

    Capture<Runnable> timeoutCapture = expectTaskRetryIn(FIRST_SCHEDULE_DELAY_MS);
    Capture<Runnable> offerExpirationCapture = expectOfferDeclineIn(10);
    expectAnyMaintenanceCalls();
    expectNoReservation(SLAVE_A);
    expectReservationCheck(task).times(2);
    expectMaybeAssign(OFFER_A, task, EMPTY).andReturn(Assignment.failure());
    Capture<Runnable> timeoutCapture2 = expectTaskGroupBackoff(FIRST_SCHEDULE_DELAY_MS, 10);
    expectPreemptorCall(task.getAssignedTask());
    driver.declineOffer(OFFER_A.getOffer().getId());
    expectTaskGroupBackoff(10, 20);
    expectPreemptorCall(task.getAssignedTask());

    replayAndCreateScheduler();

    changeState(task, INIT, PENDING);
    offerManager.addOffer(OFFER_A);
    timeoutCapture.getValue().run();
    offerExpirationCapture.getValue().run();
    timeoutCapture2.getValue().run();
  }

  @Test
  public void testOneOfferPerSlave() {
    expectAnyMaintenanceCalls();
    Capture<Runnable> offerExpirationCapture = expectOfferDeclineIn(10);

    HostOffer offerAB = new HostOffer(
        Offers.makeOffer("OFFER_B").toBuilder().setSlaveId(OFFER_A.getOffer().getSlaveId()).build(),
        IHostAttributes.build(new HostAttributes()));

    driver.declineOffer(OFFER_A.getOffer().getId());
    driver.declineOffer(offerAB.getOffer().getId());

    replayAndCreateScheduler();

    offerManager.addOffer(OFFER_A);
    offerManager.addOffer(offerAB);
    offerExpirationCapture.getValue().run();
  }

  @Test
  public void testDontDeclineAcceptedOffer() throws OfferManager.LaunchException {
    expectAnyMaintenanceCalls();
    Capture<Runnable> offerExpirationCapture = expectOfferDeclineIn(10);

    Function<HostOffer, Assignment> offerAcceptor =
        createMock(new Clazz<Function<HostOffer, Assignment>>() { });
    final TaskInfo taskInfo = TaskInfo.getDefaultInstance();
    expect(offerAcceptor.apply(OFFER_A)).andReturn(Assignment.success(taskInfo));
    driver.launchTask(OFFER_A.getOffer().getId(), taskInfo);

    replayAndCreateScheduler();

    offerManager.addOffer(OFFER_A);
    offerManager.launchFirst(offerAcceptor, TaskGroupKey.from(ITaskConfig.build(new TaskConfig())));
    offerExpirationCapture.getValue().run();
  }

  @Test
  public void testBasicMaintenancePreferences() {
    expectOffer();
    expectOffer();
    expectOffer();
    expectOffer();

    IScheduledTask taskA = createTask("A", PENDING);
    TaskInfo mesosTaskA = makeTaskInfo(taskA);
    expectNoReservation(SLAVE_A);
    expectMaybeAssign(OFFER_A, taskA, EMPTY).andReturn(Assignment.success(mesosTaskA));
    driver.launchTask(OFFER_A.getOffer().getId(), mesosTaskA);
    Capture<Runnable> captureA = expectTaskRetryIn(FIRST_SCHEDULE_DELAY_MS);

    IScheduledTask taskB = createTask("B", PENDING);
    TaskInfo mesosTaskB = makeTaskInfo(taskB);
    expectNoReservation(SLAVE_B);
    expectMaybeAssign(OFFER_B, taskB, EMPTY).andReturn(Assignment.success(mesosTaskB));
    driver.launchTask(OFFER_B.getOffer().getId(), mesosTaskB);
    Capture<Runnable> captureB = expectTaskRetryIn(FIRST_SCHEDULE_DELAY_MS);

    replayAndCreateScheduler();

    offerManager.addOffer(OFFER_D);
    offerManager.addOffer(OFFER_C);
    offerManager.addOffer(OFFER_B);
    offerManager.addOffer(OFFER_A);

    changeState(taskA, INIT, PENDING);
    captureA.getValue().run();

    changeState(taskB, INIT, PENDING);
    captureB.getValue().run();
  }

  @Test
  public void testChangingMaintenancePreferences() {
    expectOffer();
    expectOffer();
    expectOffer();

    IScheduledTask taskA = createTask("A", PENDING);
    TaskInfo mesosTaskA = makeTaskInfo(taskA);
    expectNoReservation(SLAVE_B);
    expectMaybeAssign(OFFER_B, taskA, EMPTY).andReturn(Assignment.success(mesosTaskA));
    driver.launchTask(OFFER_B.getOffer().getId(), mesosTaskA);
    Capture<Runnable> captureA = expectTaskRetryIn(FIRST_SCHEDULE_DELAY_MS);

    IScheduledTask taskB = createTask("B", PENDING);
    TaskInfo mesosTaskB = makeTaskInfo(taskB);
    HostOffer updatedOfferC = new HostOffer(
        OFFER_C.getOffer(),
        IHostAttributes.build(OFFER_C.getAttributes().newBuilder().setMode(NONE)));
    expectNoReservation(SLAVE_C);
    expectMaybeAssign(updatedOfferC, taskB, EMPTY).andReturn(Assignment.success(mesosTaskB));
    driver.launchTask(OFFER_C.getOffer().getId(), mesosTaskB);
    Capture<Runnable> captureB = expectTaskRetryIn(FIRST_SCHEDULE_DELAY_MS);

    replayAndCreateScheduler();

    offerManager.addOffer(OFFER_A);
    offerManager.addOffer(OFFER_B);
    offerManager.addOffer(OFFER_C);

    // Initially, we'd expect the offers to be consumed in order (A, B), with (C) unschedulable

    // Expected order now (B), with (C, A) unschedulable
    changeHostMaintenanceState(OFFER_A.getAttributes(), DRAINING);
    changeState(taskA, INIT, PENDING);
    captureA.getValue().run();

    // Expected order now (C), with (A) unschedulable and (B) already consumed
    changeHostMaintenanceState(OFFER_C.getAttributes(), NONE);
    changeState(taskB, INIT, PENDING);
    captureB.getValue().run();
  }

  private Capture<String> expectTaskScheduled(IScheduledTask task) {
    TaskInfo mesosTask = makeTaskInfo(task);
    Capture<String> taskId = createCapture();
    expect(assigner.maybeAssign(
        EasyMock.anyObject(),
        EasyMock.anyObject(),
        EasyMock.anyObject(),
        capture(taskId))).andReturn(Assignment.success(mesosTask));
    driver.launchTask(EasyMock.anyObject(), eq(mesosTask));
    return taskId;
  }

  @Test
  public void testResistsStarvation() {
    // TODO(wfarner): This test requires intimate knowledge of the way futures are used inside
    // TaskScheduler.  It's time to test using a real ScheduledExecutorService.

    expectAnyMaintenanceCalls();

    IScheduledTask jobA0 = setStatus(makeTask("a0", JobKeys.from("a", "b", "c")), PENDING);

    ScheduledTask jobA1Builder = jobA0.newBuilder();
    jobA1Builder.getAssignedTask().setTaskId("a1");
    jobA1Builder.getAssignedTask().setInstanceId(1);
    IScheduledTask jobA1 = IScheduledTask.build(jobA1Builder);

    ScheduledTask jobA2Builder = jobA0.newBuilder();
    jobA2Builder.getAssignedTask().setTaskId("a2");
    jobA2Builder.getAssignedTask().setInstanceId(2);
    IScheduledTask jobA2 = IScheduledTask.build(jobA2Builder);

    IScheduledTask jobB0 = setStatus(makeTask("b0", JobKeys.from("d", "e", "f")), PENDING);

    expectNoReservation(SLAVE_A);
    expectNoReservation(SLAVE_B);

    expectOfferDeclineIn(10);
    expectOfferDeclineIn(10);
    expectOfferDeclineIn(10);
    expectOfferDeclineIn(10);

    Capture<Runnable> timeoutA = expectTaskRetryIn(FIRST_SCHEDULE_DELAY_MS);
    Capture<Runnable> timeoutB = expectTaskRetryIn(FIRST_SCHEDULE_DELAY_MS);

    Capture<String> firstScheduled = expectTaskScheduled(jobA0);
    Capture<String> secondScheduled = expectTaskScheduled(jobB0);

    // Expect another watch of the task group for job A.
    expectTaskRetryIn(FIRST_SCHEDULE_DELAY_MS);

    replayAndCreateScheduler();

    offerManager.addOffer(OFFER_A);
    offerManager.addOffer(OFFER_B);
    offerManager.addOffer(OFFER_C);
    offerManager.addOffer(OFFER_D);
    changeState(jobA0, INIT, PENDING);
    changeState(jobA1, INIT, PENDING);
    changeState(jobA2, INIT, PENDING);
    changeState(jobB0, INIT, PENDING);
    timeoutA.getValue().run();
    timeoutB.getValue().run();
    assertEquals(
        ImmutableSet.of(Tasks.id(jobA0), Tasks.id(jobB0)),
        ImmutableSet.of(firstScheduled.getValue(), secondScheduled.getValue()));
  }

  @Test
  public void testTaskDeleted() {
    expectAnyMaintenanceCalls();
    expectOfferDeclineIn(10);

    final IScheduledTask task = createTask("a", PENDING);

    Capture<Runnable> timeoutCapture = expectTaskRetryIn(FIRST_SCHEDULE_DELAY_MS);
    expectNoReservation(SLAVE_A);
    expectMaybeAssign(OFFER_A, task, EMPTY).andReturn(Assignment.failure());
    expectTaskGroupBackoff(FIRST_SCHEDULE_DELAY_MS, 20);
    expectReservationCheck(task);
    expectPreemptorCall(task.getAssignedTask());

    replayAndCreateScheduler();

    offerManager.addOffer(OFFER_A);
    changeState(task, INIT, PENDING);
    timeoutCapture.getValue().run();

    // Ensure the offer was consumed.
    changeState(task, INIT, PENDING);
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().deleteTasks(Tasks.ids(task));
      }
    });
    taskGroups.tasksDeleted(new TasksDeleted(ImmutableSet.of(task)));
    timeoutCapture.getValue().run();
  }

  private TaskInfo makeTaskInfo(IScheduledTask task) {
    return TaskInfo.newBuilder()
        .setName(Tasks.id(task))
        .setTaskId(TaskID.newBuilder().setValue(Tasks.id(task)))
        .setSlaveId(SlaveID.newBuilder().setValue("slave-id" + task.toString()))
        .build();
  }

  private void expectAnyMaintenanceCalls() {
    expect(maintenance.getMode(isA(String.class))).andReturn(NONE).anyTimes();
  }

  private void changeHostMaintenanceState(IHostAttributes attributes, MaintenanceMode mode) {
    offerManager.hostAttributesChanged(new PubsubEvent.HostAttributesChanged(
        IHostAttributes.build(attributes.newBuilder().setMode(mode))));
  }

  private static HostOffer makeOffer(String offerId, String hostName, MaintenanceMode mode) {
    Offer offer = Offers.makeOffer(offerId, hostName);
    return new HostOffer(
        offer,
        IHostAttributes.build(new HostAttributes()
            .setHost(hostName)
            .setSlaveId(offer.getSlaveId().getValue())
            .setAttributes(ImmutableSet.of())
            .setMode(mode)));
  }

  private void expectPreemptorCall(IAssignedTask task) {
    expect(preemptor.attemptPreemptionFor(
        eq(task),
        eq(EMPTY),
        EasyMock.anyObject())).andReturn(Optional.absent());
  }
}

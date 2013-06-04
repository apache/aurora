package com.twitter.mesos.scheduler.async;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.BackoffStrategy;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.HostStatus;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.MaintenanceMode;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.Driver;
import com.twitter.mesos.scheduler.MaintenanceController;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.StateManager;
import com.twitter.mesos.scheduler.TaskAssigner;
import com.twitter.mesos.scheduler.async.OfferQueue.OfferQueueImpl;
import com.twitter.mesos.scheduler.async.OfferQueue.OfferReturnDelay;
import com.twitter.mesos.scheduler.async.TaskScheduler.TaskSchedulerImpl;
import com.twitter.mesos.scheduler.events.PubsubEvent.HostMaintenanceStateChange;
import com.twitter.mesos.scheduler.events.PubsubEvent.StorageStarted;
import com.twitter.mesos.scheduler.events.PubsubEvent.TaskStateChange;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;
import com.twitter.mesos.scheduler.storage.Storage.StorageException;
import com.twitter.mesos.scheduler.storage.mem.MemStorage;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;

import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.INIT;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED;
import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;

/**
 * TODO(wfarner): Break this test up to independently test TaskSchedulerImpl and OfferQueueImpl.
 */
public class TaskSchedulerImplTest extends EasyMockTest {

  private static final Offer OFFER_A = Offers.makeOffer("OFFER_A", "HOST_A");
  private static final Offer OFFER_B = Offers.makeOffer("OFFER_B", "HOST_B");
  private static final Offer OFFER_C = Offers.makeOffer("OFFER_C", "HOST_C");
  private static final Offer OFFER_D = Offers.makeOffer("OFFER_D", "HOST_D");

  private Storage storage;
  private MaintenanceController maintenance;
  private StateManager stateManager;
  private TaskAssigner assigner;
  private BackoffStrategy retryStrategy;
  private Driver driver;
  private ScheduledExecutorService executor;
  private ScheduledFuture<?> future;
  private OfferReturnDelay returnDelay;
  private TaskSchedulerImpl scheduler;
  private OfferQueue offerQueue;

  @Before
  public void setUp() {
    storage = MemStorage.newEmptyStorage();
    maintenance = createMock(MaintenanceController.class);
    stateManager = createMock(StateManager.class);
    assigner = createMock(TaskAssigner.class);
    retryStrategy = createMock(BackoffStrategy.class);
    driver = createMock(Driver.class);
    executor = createMock(ScheduledExecutorService.class);
    future = createMock(ScheduledFuture.class);
    returnDelay = createMock(OfferReturnDelay.class);
  }

  private void replayAndCreateScheduler() {
    control.replay();
    offerQueue = new OfferQueueImpl(driver, returnDelay, executor, maintenance);
    scheduler = new TaskSchedulerImpl(
        storage,
        stateManager,
        assigner,
        retryStrategy,
        executor,
        offerQueue);
  }

  private Capture<Runnable> expectOffer() {
    return expectOfferDeclineIn(10);
  }

  private Capture<Runnable> expectOfferDeclineIn(int delayMillis) {
    expect(returnDelay.get()).andReturn(Amount.of(delayMillis, Time.MILLISECONDS));
    Capture<Runnable> runnable = createCapture();
    executor.schedule(capture(runnable), eq((long) delayMillis), eq(TimeUnit.MILLISECONDS));
    expectLastCall().andReturn(createMock(ScheduledFuture.class));
    return runnable;
  }

  private void changeState(String taskId, ScheduleStatus oldState, ScheduleStatus newState) {
    ScheduledTask task = new ScheduledTask()
        .setStatus(newState)
        .setAssignedTask(new AssignedTask().setTaskId(taskId));
    scheduler.taskChangedState(new TaskStateChange(task, oldState));
  }

  private Capture<Runnable> expectTaskWatch(long previousPenaltyMs, long nextPenaltyMs) {
    expect(retryStrategy.calculateBackoffMs(previousPenaltyMs)).andReturn(nextPenaltyMs);
    Capture<Runnable> capture = createCapture();
    executor.schedule(
        EasyMock.capture(capture),
        eq(nextPenaltyMs),
        eq(TimeUnit.MILLISECONDS));
    expectLastCall().andReturn(future);
    return capture;
  }

  private Capture<Runnable> expectTaskWatch(long nextPenaltyMs) {
    return expectTaskWatch(0, nextPenaltyMs);
  }

  @Test
  public void testNoTasks() {
    expectAnyMaintenanceCalls();
    expectOfferDeclineIn(10);
    expectOfferDeclineIn(10);

    replayAndCreateScheduler();

    offerQueue.addOffer(OFFER_A);
    offerQueue.addOffer(OFFER_B);
  }

  @Test
  public void testNoOffers() {
    Capture<Runnable> timeoutCapture = expectTaskWatch(10);
    expectTaskWatch(10, 20);

    replayAndCreateScheduler();

    insertTasks(makeTask("a", PENDING));
    changeState("a", INIT, PENDING);
    timeoutCapture.getValue().run();
  }

  private ScheduledTask makeTask(String taskId, ScheduleStatus status) {
    return new ScheduledTask()
        .setStatus(status)
        .setAssignedTask(new AssignedTask()
            .setTaskId(taskId)
            .setTask(new TwitterTaskInfo()
                .setOwner(new Identity().setRole("role-" + taskId).setUser("user-" + taskId))));
  }

  private void insertTasks(final ScheduledTask task, final ScheduledTask... tasks) {
    storage.writeOp(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider store) {
        store.getUnsafeTaskStore().saveTasks(
            ImmutableSet.<ScheduledTask>builder().add(task).add(tasks).build());
      }
    });
  }

  @Test
  public void testLoadFromStorage() {
    expectTaskWatch(10);

    replayAndCreateScheduler();

    insertTasks(
        makeTask("a", KILLED),
        makeTask("b", PENDING),
        makeTask("c", RUNNING));
    scheduler.storageStarted(new StorageStarted());
    changeState("c", RUNNING, FINISHED);
  }

  @Test
  public void testTaskMissing() {
    Capture<Runnable> timeoutCapture = expectTaskWatch(10);

    replayAndCreateScheduler();

    changeState("a", INIT, PENDING);
    timeoutCapture.getValue().run();
  }

  @Test
  public void testTaskAssigned() {
    expectAnyMaintenanceCalls();
    expectOfferDeclineIn(10);

    ScheduledTask task = makeTask("a", PENDING);
    TaskInfo mesosTask = makeTaskInfo(task);

    Capture<Runnable> timeoutCapture = expectTaskWatch(10);
    expect(assigner.maybeAssign(OFFER_A, task)).andReturn(Optional.<TaskInfo>absent());

    Capture<Runnable> timeoutCapture2 = expectTaskWatch(10, 20);
    expect(assigner.maybeAssign(OFFER_A, task)).andReturn(Optional.of(mesosTask));
    driver.launchTask(OFFER_A.getId(), mesosTask);

    Capture<Runnable> timeoutCapture3 = expectTaskWatch(10);
    expectTaskWatch(10, 20);

    replayAndCreateScheduler();

    offerQueue.addOffer(OFFER_A);
    insertTasks(task);
    changeState("a", INIT, PENDING);
    timeoutCapture.getValue().run();
    timeoutCapture2.getValue().run();

    // Ensure the offer was consumed
    insertTasks(makeTask("b", PENDING));
    changeState("b", INIT, PENDING);
    timeoutCapture3.getValue().run();
  }

  @Test
  public void testDriverNotReady() {
    ScheduledTask task = makeTask("a", PENDING);
    TaskInfo mesosTask = TaskInfo.newBuilder()
        .setName(Tasks.id(task))
        .setTaskId(TaskID.newBuilder().setValue(Tasks.id(task)))
        .setSlaveId(SlaveID.newBuilder().setValue("slaveId"))
        .build();

    Capture<Runnable> timeoutCapture = expectTaskWatch(10);
    expectAnyMaintenanceCalls();
    expectOfferDeclineIn(10);
    expect(assigner.maybeAssign(OFFER_A, task)).andReturn(Optional.of(mesosTask));
    driver.launchTask(OFFER_A.getId(), mesosTask);
    expectLastCall().andThrow(new IllegalStateException("Driver not ready."));
    expect(stateManager.changeState(
        Query.taskScoped("a").byStatus(PENDING).get(),
        LOST,
        TaskSchedulerImpl.LAUNCH_FAILED_MSG))
        .andReturn(1);

    replayAndCreateScheduler();

    insertTasks(task);
    changeState("a", INIT, PENDING);
    offerQueue.addOffer(OFFER_A);
    timeoutCapture.getValue().run();
  }

  @Test
  public void testStorageException() {
    ScheduledTask task = makeTask("a", PENDING);
    TaskInfo mesosTask = TaskInfo.newBuilder()
        .setName(Tasks.id(task))
        .setTaskId(TaskID.newBuilder().setValue(Tasks.id(task)))
        .setSlaveId(SlaveID.newBuilder().setValue("slaveId"))
        .build();

    Capture<Runnable> timeoutCapture = expectTaskWatch(10);
    expectAnyMaintenanceCalls();
    expectOfferDeclineIn(10);
    expect(assigner.maybeAssign(OFFER_A, task)).andThrow(new StorageException("Injected failure."));

    Capture<Runnable> timeoutCapture2 = expectTaskWatch(10, 20);
    expect(assigner.maybeAssign(OFFER_A, task)).andReturn(Optional.of(mesosTask));
    driver.launchTask(OFFER_A.getId(), mesosTask);
    expectLastCall();

    replayAndCreateScheduler();

    insertTasks(task);
    changeState("a", INIT, PENDING);
    offerQueue.addOffer(OFFER_A);
    timeoutCapture.getValue().run();
    timeoutCapture2.getValue().run();
  }

  @Test
  public void testExpiration() {
    ScheduledTask task = makeTask("a", PENDING);

    Capture<Runnable> timeoutCapture = expectTaskWatch(10);
    Capture<Runnable> offerExpirationCapture = expectOfferDeclineIn(10);
    expectAnyMaintenanceCalls();
    expect(assigner.maybeAssign(OFFER_A, task)).andReturn(Optional.<TaskInfo>absent());
    Capture<Runnable> timeoutCapture2 = expectTaskWatch(10, 20);
    driver.declineOffer(OFFER_A.getId());
    expectTaskWatch(20, 30);

    replayAndCreateScheduler();

    insertTasks(task);
    changeState("a", INIT, PENDING);
    offerQueue.addOffer(OFFER_A);
    timeoutCapture.getValue().run();
    offerExpirationCapture.getValue().run();
    timeoutCapture2.getValue().run();
  }

  @Test
  public void testOneOfferPerSlave() {
    expectAnyMaintenanceCalls();
    Capture<Runnable> offerExpirationCapture = expectOfferDeclineIn(10);

    Offer offerAB =
        Offers.makeOffer("OFFER_B").toBuilder().setSlaveId(OFFER_A.getSlaveId()).build();

    driver.declineOffer(OFFER_A.getId());
    driver.declineOffer(offerAB.getId());

    // No attempt is made to avoid declining offers multiple times, so OFFER_A is declined twice.
    driver.declineOffer(OFFER_A.getId());

    replayAndCreateScheduler();

    offerQueue.addOffer(OFFER_A);
    offerQueue.addOffer(offerAB);
    offerExpirationCapture.getValue().run();
  }

  @Test
  public void testBasicMaintenancePreferences() {
    expectOffer();
    expect(maintenance.getMode("HOST_D")).andReturn(MaintenanceMode.DRAINED);
    expectOffer();
    expect(maintenance.getMode("HOST_C")).andReturn(MaintenanceMode.DRAINING);
    expectOffer();
    expect(maintenance.getMode("HOST_B")).andReturn(MaintenanceMode.SCHEDULED);
    expectOffer();
    expect(maintenance.getMode("HOST_A")).andReturn(MaintenanceMode.NONE);

    ScheduledTask taskA = makeTask("A", PENDING);
    TaskInfo mesosTaskA = makeTaskInfo(taskA);
    expect(assigner.maybeAssign(OFFER_A, taskA)).andReturn(Optional.of(mesosTaskA));
    driver.launchTask(OFFER_A.getId(), mesosTaskA);
    Capture<Runnable> captureA = expectTaskWatch(10);

    ScheduledTask taskB = makeTask("B", PENDING);
    TaskInfo mesosTaskB = makeTaskInfo(taskB);
    expect(assigner.maybeAssign(OFFER_B, taskB)).andReturn(Optional.of(mesosTaskB));
    driver.launchTask(OFFER_B.getId(), mesosTaskB);
    Capture<Runnable> captureB = expectTaskWatch(10);

    replayAndCreateScheduler();

    offerQueue.addOffer(OFFER_D);
    offerQueue.addOffer(OFFER_C);
    offerQueue.addOffer(OFFER_B);
    offerQueue.addOffer(OFFER_A);

    insertTasks(taskA);
    changeState("A", INIT, PENDING);
    captureA.getValue().run();

    insertTasks(taskB);
    changeState("B", INIT, PENDING);
    captureB.getValue().run();
  }

  @Test
  public void testChangingMaintenancePreferences() {
    expectOffer();
    expect(maintenance.getMode("HOST_A")).andReturn(MaintenanceMode.NONE);
    expectOffer();
    expect(maintenance.getMode("HOST_B")).andReturn(MaintenanceMode.SCHEDULED);
    expectOffer();
    expect(maintenance.getMode("HOST_C")).andReturn(MaintenanceMode.DRAINED);

    ScheduledTask taskA = makeTask("A", PENDING);
    TaskInfo mesosTaskA = makeTaskInfo(taskA);
    expect(assigner.maybeAssign(OFFER_B, taskA)).andReturn(Optional.of(mesosTaskA));
    driver.launchTask(OFFER_B.getId(), mesosTaskA);
    Capture<Runnable> captureA = expectTaskWatch(10);

    ScheduledTask taskB = makeTask("B", PENDING);
    TaskInfo mesosTaskB = makeTaskInfo(taskB);
    expect(assigner.maybeAssign(OFFER_C, taskB)).andReturn(Optional.of(mesosTaskB));
    driver.launchTask(OFFER_C.getId(), mesosTaskB);
    Capture<Runnable> captureB = expectTaskWatch(10);

    replayAndCreateScheduler();

    offerQueue.addOffer(OFFER_A);
    offerQueue.addOffer(OFFER_B);
    offerQueue.addOffer(OFFER_C);

    // Initially, we'd expect the offers to be consumed in order (A, B), with (C) unschedulable

    // Expected order now (B), with (C, A) unschedulable
    changeHostMaintenanceState("HOST_A", MaintenanceMode.DRAINING);
    insertTasks(taskA);
    changeState("A", INIT, PENDING);
    captureA.getValue().run();

    // Expected order now (C), with (A) unschedulable and (B) already consumed
    changeHostMaintenanceState("HOST_C", MaintenanceMode.NONE);
    insertTasks(taskB);
    changeState("B", INIT, PENDING);
    captureB.getValue().run();
  }

  private TaskInfo makeTaskInfo(ScheduledTask task) {
    return TaskInfo.newBuilder()
        .setName(Tasks.id(task))
        .setTaskId(TaskID.newBuilder().setValue(Tasks.id(task)))
        .setSlaveId(SlaveID.newBuilder().setValue("slave-id" + task.toString()))
        .build();
  }

  private void expectAnyMaintenanceCalls() {
    expect(maintenance.getMode(isA(String.class))).andReturn(MaintenanceMode.NONE).anyTimes();
  }

  private void changeHostMaintenanceState(String hostName, MaintenanceMode mode) {
    offerQueue.hostChangedState(new HostMaintenanceStateChange(new HostStatus(hostName, mode)));
  }
}

package com.twitter.mesos.scheduler.async;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
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
import com.twitter.mesos.scheduler.StateManager;
import com.twitter.mesos.scheduler.TaskAssigner;
import com.twitter.mesos.scheduler.async.OfferQueue.OfferQueueImpl;
import com.twitter.mesos.scheduler.async.OfferQueue.OfferReturnDelay;
import com.twitter.mesos.scheduler.async.TaskGroups.SchedulingAction;
import com.twitter.mesos.scheduler.base.Query;
import com.twitter.mesos.scheduler.events.PubsubEvent.HostMaintenanceStateChange;
import com.twitter.mesos.scheduler.events.PubsubEvent.StorageStarted;
import com.twitter.mesos.scheduler.events.PubsubEvent.TaskStateChange;
import com.twitter.mesos.scheduler.events.PubsubEvent.TasksDeleted;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;
import com.twitter.mesos.scheduler.storage.Storage.StorageException;
import com.twitter.mesos.scheduler.storage.TaskStore;
import com.twitter.mesos.scheduler.storage.mem.MemStorage;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;

import static org.junit.Assert.assertEquals;

import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.INIT;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED;
import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;

/**
 * TODO(wfarner): Break this test up to independently test TaskScheduler and OfferQueueImpl.
 */
public class TaskSchedulerTest extends EasyMockTest {

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
  private OfferQueue offerQueue;
  private TaskGroups taskGroups;

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
    RateLimiter rateLimiter = RateLimiter.create(1);
    SchedulingAction scheduler =
        new TaskScheduler(storage, stateManager, assigner, offerQueue);
    taskGroups = new TaskGroups(executor, storage, retryStrategy, rateLimiter, scheduler);
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

  private void changeState(
      ScheduledTask task,
      ScheduleStatus oldState,
      ScheduleStatus newState) {

    final ScheduledTask copy = task.deepCopy().setStatus(newState);
    // Insert the task if it doesn't already exist.
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        TaskStore.Mutable taskStore = storeProvider.getUnsafeTaskStore();
        if (taskStore.fetchTaskIds(Query.taskScoped(Tasks.id(copy))).isEmpty()) {
          taskStore.saveTasks(ImmutableSet.of(copy));
        }
      }
    });
    taskGroups.taskChangedState(new TaskStateChange(copy, oldState));
  }

  private Capture<Runnable> expectTaskRetryIn(long penaltyMs) {
    Capture<Runnable> capture = createCapture();
    executor.schedule(
        EasyMock.capture(capture),
        eq(penaltyMs),
        eq(TimeUnit.MILLISECONDS));
    expectLastCall().andReturn(future);
    return capture;
  }

  private Capture<Runnable> expectTaskBackoff(long previousPenaltyMs, long nextPenaltyMs) {
    expect(retryStrategy.calculateBackoffMs(previousPenaltyMs)).andReturn(nextPenaltyMs);
    return expectTaskRetryIn(nextPenaltyMs);
  }

  private Capture<Runnable> expectTaskBackoff(long nextPenaltyMs) {
    return expectTaskBackoff(0, nextPenaltyMs);
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
    Capture<Runnable> timeoutCapture = expectTaskBackoff(10);
    expectTaskBackoff(10, 20);

    replayAndCreateScheduler();

    changeState(makeTask("a"), INIT, PENDING);
    timeoutCapture.getValue().run();
  }

  private ScheduledTask makeTask(String taskId) {
    return new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setTaskId(taskId)
            .setTask(new TwitterTaskInfo()
                .setJobName("job-" + taskId)
                .setShardId(0)
                .setOwner(new Identity().setRole("role-" + taskId).setUser("user-" + taskId))));
  }


  private ScheduledTask makeTask(String taskId, ScheduleStatus status) {
    return makeTask(taskId).setStatus(status);
  }

  @Test
  public void testLoadFromStorage() {
    expectTaskBackoff(10);

    replayAndCreateScheduler();

    final ScheduledTask c = makeTask("c", RUNNING);
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider store) {
        store.getUnsafeTaskStore().saveTasks(ImmutableSet.of(
            makeTask("a", KILLED),
            makeTask("b", PENDING),
            c));
      }
    });
    taskGroups.storageStarted(new StorageStarted());
    changeState(c, RUNNING, FINISHED);
  }

  @Test
  public void testTaskMissing() {
    Capture<Runnable> timeoutCapture = expectTaskBackoff(10);

    replayAndCreateScheduler();

    taskGroups.taskChangedState(new TaskStateChange(makeTask("a", PENDING), INIT));
    timeoutCapture.getValue().run();
  }

  @Test
  public void testTaskAssigned() {
    expectAnyMaintenanceCalls();
    expectOfferDeclineIn(10);

    ScheduledTask task = makeTask("a", PENDING);
    TaskInfo mesosTask = makeTaskInfo(task);

    Capture<Runnable> timeoutCapture = expectTaskBackoff(10);
    expect(assigner.maybeAssign(OFFER_A, task)).andReturn(Optional.<TaskInfo>absent());

    Capture<Runnable> timeoutCapture2 = expectTaskBackoff(10, 20);
    expect(assigner.maybeAssign(OFFER_A, task)).andReturn(Optional.of(mesosTask));
    driver.launchTask(OFFER_A.getId(), mesosTask);

    Capture<Runnable> timeoutCapture3 = expectTaskBackoff(10);
    expectTaskBackoff(10, 20);

    replayAndCreateScheduler();

    offerQueue.addOffer(OFFER_A);
    changeState(task, INIT, PENDING);
    timeoutCapture.getValue().run();
    timeoutCapture2.getValue().run();

    // Ensure the offer was consumed.
    changeState(makeTask("b"), INIT, PENDING);
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

    Capture<Runnable> timeoutCapture = expectTaskBackoff(10);
    expectAnyMaintenanceCalls();
    expectOfferDeclineIn(10);
    expect(assigner.maybeAssign(OFFER_A, task)).andReturn(Optional.of(mesosTask));
    driver.launchTask(OFFER_A.getId(), mesosTask);
    expectLastCall().andThrow(new IllegalStateException("Driver not ready."));
    expect(stateManager.changeState(
        Query.taskScoped("a").byStatus(PENDING).get(),
        LOST,
        TaskScheduler.LAUNCH_FAILED_MSG))
        .andReturn(1);

    replayAndCreateScheduler();

    changeState(task, INIT, PENDING);
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

    Capture<Runnable> timeoutCapture = expectTaskBackoff(10);
    expectAnyMaintenanceCalls();
    expectOfferDeclineIn(10);
    expect(assigner.maybeAssign(OFFER_A, task)).andThrow(new StorageException("Injected failure."));

    Capture<Runnable> timeoutCapture2 = expectTaskBackoff(10, 20);
    expect(assigner.maybeAssign(OFFER_A, task)).andReturn(Optional.of(mesosTask));
    driver.launchTask(OFFER_A.getId(), mesosTask);
    expectLastCall();

    replayAndCreateScheduler();

    changeState(task, INIT, PENDING);
    offerQueue.addOffer(OFFER_A);
    timeoutCapture.getValue().run();
    timeoutCapture2.getValue().run();
  }

  @Test
  public void testExpiration() {
    ScheduledTask task = makeTask("a", PENDING);

    Capture<Runnable> timeoutCapture = expectTaskBackoff(10);
    Capture<Runnable> offerExpirationCapture = expectOfferDeclineIn(10);
    expectAnyMaintenanceCalls();
    expect(assigner.maybeAssign(OFFER_A, task)).andReturn(Optional.<TaskInfo>absent());
    Capture<Runnable> timeoutCapture2 = expectTaskBackoff(10, 20);
    driver.declineOffer(OFFER_A.getId());
    expectTaskBackoff(20, 30);

    replayAndCreateScheduler();

    changeState(task, INIT, PENDING);
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
    Capture<Runnable> captureA = expectTaskBackoff(10);

    ScheduledTask taskB = makeTask("B", PENDING);
    TaskInfo mesosTaskB = makeTaskInfo(taskB);
    expect(assigner.maybeAssign(OFFER_B, taskB)).andReturn(Optional.of(mesosTaskB));
    driver.launchTask(OFFER_B.getId(), mesosTaskB);
    Capture<Runnable> captureB = expectTaskBackoff(10);

    replayAndCreateScheduler();

    offerQueue.addOffer(OFFER_D);
    offerQueue.addOffer(OFFER_C);
    offerQueue.addOffer(OFFER_B);
    offerQueue.addOffer(OFFER_A);

    changeState(taskA, INIT, PENDING);
    captureA.getValue().run();

    changeState(taskB, INIT, PENDING);
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
    Capture<Runnable> captureA = expectTaskBackoff(10);

    ScheduledTask taskB = makeTask("B", PENDING);
    TaskInfo mesosTaskB = makeTaskInfo(taskB);
    expect(assigner.maybeAssign(OFFER_C, taskB)).andReturn(Optional.of(mesosTaskB));
    driver.launchTask(OFFER_C.getId(), mesosTaskB);
    Capture<Runnable> captureB = expectTaskBackoff(10);

    replayAndCreateScheduler();

    offerQueue.addOffer(OFFER_A);
    offerQueue.addOffer(OFFER_B);
    offerQueue.addOffer(OFFER_C);

    // Initially, we'd expect the offers to be consumed in order (A, B), with (C) unschedulable

    // Expected order now (B), with (C, A) unschedulable
    changeHostMaintenanceState("HOST_A", MaintenanceMode.DRAINING);
    changeState(taskA, INIT, PENDING);
    captureA.getValue().run();

    // Expected order now (C), with (A) unschedulable and (B) already consumed
    changeHostMaintenanceState("HOST_C", MaintenanceMode.NONE);
    changeState(taskB, INIT, PENDING);
    captureB.getValue().run();
  }

  private Capture<ScheduledTask> expectTaskScheduled(ScheduledTask task) {
    TaskInfo mesosTask = makeTaskInfo(task);
    Capture<ScheduledTask> taskScheduled = createCapture();
    expect(assigner.maybeAssign(EasyMock.<Offer>anyObject(), capture(taskScheduled)))
        .andReturn(Optional.of(mesosTask));
    driver.launchTask(EasyMock.<OfferID>anyObject(), EasyMock.eq(mesosTask));
    return taskScheduled;
  }

  @Test
  public void testResistsStarvation() {
    // TODO(wfarner): This test requires intimate knowledge of the way futures are used inside
    // TaskScheduler.  It's time to test using a real ScheduledExecutorService.

    expectAnyMaintenanceCalls();

    ScheduledTask jobA0 = makeTask("a0", PENDING);

    ScheduledTask jobA1 = jobA0.deepCopy();
    jobA1.getAssignedTask().setTaskId("a1");
    jobA1.getAssignedTask().getTask().setShardId(1);

    ScheduledTask jobA2 = jobA0.deepCopy();
    jobA2.getAssignedTask().setTaskId("a2");
    jobA2.getAssignedTask().getTask().setShardId(2);

    ScheduledTask jobB0 = makeTask("b0", PENDING);

    expectOfferDeclineIn(10);
    expectOfferDeclineIn(10);
    expectOfferDeclineIn(10);
    expectOfferDeclineIn(10);

    Capture<Runnable> timeoutA = expectTaskBackoff(10);
    Capture<Runnable> timeoutB = expectTaskBackoff(10);

    Capture<ScheduledTask> firstScheduled = expectTaskScheduled(jobA0);
    Capture<ScheduledTask> secondScheduled = expectTaskScheduled(jobB0);

    // Expect another watch of the task group for job A.
    expectTaskBackoff(10);

    replayAndCreateScheduler();

    offerQueue.addOffer(OFFER_A);
    offerQueue.addOffer(OFFER_B);
    offerQueue.addOffer(OFFER_C);
    offerQueue.addOffer(OFFER_D);
    changeState(jobA0, INIT, PENDING);
    changeState(jobA1, INIT, PENDING);
    changeState(jobA2, INIT, PENDING);
    changeState(jobB0, INIT, PENDING);
    timeoutA.getValue().run();
    timeoutB.getValue().run();
    assertEquals(
        ImmutableSet.of(jobA0, jobB0),
        ImmutableSet.of(firstScheduled.getValue(), secondScheduled.getValue()));
  }

  @Test
  public void testTaskDeleted() {
    expectAnyMaintenanceCalls();
    expectOfferDeclineIn(10);

    final ScheduledTask task = makeTask("a", PENDING);

    Capture<Runnable> timeoutCapture = expectTaskBackoff(10);
    expect(assigner.maybeAssign(OFFER_A, task)).andReturn(Optional.<TaskInfo>absent());
    expectTaskBackoff(10, 20);

    replayAndCreateScheduler();

    offerQueue.addOffer(OFFER_A);
    changeState(task, INIT, PENDING);
    timeoutCapture.getValue().run();

    // Ensure the offer was consumed.
    changeState(task, INIT, PENDING);
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().deleteTasks(Tasks.ids(task));
      }
    });
    taskGroups.tasksDeleted(new TasksDeleted(ImmutableSet.of(task)));
    timeoutCapture.getValue().run();
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

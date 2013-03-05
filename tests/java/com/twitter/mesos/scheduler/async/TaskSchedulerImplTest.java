package com.twitter.mesos.scheduler.async;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.mesos.Protos.FrameworkID;
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
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.Driver;
import com.twitter.mesos.scheduler.StateManager;
import com.twitter.mesos.scheduler.TaskAssigner;
import com.twitter.mesos.scheduler.async.TaskScheduler.OfferReturnDelay;
import com.twitter.mesos.scheduler.async.TaskScheduler.TaskSchedulerImpl;
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

import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.INIT;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED;
import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;

public class TaskSchedulerImplTest extends EasyMockTest {

  private Storage storage;
  private StateManager stateManager;
  private TaskAssigner assigner;
  private BackoffStrategy retryStrategy;
  private Driver driver;
  private ScheduledExecutorService executor;
  private ScheduledFuture<?> future;
  private OfferReturnDelay returnDelay;

  private TaskSchedulerImpl scheduler;

  @Before
  public void setUp() {
    storage = MemStorage.newEmptyStorage();
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
    scheduler = new TaskSchedulerImpl(
        storage,
        stateManager,
        assigner,
        retryStrategy,
        driver,
        executor,
        returnDelay);
  }

  private Offer makeOffer(String offerId) {
    return Offer.newBuilder()
        .setId(OfferID.newBuilder().setValue(offerId))
        .setFrameworkId(FrameworkID.newBuilder().setValue("framework_id"))
        .setSlaveId(SlaveID.newBuilder().setValue("slave_id-" + offerId))
        .setHostname("hostname")
        .build();
  }

  private Capture<Runnable> expectOfferDeclineIn(int delayMillis) {
    expect(returnDelay.get()).andReturn(Amount.of(delayMillis, Time.MILLISECONDS));
    Capture<Runnable> runnable = createCapture();
    executor.schedule(capture(runnable), eq((long) delayMillis), eq(TimeUnit.MILLISECONDS));
    expectLastCall().andReturn(createMock(ScheduledFuture.class));
    return runnable;
  }

  private void sendOffer(Offer offer) {
    scheduler.offer(ImmutableList.of(offer));
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
    expectOfferDeclineIn(10);
    expectOfferDeclineIn(10);

    replayAndCreateScheduler();

    sendOffer(makeOffer("a"));
    sendOffer(makeOffer("b"));
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
    storage.doInWriteTransaction(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider store) {
        store.getTaskStore().saveTasks(
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
    expectOfferDeclineIn(10);
    Offer offer = makeOffer("offerA");
    ScheduledTask task = makeTask("a", PENDING);
    TaskInfo mesosTask = TaskInfo.newBuilder()
        .setName(Tasks.id(task))
        .setTaskId(TaskID.newBuilder().setValue(Tasks.id(task)))
        .setSlaveId(SlaveID.newBuilder().setValue("slaveId"))
        .build();

    Capture<Runnable> timeoutCapture = expectTaskWatch(10);
    expect(assigner.maybeAssign(offer, task)).andReturn(Optional.<TaskInfo>absent());

    Capture<Runnable> timeoutCapture2 = expectTaskWatch(10, 20);
    expect(assigner.maybeAssign(offer, task)).andReturn(Optional.of(mesosTask));
    driver.launchTask(offer.getId(), mesosTask);

    Capture<Runnable> timeoutCapture3 = expectTaskWatch(10);
    expectTaskWatch(10, 20);

    replayAndCreateScheduler();

    sendOffer(offer);
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
    Offer offer = makeOffer("offerA");
    ScheduledTask task = makeTask("a", PENDING);
    TaskInfo mesosTask = TaskInfo.newBuilder()
        .setName(Tasks.id(task))
        .setTaskId(TaskID.newBuilder().setValue(Tasks.id(task)))
        .setSlaveId(SlaveID.newBuilder().setValue("slaveId"))
        .build();

    Capture<Runnable> timeoutCapture = expectTaskWatch(10);
    expectOfferDeclineIn(10);
    expect(assigner.maybeAssign(offer, task)).andReturn(Optional.of(mesosTask));
    driver.launchTask(offer.getId(), mesosTask);
    expectLastCall().andThrow(new IllegalStateException("Driver not ready."));
    expect(stateManager.changeState(
        new TaskQuery()
            .setTaskIds(ImmutableSet.of("a"))
            .setStatuses(ImmutableSet.of(PENDING)),
        LOST,
        TaskSchedulerImpl.LAUNCH_FAILED_MSG))
        .andReturn(1);

    replayAndCreateScheduler();

    insertTasks(task);
    changeState("a", INIT, PENDING);
    sendOffer(offer);
    timeoutCapture.getValue().run();
  }

  @Test
  public void testStorageException() {
    Offer offer = makeOffer("offerA");
    ScheduledTask task = makeTask("a", PENDING);
    TaskInfo mesosTask = TaskInfo.newBuilder()
        .setName(Tasks.id(task))
        .setTaskId(TaskID.newBuilder().setValue(Tasks.id(task)))
        .setSlaveId(SlaveID.newBuilder().setValue("slaveId"))
        .build();

    Capture<Runnable> timeoutCapture = expectTaskWatch(10);
    expectOfferDeclineIn(10);
    expect(assigner.maybeAssign(offer, task)).andThrow(new StorageException("Injected failure."));

    Capture<Runnable> timeoutCapture2 = expectTaskWatch(10, 20);
    expect(assigner.maybeAssign(offer, task)).andReturn(Optional.of(mesosTask));
    driver.launchTask(offer.getId(), mesosTask);

    replayAndCreateScheduler();

    insertTasks(task);
    changeState("a", INIT, PENDING);
    sendOffer(offer);
    timeoutCapture.getValue().run();
    timeoutCapture2.getValue().run();
  }

  @Test
  public void testExpiration() {
    Offer offerA = makeOffer("offerA");
    ScheduledTask task = makeTask("a", PENDING);
    Capture<Runnable> timeoutCapture = expectTaskWatch(10);
    Capture<Runnable> offerExpirationCapture = expectOfferDeclineIn(10);
    expect(assigner.maybeAssign(offerA, task)).andReturn(Optional.<TaskInfo>absent());
    Capture<Runnable> timeoutCapture2 = expectTaskWatch(10, 20);
    driver.declineOffer(offerA.getId());
    expectTaskWatch(20, 30);

    replayAndCreateScheduler();

    insertTasks(task);
    changeState("a", INIT, PENDING);
    sendOffer(offerA);
    timeoutCapture.getValue().run();
    offerExpirationCapture.getValue().run();
    timeoutCapture2.getValue().run();
  }

  @Test
  public void testOneOfferPerSlave() {
    Capture<Runnable> offerExpirationCapture = expectOfferDeclineIn(10);

    Offer offerA = makeOffer("offerA");
    Offer offerB = makeOffer("offerB").toBuilder().setSlaveId(offerA.getSlaveId()).build();
    driver.declineOffer(offerA.getId());
    driver.declineOffer(offerB.getId());

    // No attempt is made to avoid declining offers multiple times, so offerA is declined twice.
    driver.declineOffer(offerA.getId());

    replayAndCreateScheduler();

    sendOffer(offerA);
    sendOffer(offerB);
    offerExpirationCapture.getValue().run();
  }
}

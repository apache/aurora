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
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.SchedulerDriver;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.BackoffStrategy;
import com.twitter.common.util.testing.FakeTicker;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.scheduler.TaskAssigner;
import com.twitter.mesos.scheduler.async.TaskScheduler.TaskSchedulerImpl;
import com.twitter.mesos.scheduler.events.PubsubEvent.StorageStarted;
import com.twitter.mesos.scheduler.events.PubsubEvent.TaskStateChange;
import com.twitter.mesos.scheduler.events.PubsubEvent.TasksDeleted;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;
import com.twitter.mesos.scheduler.storage.mem.MemStorage;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertTrue;

import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.INIT;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static com.twitter.mesos.scheduler.async.TaskScheduler.TaskSchedulerImpl.INITIAL_PENALTY_MS;

public class TaskSchedulerImplTest extends EasyMockTest {

  private static final Amount<Long, Time> OFFER_EXPIRY = Amount.of(1L, Time.MINUTES);

  private Storage storage;
  private TaskAssigner assigner;
  private BackoffStrategy retryStrategy;
  private SchedulerDriver driver;
  private ScheduledExecutorService executor;
  private FakeTicker ticker;
  private ScheduledFuture<?> future;

  private TaskSchedulerImpl scheduler;

  @Before
  public void setUp() {
    storage = MemStorage.newEmptyStorage();
    assigner = createMock(TaskAssigner.class);
    retryStrategy = createMock(BackoffStrategy.class);
    driver = createMock(SchedulerDriver.class);
    executor = createMock(ScheduledExecutorService.class);
    ticker = new FakeTicker();
    future = createMock(ScheduledFuture.class);

    scheduler = new TaskSchedulerImpl(
        storage,
        assigner,
        retryStrategy,
        driver,
        executor,
        OFFER_EXPIRY,
        ticker);
  }

  @After
  public void validateNoLeak() {
    assertTrue(scheduler.futures.isEmpty());
  }

  private Offer makeOffer(String offerId) {
    return Offer.newBuilder()
        .setId(OfferID.newBuilder().setValue(offerId))
        .setFrameworkId(FrameworkID.newBuilder().setValue("framework_id"))
        .setSlaveId(SlaveID.newBuilder().setValue("slave_id"))
        .setHostname("hostname")
        .build();
  }

  private void sendOffer(Offer offer) {
    scheduler.offer(ImmutableList.of(offer));
  }

  private void changeState(String taskId, ScheduleStatus oldState, ScheduleStatus newState) {
    scheduler.taskChangedState(new TaskStateChange(taskId, oldState, newState));
  }

  private Capture<Runnable> expectTaskWatch(long penaltyMs) {
    Capture<Runnable> capture = createCapture();
    executor.schedule(
        EasyMock.capture(capture),
        eq(penaltyMs),
        eq(TimeUnit.MILLISECONDS));
    expectLastCall().andReturn(future);
    return capture;
  }

  private Capture<Runnable> expectTaskWatch() {
    return expectTaskWatch(INITIAL_PENALTY_MS);
  }

  private IExpectationSetters<?> expectCancel(boolean interrupt) {
    return expect(future.cancel(interrupt)).andReturn(true);
  }

  @Test
  public void testNoTasks() {
    control.replay();

    sendOffer(makeOffer("a"));
    sendOffer(makeOffer("b"));
  }

  @Test
  public void testNoOffers() {
    Capture<Runnable> timeoutCapture = expectTaskWatch();
    expect(retryStrategy.calculateBackoffMs(INITIAL_PENALTY_MS))
        .andReturn(INITIAL_PENALTY_MS * 2);
    expectTaskWatch(INITIAL_PENALTY_MS * 2);
    expectCancel(true);

    control.replay();

    insertTasks(makeTask("a", PENDING));
    changeState("a", INIT, PENDING);
    timeoutCapture.getValue().run();
    scheduler.tasksDeleted(new TasksDeleted(ImmutableSet.of("a", "b")));
  }

  private ScheduledTask makeTask(String taskId, ScheduleStatus status) {
    return new ScheduledTask()
        .setStatus(status)
        .setAssignedTask(new AssignedTask()
            .setTaskId(taskId));
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
    expectTaskWatch();
    expectCancel(true);

    control.replay();

    insertTasks(
        makeTask("a", KILLED),
        makeTask("b", PENDING),
        makeTask("c", RUNNING));
    scheduler.storageStarted(new StorageStarted());
    changeState("c", RUNNING, FINISHED);
    scheduler.tasksDeleted(new TasksDeleted(ImmutableSet.of("b")));
  }

  @Test
  public void testTaskMissing() {
    Capture<Runnable> timeoutCapture = expectTaskWatch();

    control.replay();

    changeState("a", INIT, PENDING);
    timeoutCapture.getValue().run();
  }

  @Test
  public void testTaskAssigned() {
    Offer offer = makeOffer("offerA");
    ScheduledTask task = makeTask("a", PENDING);
    TaskInfo mesosTask = TaskInfo.newBuilder()
        .setName(Tasks.id(task))
        .setTaskId(TaskID.newBuilder().setValue(Tasks.id(task)))
        .setSlaveId(SlaveID.newBuilder().setValue("slaveId"))
        .build();

    Capture<Runnable> timeoutCapture = expectTaskWatch();
    expect(assigner.maybeAssign(offer, task)).andReturn(Optional.<TaskInfo>absent());
    expect(retryStrategy.calculateBackoffMs(INITIAL_PENALTY_MS))
        .andReturn(INITIAL_PENALTY_MS * 2);

    Capture<Runnable> timeoutCapture2 = expectTaskWatch(INITIAL_PENALTY_MS * 2);
    expect(assigner.maybeAssign(offer, task)).andReturn(Optional.of(mesosTask));
    expect(driver.launchTasks(offer.getId(), ImmutableList.of(mesosTask)))
        .andReturn(Status.DRIVER_RUNNING);

    Capture<Runnable> timeoutCapture3 = expectTaskWatch();
    expect(retryStrategy.calculateBackoffMs(INITIAL_PENALTY_MS))
        .andReturn(INITIAL_PENALTY_MS * 2);
    expectTaskWatch(INITIAL_PENALTY_MS * 2);
    expectCancel(true);

    control.replay();

    sendOffer(offer);
    insertTasks(task);
    changeState("a", INIT, PENDING);
    timeoutCapture.getValue().run();
    timeoutCapture2.getValue().run();

    // Ensure the offer was consumed
    insertTasks(makeTask("b", PENDING));
    changeState("b", INIT, PENDING);
    timeoutCapture3.getValue().run();
    scheduler.tasksDeleted(new TasksDeleted(ImmutableSet.of("b")));
  }
}

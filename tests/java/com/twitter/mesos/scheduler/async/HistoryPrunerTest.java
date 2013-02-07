package com.twitter.mesos.scheduler.async;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IExpectationSetters;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.concurrent.ExecutorServiceShutdown;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskEvent;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.StateManager;
import com.twitter.mesos.scheduler.events.PubsubEvent;
import com.twitter.mesos.scheduler.events.PubsubEvent.StorageStarted;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED;
import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static com.twitter.mesos.gen.ScheduleStatus.STARTING;
import static com.twitter.mesos.scheduler.events.PubsubEvent.TasksDeleted;

public class HistoryPrunerTest extends EasyMockTest {
  private static final String JOB_A = "job-a";
  private static final String TASK_ID = "task_id";
  private static final String SLAVE_HOST = "HOST_A";
  private static final Amount<Long, Time> ONE_MS = Amount.of(1L, Time.MILLISECONDS);
  private static final Amount<Long, Time> ONE_DAY = Amount.of(1L, Time.DAYS);
  private static final Amount<Long, Time> ONE_HOUR = Amount.of(1L, Time.HOURS);
  private static final int PER_JOB_HISTORY = 2;

  private ScheduledFuture<?> future;
  private ScheduledExecutorService executor;
  private FakeClock clock;
  private StateManager stateManager;
  private HistoryPruner pruner;

  @Before
  public void setUp() {
    future = createMock(new Clazz<ScheduledFuture<?>>() { });
    executor = createMock(ScheduledExecutorService.class);
    clock = new FakeClock();
    stateManager = createMock(StateManager.class);
    pruner = new HistoryPruner(executor, stateManager, clock, ONE_DAY, PER_JOB_HISTORY);
  }

  @After
  public void validateNoLeak() {
    assertTrue(pruner.tasksByJob.isEmpty());
  }

  @Test
  public void testNoTasksOnStorageStart() {
    expectGetInactiveTasks();

    control.replay();

    pruner.storageStarted(new StorageStarted());
  }

  private IExpectationSetters<?> expectAsyncTaskDelete() {
    expect(executor.submit(EasyMock.<Runnable>anyObject()));
    return expectLastCall().andAnswer(new IAnswer<Future<?>>() {
      @Override public Future<?> answer() {
        ((Runnable) EasyMock.getCurrentArguments()[0]).run();
        return null;
      }
    });
  }

  @Test
  public void testStorageStartWithoutPruning() {
    long taskATimestamp = clock.nowMillis();
    ScheduledTask a = makeTask("a", FINISHED);

    clock.advance(ONE_MS);
    long taskBTimestamp = clock.nowMillis();
    ScheduledTask b = makeTask("b", LOST);

    expectGetInactiveTasks(a, b);
    expectOneTaskWatch(taskATimestamp);
    expectOneTaskWatch(taskBTimestamp);

    expectAsyncTaskDelete();
    expectCancelFuture().times(2);

    control.replay();

    pruner.storageStarted(new StorageStarted());

    // Clean-up
    pruner.tasksDeleted(new TasksDeleted(ImmutableSet.of(a, b)));
  }

  @Test
  public void testStorageStartedWithPruning() {
    long taskATimestamp = clock.nowMillis();
    ScheduledTask a = makeTask("a", FINISHED);

    clock.advance(ONE_MS);
    long taskBTimestamp = clock.nowMillis();
    ScheduledTask b = makeTask("b", LOST);

    clock.advance(ONE_MS);
    long taskCTimestamp = clock.nowMillis();
    ScheduledTask c = makeTask("c", FINISHED);

    clock.advance(ONE_MS);
    ScheduledTask d = makeTask("d", FINISHED);
    ScheduledTask e = makeTask("job-x", "e", FINISHED);

    expectGetInactiveTasks(a, b, c, d, e);
    expectOneTaskWatch(taskATimestamp);
    expectOneTaskWatch(taskBTimestamp);
    expectOneTaskWatch(taskCTimestamp);
    expectDefaultTaskWatch();
    expectDefaultTaskWatch();

    // Cancel future and delete pruned task "a"
    expectCancelFuture();
    stateManager.deleteTasks(Tasks.ids(a));

    // Cancel future and delete pruned task "b"
    expectAsyncTaskDelete();
    expectCancelFuture();
    stateManager.deleteTasks(Tasks.ids(b));

    expectCancelFuture().times(3);

    control.replay();

    pruner.storageStarted(new StorageStarted());

    // Clean-up
    pruner.tasksDeleted(new TasksDeleted(ImmutableSet.of(c, d, e)));
  }

  @Test
  public void testStateChange() {
    expectDefaultTaskWatch();

    expectAsyncTaskDelete();
    expectCancelFuture();

    control.replay();

    // No future set for non-terminal state transition.
    changeState(STARTING, RUNNING);

    // Future set for terminal state transition.
    ScheduledTask a = changeState(RUNNING, KILLED);

    // Clean-up
    pruner.tasksDeleted(new TasksDeleted(ImmutableSet.of(a)));
  }

  @Test
  public void testActivateFutureAndExceedHistoryGoal() {
    Capture<Runnable> capture = expectDefaultTaskWatch();

    // Expect task "a" to be pruned when future is activated.
    stateManager.deleteTasks(ImmutableSet.of("a"));

    control.replay();

    // Capture future for inactive task "a"
    changeState("a", RUNNING, KILLED);
    clock.advance(ONE_HOUR);
    // Execute future to prune task "a" from the system.
    capture.getValue().run();
  }

  @Test
  public void testJobHistoryExceeded() {
    // Future for tasks - a,b,c
    expectDefaultTaskWatchTimes(3);

    // Cancel future and delete task "a" when history goal is exceeded.
    expectCancelFuture();
    stateManager.deleteTasks(ImmutableSet.of("a"));

    expectAsyncTaskDelete();
    expectCancelFuture().times(2);

    control.replay();

    changeState("a", RUNNING, KILLED);
    clock.advance(ONE_HOUR);
    ScheduledTask b = changeState("b", RUNNING, KILLED);
    clock.advance(ONE_HOUR);
    ScheduledTask c = changeState("c", RUNNING, LOST);

    // Clean-up
    pruner.tasksDeleted(new TasksDeleted(ImmutableSet.of(b, c)));
  }

  @Test
  public void testTasksDeleted() {
    ScheduledTask a = makeTask("a", FINISHED);
    ScheduledTask b = makeTask("b", FINISHED);
    expectGetInactiveTasks(a);
    expectDefaultTaskWatch();
    expectAsyncTaskDelete().times(2);
    expectCancelFuture();

    control.replay();

    pruner.storageStarted(new StorageStarted());

    // Cancels existing future for task 'a'
    pruner.tasksDeleted(new TasksDeleted(ImmutableSet.of(a)));

    // No-Op
    pruner.tasksDeleted(new TasksDeleted(ImmutableSet.of(b)));
  }

  @Test
  public void testThreadSafeEvents() throws Exception {
    // This tests against regression where deleting a task causes an event to be fired
    // synchronously (on the EventBus) from a separate thread.  This posed a problem because
    // the event handler was synchronized, causing the EventBus thread to deadlock acquiring
    // the lock held by the thread deleting tasks.

    final ScheduledExecutorService realExecutor = Executors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("testThreadSafeEvents-executor")
            .build());
    pruner = new HistoryPruner(
        realExecutor,
        stateManager,
        clock,
        Amount.of(1L, Time.MILLISECONDS),
        PER_JOB_HISTORY);

    final CountDownLatch deleteCalled = new CountDownLatch(1);
    final CountDownLatch eventDelivered = new CountDownLatch(1);
    final String id = "a";

    Thread eventDispatch = new Thread() {
      @Override public void run() {
        try {
          deleteCalled.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          fail("Interrupted while awaiting for delete call.");
          return;
        }
        pruner.tasksDeleted(new TasksDeleted(ImmutableSet.of(makeTask(id, ScheduleStatus.KILLED))));
        eventDelivered.countDown();
      }
    };
    eventDispatch.setDaemon(true);
    eventDispatch.setName(getClass().getName() + "-EventDispatch");
    eventDispatch.start();

    // Expect task "a" to be pruned when future is activated.
    stateManager.deleteTasks(ImmutableSet.of(id));
    expectLastCall().andAnswer(new IAnswer<Void>() {
      @Override public Void answer() {
        deleteCalled.countDown();
        try {
          eventDelivered.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          fail("Interrupted while awaiting for event delivery.");
        }
        return null;
      }
    });

    control.replay();

    // Capture future for inactive task "a"
    changeState(id, RUNNING, KILLED);
    eventDelivered.await();
    new ExecutorServiceShutdown(realExecutor, Amount.of(1L, Time.MINUTES)).execute();
  }

  private Capture<Runnable> expectDefaultTaskWatch() {
    return expectTaskWatch(ONE_DAY.as(Time.MILLISECONDS), 1);
  }

  private Capture<Runnable> expectDefaultTaskWatchTimes(int count) {
    return expectTaskWatch(ONE_DAY.as(Time.MILLISECONDS), count);
  }

  private Capture<Runnable> expectOneTaskWatch(long timestampMillis) {
    return expectTaskWatch(timestampMillis, 1);
  }

  private Capture<Runnable> expectTaskWatch(long timestampMillis, int count) {
    Capture<Runnable> capture = createCapture();
    executor.schedule(
        EasyMock.capture(capture),
        eq(pruner.calculateTimeout(timestampMillis)),
        eq(TimeUnit.MILLISECONDS));
    expectLastCall().andReturn(future).times(count);
    return capture;
  }

  private IExpectationSetters<?> expectCancelFuture() {
    return expect(future.cancel(false)).andReturn(true);
  }

  private ScheduledTask changeState(ScheduleStatus from, ScheduleStatus to) {
    return changeState(TASK_ID, from, to);
  }

  private ScheduledTask changeState(String taskId, ScheduleStatus from, ScheduleStatus to) {
    ScheduledTask task = makeTask(taskId, to);
    pruner.recordStateChange(new PubsubEvent.TaskStateChange(taskId, from, task));
    return task;
  }

  private void expectGetInactiveTasks(ScheduledTask... tasks) {
    expect(stateManager.fetchTasks(HistoryPruner.INACTIVE_QUERY))
        .andReturn(ImmutableSet.<ScheduledTask>builder().add(tasks).build());
  }

  private ScheduledTask makeTask(
      String job,
      String taskId,
      ScheduleStatus status) {

    return new ScheduledTask()
        .setStatus(status)
        .setTaskEvents(ImmutableList.of(new TaskEvent(clock.nowMillis(), status)))
        .setAssignedTask(makeAssignedTask(job, taskId));
  }

  private ScheduledTask makeTask(
      String taskId,
      ScheduleStatus status) {

    return makeTask(JOB_A, taskId, status);
  }

  private AssignedTask makeAssignedTask(String job, String taskId) {
    return new AssignedTask()
        .setSlaveHost(SLAVE_HOST)
        .setTaskId(taskId)
        .setTask(new TwitterTaskInfo()
            .setOwner(new Identity().setRole("role").setUser("user"))
            .setJobName(job)
            .setThermosConfig(new byte[]{1, 2, 3}));
  }
}

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

import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskEvent;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.events.PubsubEvent;
import com.twitter.mesos.scheduler.events.PubsubEvent.StorageStarted;
import com.twitter.mesos.scheduler.storage.testing.StorageTestUtil;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static com.twitter.mesos.gen.ScheduleStatus.ASSIGNED;
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
  private StorageTestUtil storageUtil;
  private HistoryPruner pruner;

  @Before
  public void setUp() {
    future = createMock(new Clazz<ScheduledFuture<?>>() { });
    executor = createMock(ScheduledExecutorService.class);
    clock = new FakeClock();
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectTransactions();
    pruner = new HistoryPruner(executor, storageUtil.storage, clock, ONE_DAY, PER_JOB_HISTORY);
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

    // Cancel future and delete pruned task "a" asynchronously.
    expectAsyncTaskDelete();
    expectCancelFuture();
    storageUtil.taskStore.deleteTasks(Tasks.ids(a));

    // Cancel future and delete pruned task "b" asynchronously.
    expectAsyncTaskDelete();
    expectCancelFuture();
    storageUtil.taskStore.deleteTasks(Tasks.ids(b));

    expectAsyncTaskDelete();
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
    Capture<Runnable> delayedDelete = expectDelayedTaskDeletion();

    // Expect task "a" to be pruned when future is activated.
    storageUtil.taskStore.deleteTasks(ImmutableSet.of("a"));

    control.replay();

    // Capture future for inactive task "a"
    changeState("a", RUNNING, KILLED);
    clock.advance(ONE_HOUR);
    // Execute future to prune task "a" from the system.
    capture.getValue().run();
    delayedDelete.getValue().run();
  }

  @Test
  public void testJobHistoryExceeded() {
    // Future for tasks - a,b,c
    expectDefaultTaskWatchTimes(3);

    // Cancel future and delete task "a" asynchronously when history goal is exceeded.
    expectAsyncTaskDelete();
    expectCancelFuture();
    storageUtil.taskStore.deleteTasks(ImmutableSet.of("a"));

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
  public void testThreadSafeStateChangeEvent() throws Exception {
    // This tests against regression where an executor pruning a task holds an intrinsic lock and
    // an unrelated task state change in the scheduler fires an event that requires this intrinsic
    // lock. This causes a deadlock when the executor tries to acquire a lock held by the event
    // fired.

    pruner = prunerWithRealExecutor();
    Command onDeleted = new Command() {
      @Override public void execute() {
        // The goal is to verify that the call does not deadlock. We do not care about the outcome.
        changeState("b", ASSIGNED, STARTING);
      }
    };
    CountDownLatch taskDeleted = expectTaskDeleted(onDeleted, TASK_ID);

    control.replay();

    // Change the task to a terminal state and wait for it to be pruned.
    changeState(TASK_ID, RUNNING, KILLED);
    taskDeleted.await();
  }

  @Test
  public void testThreadSafeDeleteEvents() throws Exception {
    // This tests against regression where deleting a task causes an event to be fired
    // synchronously (on the EventBus) from a separate thread.  This posed a problem because
    // the event handler was synchronized, causing the EventBus thread to deadlock acquiring
    // the lock held by the thread deleting tasks.

    pruner = prunerWithRealExecutor();
    Command onDeleted = new Command() {
      @Override public void execute() {
        // The goal is to verify that the call does not deadlock. We do not care about the outcome.
        pruner.tasksDeleted(
            new TasksDeleted(ImmutableSet.of(makeTask("a", ScheduleStatus.KILLED))));
      }
    };
    CountDownLatch taskDeleted = expectTaskDeleted(onDeleted, TASK_ID);

    control.replay();

    // Change the task to a terminal state and wait for it to be pruned.
    changeState(TASK_ID, RUNNING, KILLED);
    taskDeleted.await();
  }

  private HistoryPruner prunerWithRealExecutor() {
    ScheduledExecutorService realExecutor = Executors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("testThreadSafeEvents-executor")
            .build());
    return new HistoryPruner(
        realExecutor,
        storageUtil.storage,
        clock,
        Amount.of(1L, Time.MILLISECONDS),
        PER_JOB_HISTORY);
  }

  private CountDownLatch expectTaskDeleted(final Command onDelete, String taskId) {
    final CountDownLatch deleteCalled = new CountDownLatch(1);
    final CountDownLatch eventDelivered = new CountDownLatch(1);

    Thread eventDispatch = new Thread() {
      @Override public void run() {
        try {
          deleteCalled.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          fail("Interrupted while awaiting for delete call.");
          return;
        }
        onDelete.execute();
        eventDelivered.countDown();
      }
    };
    eventDispatch.setDaemon(true);
    eventDispatch.setName(getClass().getName() + "-EventDispatch");
    eventDispatch.start();

    storageUtil.taskStore.deleteTasks(ImmutableSet.of(taskId));
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

    return eventDelivered;
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

  private Capture<Runnable> expectDelayedTaskDeletion() {
    Capture<Runnable> capture = createCapture();
    executor.submit(EasyMock.capture(capture));
    expectLastCall().andReturn(future);
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
    pruner.recordStateChange(new PubsubEvent.TaskStateChange(task, from));
    return task;
  }

  private void expectGetInactiveTasks(ScheduledTask... tasks) {
    expect(storageUtil.taskStore.fetchTasks(HistoryPruner.INACTIVE_QUERY))
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

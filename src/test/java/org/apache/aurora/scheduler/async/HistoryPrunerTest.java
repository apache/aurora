/**
 * Copyright 2013 Apache Software Foundation
 *
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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.events.PubsubEvent.StorageStarted;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IExpectationSetters;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.ASSIGNED;
import static org.apache.aurora.gen.ScheduleStatus.FINISHED;
import static org.apache.aurora.gen.ScheduleStatus.KILLED;
import static org.apache.aurora.gen.ScheduleStatus.LOST;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.gen.ScheduleStatus.STARTING;
import static org.apache.aurora.scheduler.events.PubsubEvent.TasksDeleted;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
  private StateManager stateManager;
  private HistoryPruner pruner;

  @Before
  public void setUp() {
    future = createMock(new Clazz<ScheduledFuture<?>>() { });
    executor = createMock(ScheduledExecutorService.class);
    clock = new FakeClock();
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    stateManager = createMock(StateManager.class);
    pruner = new HistoryPruner(
        executor,
        storageUtil.storage,
        stateManager,
        clock,
        ONE_DAY,
        PER_JOB_HISTORY);
  }

  @After
  public void validateNoLeak() {
    synchronized (pruner.getTasksByJob()) {
      assertEquals(
          ImmutableMultimap.<IJobKey, String>of(),
          ImmutableMultimap.copyOf(pruner.getTasksByJob()));
    }
  }

  @Test
  public void testNoTasksOnStorageStart() {
    expectGetInactiveTasks();

    control.replay();

    pruner.storageStarted(new StorageStarted());
  }

  @Test
  public void testStorageStartWithoutPruning() {
    long taskATimestamp = clock.nowMillis();
    IScheduledTask a = makeTask("a", FINISHED);

    clock.advance(ONE_MS);
    long taskBTimestamp = clock.nowMillis();
    IScheduledTask b = makeTask("b", LOST);

    expectGetInactiveTasks(a, b);
    expectOneTaskWatch(taskATimestamp);
    expectOneTaskWatch(taskBTimestamp);

    expectCancelFuture().times(2);

    control.replay();

    pruner.storageStarted(new StorageStarted());

    // Clean-up
    pruner.tasksDeleted(new TasksDeleted(ImmutableSet.of(a, b)));
  }

  @Test
  public void testStorageStartedWithPruning() {
    long taskATimestamp = clock.nowMillis();
    IScheduledTask a = makeTask("a", FINISHED);

    clock.advance(ONE_MS);
    long taskBTimestamp = clock.nowMillis();
    IScheduledTask b = makeTask("b", LOST);

    clock.advance(ONE_MS);
    long taskCTimestamp = clock.nowMillis();
    IScheduledTask c = makeTask("c", FINISHED);

    clock.advance(ONE_MS);
    IScheduledTask d = makeTask("d", FINISHED);
    IScheduledTask e = makeTask("job-x", "e", FINISHED);

    expectGetInactiveTasks(a, b, c, d, e);
    expectOneTaskWatch(taskATimestamp);
    expectOneTaskWatch(taskBTimestamp);
    expectOneTaskWatch(taskCTimestamp);
    expectDefaultTaskWatch();
    expectDefaultTaskWatch();

    // Cancel future and delete pruned task "a" asynchronously.
    expectCancelFuture();
    stateManager.deleteTasks(Tasks.ids(a));

    // Cancel future and delete pruned task "b" asynchronously.
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

    expectCancelFuture();

    control.replay();

    // No future set for non-terminal state transition.
    changeState(STARTING, RUNNING);

    // Future set for terminal state transition.
    IScheduledTask a = changeState(RUNNING, KILLED);

    // Clean-up
    pruner.tasksDeleted(new TasksDeleted(ImmutableSet.of(a)));
  }

  @Test
  public void testActivateFutureAndExceedHistoryGoal() {
    Capture<Runnable> delayedDelete = expectDefaultTaskWatch();

    // Expect task "a" to be pruned when future is activated.
    stateManager.deleteTasks(ImmutableSet.of("a"));

    control.replay();

    // Capture future for inactive task "a"
    changeState("a", RUNNING, KILLED);
    clock.advance(ONE_HOUR);
    // Execute future to prune task "a" from the system.
    delayedDelete.getValue().run();
  }

  @Test
  public void testJobHistoryExceeded() {
    // Future for tasks - a,b,c
    expectDefaultTaskWatchTimes(3);

    // Cancel future and delete task "a" asynchronously when history goal is exceeded.
    expectCancelFuture();
    stateManager.deleteTasks(ImmutableSet.of("a"));

    expectCancelFuture().times(2);

    control.replay();

    changeState("a", RUNNING, KILLED);
    clock.advance(ONE_HOUR);
    IScheduledTask b = changeState("b", RUNNING, KILLED);
    clock.advance(ONE_HOUR);
    IScheduledTask c = changeState("c", RUNNING, LOST);

    // Clean-up
    pruner.tasksDeleted(new TasksDeleted(ImmutableSet.of(b, c)));
  }

  @Test
  public void testTasksDeleted() {
    IScheduledTask a = makeTask("a", FINISHED);
    IScheduledTask b = makeTask("b", FINISHED);
    expectGetInactiveTasks(a);
    expectDefaultTaskWatch();
    expectCancelFuture();

    control.replay();

    pruner.storageStarted(new StorageStarted());

    // Cancels existing future for task 'a'
    pruner.tasksDeleted(new TasksDeleted(ImmutableSet.of(a)));

    // No-Op
    pruner.tasksDeleted(new TasksDeleted(ImmutableSet.of(b)));
  }

  // TODO(William Farner): Consider removing the thread safety tests.  Now that intrinsic locks
  // are not used, it is rather awkward to test this.
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
        stateManager,
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

    stateManager.deleteTasks(ImmutableSet.of(taskId));
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

  private IExpectationSetters<?> expectCancelFuture() {
    return expect(future.cancel(false)).andReturn(true);
  }

  private IScheduledTask changeState(ScheduleStatus from, ScheduleStatus to) {
    return changeState(TASK_ID, from, to);
  }

  private IScheduledTask changeState(String taskId, ScheduleStatus from, ScheduleStatus to) {
    IScheduledTask task = makeTask(taskId, to);
    pruner.recordStateChange(new PubsubEvent.TaskStateChange(task, from));
    return task;
  }

  private void expectGetInactiveTasks(IScheduledTask... tasks) {
    expect(storageUtil.taskStore.fetchTasks(HistoryPruner.INACTIVE_QUERY))
        .andReturn(ImmutableSet.copyOf(tasks));
  }

  private IScheduledTask makeTask(
      String job,
      String taskId,
      ScheduleStatus status) {

    return IScheduledTask.build(new ScheduledTask()
        .setStatus(status)
        .setTaskEvents(ImmutableList.of(new TaskEvent(clock.nowMillis(), status)))
        .setAssignedTask(makeAssignedTask(job, taskId)));
  }

  private IScheduledTask makeTask(String taskId, ScheduleStatus status) {
    return makeTask(JOB_A, taskId, status);
  }

  private AssignedTask makeAssignedTask(String job, String taskId) {
    return new AssignedTask()
        .setSlaveHost(SLAVE_HOST)
        .setTaskId(taskId)
        .setTask(new TaskConfig()
            .setOwner(new Identity().setRole("role").setUser("user"))
            .setEnvironment("staging45")
            .setJobName(job)
            .setExecutorConfig(new ExecutorConfig("aurora", "config")));
  }
}

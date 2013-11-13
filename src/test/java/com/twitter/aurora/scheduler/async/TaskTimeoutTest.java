/*
 * Copyright 2013 Twitter, Inc.
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
package com.twitter.aurora.scheduler.async;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.gen.AssignedTask;
import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.gen.TaskEvent;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.events.PubsubEvent.StorageStarted;
import com.twitter.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import com.twitter.aurora.scheduler.state.StateManager;
import com.twitter.aurora.scheduler.storage.entities.IScheduledTask;
import com.twitter.aurora.scheduler.storage.testing.StorageTestUtil;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;

import static com.twitter.aurora.gen.ScheduleStatus.ASSIGNED;
import static com.twitter.aurora.gen.ScheduleStatus.FINISHED;
import static com.twitter.aurora.gen.ScheduleStatus.INIT;
import static com.twitter.aurora.gen.ScheduleStatus.KILLED;
import static com.twitter.aurora.gen.ScheduleStatus.KILLING;
import static com.twitter.aurora.gen.ScheduleStatus.LOST;
import static com.twitter.aurora.gen.ScheduleStatus.PENDING;
import static com.twitter.aurora.gen.ScheduleStatus.PREEMPTING;
import static com.twitter.aurora.gen.ScheduleStatus.RESTARTING;
import static com.twitter.aurora.gen.ScheduleStatus.ROLLBACK;
import static com.twitter.aurora.gen.ScheduleStatus.RUNNING;
import static com.twitter.aurora.gen.ScheduleStatus.STARTING;
import static com.twitter.aurora.gen.ScheduleStatus.UPDATING;

public class TaskTimeoutTest extends EasyMockTest {

  private static final String TASK_ID = "task_id";
  private static final long TIMEOUT_MS = Amount.of(1L, Time.MINUTES).as(Time.MILLISECONDS);

  private StorageTestUtil storageUtil;
  private ScheduledExecutorService executor;
  private ScheduledFuture<?> future;
  private StateManager stateManager;
  private FakeClock clock;
  private TaskTimeout timeout;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    executor = createMock(ScheduledExecutorService.class);
    future = createMock(new Clazz<ScheduledFuture<?>>() { });
    stateManager = createMock(StateManager.class);
    clock = new FakeClock();
    timeout = new TaskTimeout(
        storageUtil.storage,
        executor,
        stateManager,
        clock,
        Amount.of(TIMEOUT_MS, Time.MILLISECONDS));
  }

  @After
  public void verifyTasksDepleted() {
    try {
      // Verify there is no memory leak.
      assertEquals(0, Stats.getVariable(TaskTimeout.TRANSIENT_COUNT_STAT_NAME).read());
    } finally {
      Stats.flush();
    }
  }

  private Capture<Runnable> expectTaskWatch(long expireMs) {
    Capture<Runnable> capture = createCapture();
    executor.schedule(
        EasyMock.capture(capture),
        eq(expireMs),
        eq(TimeUnit.MILLISECONDS));
    expectLastCall().andReturn(future);
    return capture;
  }

  private Capture<Runnable> expectTaskWatch() {
    return expectTaskWatch(TIMEOUT_MS);
  }

  private IExpectationSetters<?> expectCancel() {
    return expect(future.cancel(false)).andReturn(true);
  }

  private void changeState(String taskId, ScheduleStatus from, ScheduleStatus to) {
    IScheduledTask task = IScheduledTask.build(new ScheduledTask()
        .setStatus(to)
        .setAssignedTask(new AssignedTask().setTaskId(taskId)));
    timeout.recordStateChange(new TaskStateChange(task, from));
  }

  private void changeState(ScheduleStatus from, ScheduleStatus to) {
    changeState(TASK_ID, from, to);
  }

  @Test
  public void testNormalTransitions() {
    expectTaskWatch();
    expectCancel();
    expectTaskWatch();
    expectCancel();

    control.replay();

    changeState(INIT, PENDING);
    changeState(PENDING, ASSIGNED);
    changeState(ASSIGNED, STARTING);
    changeState(STARTING, RUNNING);
    changeState(RUNNING, KILLING);
    changeState(KILLING, KILLED);
  }

  @Test
  public void testTransientToTransient() {
    expectTaskWatch();
    expectCancel();
    Capture<Runnable> killingTimeout = expectTaskWatch();
    Query.Builder query = Query.taskScoped(TASK_ID).byStatus(KILLING);
    expect(stateManager.changeState(query, LOST, TaskTimeout.TIMEOUT_MESSAGE)).andReturn(1);

    control.replay();

    changeState(PENDING, ASSIGNED);
    changeState(ASSIGNED, KILLING);
    killingTimeout.getValue().run();
  }

  @Test
  public void testTimeout() throws Exception {
    Capture<Runnable> assignedTimeout = expectTaskWatch();
    Query.Builder query = Query.taskScoped(TASK_ID).byStatus(ASSIGNED);
    expect(stateManager.changeState(query, LOST, TaskTimeout.TIMEOUT_MESSAGE)).andReturn(1);

    control.replay();

    changeState(INIT, PENDING);
    changeState(PENDING, ASSIGNED);
    assignedTimeout.getValue().run();
    checkStat(TaskTimeout.TIMED_OUT_TASKS_COUNTER, 1);
  }

  @Test
  public void testTaskDeleted() throws Exception {
    Capture<Runnable> assignedTimeout = expectTaskWatch();
    Query.Builder query = Query.taskScoped(TASK_ID).byStatus(UPDATING);
    expect(stateManager.changeState(query, LOST, TaskTimeout.TIMEOUT_MESSAGE)).andReturn(0);

    control.replay();

    changeState(INIT, PENDING);
    changeState(PENDING, UPDATING);
    assignedTimeout.getValue().run();
    checkStat(TaskTimeout.TIMED_OUT_TASKS_COUNTER, 0);
  }

  private static IScheduledTask makeTask(
      String taskId,
      ScheduleStatus status,
      long stateEnteredMs) {

    return IScheduledTask.build(new ScheduledTask()
        .setStatus(status)
        .setTaskEvents(ImmutableList.of(new TaskEvent(stateEnteredMs, status)))
        .setAssignedTask(new AssignedTask()
            .setTaskId(taskId)
            .setTask(new TaskConfig())));
  }

  @Test
  public void testStorageStart() {
    clock.setNowMillis(TIMEOUT_MS * 2);
    storageUtil.expectTaskFetch(
        TaskTimeout.TRANSIENT_QUERY,
        makeTask("a", ASSIGNED, 0),
        makeTask("b", KILLING, TIMEOUT_MS),
        makeTask("c", PREEMPTING, TIMEOUT_MS * 3) /* In the future */
    );
    expectTaskWatch(TIMEOUT_MS);
    expectTaskWatch(TIMEOUT_MS);
    expectTaskWatch(TIMEOUT_MS);
    expectCancel().times(3);

    control.replay();

    timeout.storageStarted(new StorageStarted());
    changeState("a", ASSIGNED, RUNNING);
    changeState("b", KILLING, KILLED);
    changeState("c", PREEMPTING, FINISHED);
  }

  @Test
  public void testStorageStartTwice() {
    // This should never happen, but testing that the class handles it gracefully.
    storageUtil.expectTaskFetch(TaskTimeout.TRANSIENT_QUERY, makeTask("a", ASSIGNED, 0)).times(2);
    expectTaskWatch();
    expectCancel();

    control.replay();

    timeout.storageStarted(new StorageStarted());
    timeout.storageStarted(new StorageStarted());
    changeState("a", ASSIGNED, RUNNING);
  }

  private void checkOutstandingTimer(ScheduleStatus status, long expectedValue) {
    checkStat(TaskTimeout.waitingTimeStatName(status), expectedValue);
  }

  private void checkStat(String name, long expectedValue) {
    long value = (Long) Stats.getVariable(name).read();
    assertEquals(expectedValue, value);
  }

  @Test
  public void testOutstandingTimers() throws Exception {
    expectTaskWatch();
    expectTaskWatch();
    expectCancel();
    expectTaskWatch();
    expectCancel().times(2);

    control.replay();

    checkOutstandingTimer(ASSIGNED, 0);
    checkOutstandingTimer(PREEMPTING, 0);
    checkOutstandingTimer(RESTARTING, 0);
    checkOutstandingTimer(KILLING, 0);
    checkOutstandingTimer(UPDATING, 0);
    checkOutstandingTimer(ROLLBACK, 0);

    changeState("a", PENDING, ASSIGNED);

    Amount<Long, Time> tick = Amount.of(10L, Time.SECONDS);
    clock.advance(tick);

    checkOutstandingTimer(ASSIGNED, tick.as(Time.MILLISECONDS));

    clock.advance(tick);
    changeState("b", PENDING, ASSIGNED);

    clock.advance(tick);
    checkOutstandingTimer(ASSIGNED, tick.as(Time.MILLISECONDS) * 3);

    changeState("a", ASSIGNED, RUNNING);
    clock.advance(tick);
    changeState("a", RUNNING, KILLING);
    clock.advance(tick);

    checkOutstandingTimer(ASSIGNED, tick.as(Time.MILLISECONDS) * 3);
    checkOutstandingTimer(KILLING, tick.as(Time.MILLISECONDS));

    changeState("a", KILLING, KILLED);
    changeState("b", ASSIGNED, FINISHED);

    checkOutstandingTimer(ASSIGNED, 0);
    checkOutstandingTimer(PREEMPTING, 0);
    checkOutstandingTimer(RESTARTING, 0);
    checkOutstandingTimer(KILLING, 0);
    checkOutstandingTimer(UPDATING, 0);
    checkOutstandingTimer(ROLLBACK, 0);
  }
}

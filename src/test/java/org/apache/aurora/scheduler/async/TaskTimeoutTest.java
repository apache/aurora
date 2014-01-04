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

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.StatsProvider;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.ASSIGNED;
import static org.apache.aurora.gen.ScheduleStatus.FINISHED;
import static org.apache.aurora.gen.ScheduleStatus.INIT;
import static org.apache.aurora.gen.ScheduleStatus.KILLED;
import static org.apache.aurora.gen.ScheduleStatus.KILLING;
import static org.apache.aurora.gen.ScheduleStatus.LOST;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.PREEMPTING;
import static org.apache.aurora.gen.ScheduleStatus.RESTARTING;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.gen.ScheduleStatus.STARTING;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;

public class TaskTimeoutTest extends EasyMockTest {

  private static final String TASK_ID = "task_id";
  private static final long TIMEOUT_MS = Amount.of(1L, Time.MINUTES).as(Time.MILLISECONDS);

  private AtomicLong timedOutTaskCounter;
  private Capture<Supplier<Number>> stateCountCapture;
  private Map<ScheduleStatus, Capture<Supplier<Number>>> stateCaptures;

  private ScheduledExecutorService executor;
  private ScheduledFuture<?> future;
  private StateManager stateManager;
  private FakeClock clock;
  private TaskTimeout timeout;
  private StatsProvider statsProvider;

  @Before
  public void setUp() {
    executor = createMock(ScheduledExecutorService.class);
    future = createMock(new Clazz<ScheduledFuture<?>>() { });
    stateManager = createMock(StateManager.class);
    clock = new FakeClock();
    statsProvider = createMock(StatsProvider.class);
    expectStatsProvider();
  }

  @After
  public void verifyTasksDepleted() {
    // Verify there is no memory leak.
    assertEquals(0, stateCountCapture.getValue().get().intValue());
  }

  private void expectStatsProvider() {
    timedOutTaskCounter = new AtomicLong();
    expect(statsProvider.makeCounter(TaskTimeout.TIMED_OUT_TASKS_COUNTER))
        .andReturn(timedOutTaskCounter);

    stateCountCapture = createCapture();
    expect(statsProvider.makeGauge(
        eq(TaskTimeout.TRANSIENT_COUNT_STAT_NAME),
        EasyMock.capture(stateCountCapture))).andReturn(null);

    stateCaptures = Maps.newHashMap();
    for (ScheduleStatus status : TaskTimeout.TRANSIENT_STATES) {
      Capture<Supplier<Number>> statusCapture = createCapture();
      expect(statsProvider.makeGauge(
          eq(TaskTimeout.waitingTimeStatName(status)),
          EasyMock.capture(statusCapture))).andReturn(null);
      stateCaptures.put(status, statusCapture);
    }
  }

  private void replayAndCreate() {
    control.replay();
    timeout = new TaskTimeout(
        executor,
        stateManager,
        clock,
        Amount.of(TIMEOUT_MS, Time.MILLISECONDS),
        statsProvider);
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
    timeout.recordStateChange(TaskStateChange.transition(task, from));
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

    replayAndCreate();

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

    replayAndCreate();

    changeState(PENDING, ASSIGNED);
    changeState(ASSIGNED, KILLING);
    killingTimeout.getValue().run();
  }

  @Test
  public void testTimeout() throws Exception {
    Capture<Runnable> assignedTimeout = expectTaskWatch();
    Query.Builder query = Query.taskScoped(TASK_ID).byStatus(ASSIGNED);
    expect(stateManager.changeState(query, LOST, TaskTimeout.TIMEOUT_MESSAGE)).andReturn(1);

    replayAndCreate();

    changeState(INIT, PENDING);
    changeState(PENDING, ASSIGNED);
    assignedTimeout.getValue().run();
    assertEquals(timedOutTaskCounter.intValue(), 1);
  }

  @Test
  public void testTaskDeleted() throws Exception {
    Capture<Runnable> assignedTimeout = expectTaskWatch();
    Query.Builder query = Query.taskScoped(TASK_ID).byStatus(KILLING);
    expect(stateManager.changeState(query, LOST, TaskTimeout.TIMEOUT_MESSAGE)).andReturn(0);

    replayAndCreate();

    changeState(INIT, PENDING);
    changeState(PENDING, KILLING);
    assignedTimeout.getValue().run();
    assertEquals(timedOutTaskCounter.intValue(), 0);
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

    expectTaskWatch(TIMEOUT_MS);
    expectTaskWatch(TIMEOUT_MS);
    expectTaskWatch(TIMEOUT_MS);
    expectCancel().times(3);

    replayAndCreate();

    clock.setNowMillis(TIMEOUT_MS * 2);
    for (IScheduledTask task : ImmutableList.of(
        makeTask("a", ASSIGNED, 0),
        makeTask("b", KILLING, TIMEOUT_MS),
        makeTask("c", PREEMPTING, clock.nowMillis() + TIMEOUT_MS))) {

      timeout.recordStateChange(TaskStateChange.initialized(task));
    }

    changeState("a", ASSIGNED, RUNNING);
    changeState("b", KILLING, KILLED);
    changeState("c", PREEMPTING, FINISHED);
  }

  private void checkOutstandingTimer(ScheduleStatus status, long expectedValue) {
    long value = stateCaptures.get(status).getValue().get().longValue();
    assertEquals(expectedValue, value);
  }

  @Test
  public void testOutstandingTimers() throws Exception {
    expectTaskWatch();
    expectTaskWatch();
    expectCancel();
    expectTaskWatch();
    expectCancel().times(2);

    replayAndCreate();

    checkOutstandingTimer(ASSIGNED, 0);
    checkOutstandingTimer(PREEMPTING, 0);
    checkOutstandingTimer(RESTARTING, 0);
    checkOutstandingTimer(KILLING, 0);

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
  }
}

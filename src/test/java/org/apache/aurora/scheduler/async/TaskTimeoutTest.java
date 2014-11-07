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
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
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
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.easymock.Capture;
import org.easymock.EasyMock;
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
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.gen.ScheduleStatus.STARTING;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;

public class TaskTimeoutTest extends EasyMockTest {

  private static final String TASK_ID = "task_id";
  private static final Amount<Long, Time> TIMEOUT = Amount.of(1L, Time.MINUTES);

  private AtomicLong timedOutTaskCounter;
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
    timedOutTaskCounter = new AtomicLong();
    expect(statsProvider.makeCounter(TaskTimeout.TIMED_OUT_TASKS_COUNTER))
        .andReturn(timedOutTaskCounter);
  }

  private void replayAndCreate() {
    control.replay();
    timeout = new TaskTimeout(executor, stateManager, TIMEOUT, statsProvider);
    timeout.startAsync().awaitRunning();
  }

  private Capture<Runnable> expectTaskWatch(Amount<Long, Time> expireIn) {
    Capture<Runnable> capture = createCapture();
    executor.schedule(
        EasyMock.capture(capture),
        eq((long) expireIn.getValue()),
        eq(expireIn.getUnit().getTimeUnit()));
    expectLastCall().andReturn(future);
    return capture;
  }

  private Capture<Runnable> expectTaskWatch() {
    return expectTaskWatch(TIMEOUT);
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
    expectTaskWatch();

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
    Capture<Runnable> killingTimeout = expectTaskWatch();
    expect(stateManager.changeState(
        TASK_ID,
        Optional.of(KILLING),
        LOST,
        TaskTimeout.TIMEOUT_MESSAGE))
        .andReturn(true);

    replayAndCreate();

    changeState(PENDING, ASSIGNED);
    changeState(ASSIGNED, KILLING);
    killingTimeout.getValue().run();
  }

  @Test
  public void testTimeout() throws Exception {
    Capture<Runnable> assignedTimeout = expectTaskWatch();
    expect(stateManager.changeState(
        TASK_ID,
        Optional.of(ASSIGNED),
        LOST,
        TaskTimeout.TIMEOUT_MESSAGE))
        .andReturn(true);

    replayAndCreate();

    changeState(INIT, PENDING);
    changeState(PENDING, ASSIGNED);
    assignedTimeout.getValue().run();
    assertEquals(timedOutTaskCounter.intValue(), 1);
  }

  @Test
  public void testTaskDeleted() throws Exception {
    Capture<Runnable> assignedTimeout = expectTaskWatch();
    expect(stateManager.changeState(
        TASK_ID,
        Optional.of(KILLING),
        LOST,
        TaskTimeout.TIMEOUT_MESSAGE))
        .andReturn(false);

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
    expectTaskWatch(TIMEOUT);
    expectTaskWatch(TIMEOUT);
    expectTaskWatch(TIMEOUT);

    replayAndCreate();

    clock.setNowMillis(TIMEOUT.as(Time.MILLISECONDS) * 2);
    for (IScheduledTask task : ImmutableList.of(
        makeTask("a", ASSIGNED, 0),
        makeTask("b", KILLING, TIMEOUT.as(Time.MILLISECONDS)),
        makeTask("c", PREEMPTING, clock.nowMillis() + TIMEOUT.as(Time.MILLISECONDS)))) {

      timeout.recordStateChange(TaskStateChange.initialized(task));
    }

    changeState("a", ASSIGNED, RUNNING);
    changeState("b", KILLING, KILLED);
    changeState("c", PREEMPTING, FINISHED);
  }

  @Test
  public void testTimeoutWhileNotStarted() throws Exception {
    // Since the timeout is never instructed to start, it should not attempt to transition tasks,
    // but it should try again later.
    Capture<Runnable> assignedTimeout = expectTaskWatch();
    expectTaskWatch(TaskTimeout.NOT_STARTED_RETRY);

    control.replay();
    timeout = new TaskTimeout(executor, stateManager, TIMEOUT, statsProvider);

    changeState(INIT, PENDING);
    changeState(PENDING, ASSIGNED);
    assignedTimeout.getValue().run();
    assertEquals(timedOutTaskCounter.intValue(), 0);
  }
}

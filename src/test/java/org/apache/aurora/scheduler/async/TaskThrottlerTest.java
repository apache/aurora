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
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.INIT;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.gen.ScheduleStatus.THROTTLED;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;

public class TaskThrottlerTest extends EasyMockTest {

  private RescheduleCalculator rescheduleCalculator;
  private FakeClock clock;
  private ScheduledExecutorService executor;
  private StorageTestUtil storageUtil;
  private StateManager stateManager;
  private TaskThrottler throttler;

  @Before
  public void setUp() throws Exception {
    rescheduleCalculator = createMock(RescheduleCalculator.class);
    clock = new FakeClock();
    executor = createMock(ScheduledExecutorService.class);
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    stateManager = createMock(StateManager.class);
    throttler = new TaskThrottler(
        rescheduleCalculator,
        clock,
        executor,
        storageUtil.storage,
        stateManager);
  }

  @Test
  public void testIgnoresNonThrottledTasks() {
    control.replay();

    throttler.taskChangedState(TaskStateChange.transition(makeTask("a", PENDING), INIT));
    throttler.taskChangedState(TaskStateChange.transition(makeTask("a", RUNNING), PENDING));
  }

  @Test
  public void testThrottledTask() {
    IScheduledTask task = makeTask("a", THROTTLED);

    long penaltyMs = 100;

    expect(rescheduleCalculator.getFlappingPenaltyMs(task)).andReturn(penaltyMs);
    Capture<Runnable> stateChangeCapture = expectThrottled(penaltyMs);
    expectMovedToPending(task);

    control.replay();

    throttler.taskChangedState(TaskStateChange.transition(task, INIT));
    stateChangeCapture.getValue().run();
  }

  @Test
  public void testThrottledTaskReady() {
    // Ensures that a sane delay is used when the task's penalty was already expired when
    // the -> THROTTLED transition occurred (such as in the event of a scheduler failover).

    IScheduledTask task = makeTask("a", THROTTLED);

    long penaltyMs = 100;

    expect(rescheduleCalculator.getFlappingPenaltyMs(task)).andReturn(penaltyMs);
    Capture<Runnable> stateChangeCapture = expectThrottled(0);
    expectMovedToPending(task);

    control.replay();

    clock.advance(Amount.of(1L, Time.HOURS));
    throttler.taskChangedState(TaskStateChange.transition(task, INIT));
    stateChangeCapture.getValue().run();
  }

  private Capture<Runnable> expectThrottled(long penaltyMs) {
    Capture<Runnable> stateChangeCapture = createCapture();
    expect(executor.schedule(
        capture(stateChangeCapture),
        eq(penaltyMs),
        eq(TimeUnit.MILLISECONDS)))
        .andReturn(null);
    return stateChangeCapture;
  }

  private void expectMovedToPending(IScheduledTask task) {
    expect(stateManager.changeState(
        storageUtil.mutableStoreProvider,
        Tasks.id(task),
        Optional.of(THROTTLED),
        PENDING,
        Optional.<String>absent()))
        .andReturn(true);
  }

  private IScheduledTask makeTask(String id, ScheduleStatus status) {
    return IScheduledTask.build(new ScheduledTask()
        .setTaskEvents(ImmutableList.of(
            new TaskEvent()
                .setStatus(status)
                .setTimestamp(clock.nowMillis())))
        .setStatus(status)
        .setAssignedTask(new AssignedTask().setTaskId(id)));
  }
}

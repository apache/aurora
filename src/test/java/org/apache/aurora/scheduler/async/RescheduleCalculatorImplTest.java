/*
 * Copyright 2014 Twitter, Inc.
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
import java.util.Map.Entry;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.BackoffStrategy;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.async.RescheduleCalculator.RescheduleCalculatorImpl;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.FINISHED;
import static org.apache.aurora.gen.ScheduleStatus.INIT;
import static org.apache.aurora.gen.ScheduleStatus.KILLED;
import static org.apache.aurora.gen.ScheduleStatus.KILLING;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class RescheduleCalculatorImplTest extends EasyMockTest {

  private static final Amount<Long, Time> FLAPPING_THRESHOLD = Amount.of(1L, Time.MINUTES);
  private static final Amount<Integer, Time> MAX_STARTUP_DELAY = Amount.of(10, Time.MINUTES);

  private StorageTestUtil storageUtil;
  private BackoffStrategy backoff;
  private RescheduleCalculator rescheduleCalculator;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    backoff = createMock(BackoffStrategy.class);
    rescheduleCalculator = new RescheduleCalculatorImpl(
        storageUtil.storage,
        new RescheduleCalculatorImpl.RescheduleCalculatorSettings(
            backoff,
            FLAPPING_THRESHOLD,
            MAX_STARTUP_DELAY));
    storageUtil.expectOperations();
  }

  @Test
  public void testNoPenaltyForNoAncestor() {
    control.replay();

    assertEquals(0L, rescheduleCalculator.getFlappingPenaltyMs(makeTask("a", INIT)));
  }

  @Test
  public void testNoPenaltyDeletedAncestor() {
    String ancestorId = "a";
    storageUtil.expectTaskFetch(Query.taskScoped(ancestorId));

    control.replay();

    assertEquals(
        0L,
        rescheduleCalculator.getFlappingPenaltyMs(setAncestor(makeTask("b", INIT), ancestorId)));
  }

  @Test
  public void testFlappingTask() {
    IScheduledTask ancestor = makeFlappyTask("a");
    storageUtil.expectTaskFetch(Query.taskScoped(Tasks.id(ancestor)), ancestor);
    long penaltyMs = 1000L;
    expect(backoff.calculateBackoffMs(0L)).andReturn(penaltyMs);

    control.replay();

    assertEquals(
        penaltyMs,
        rescheduleCalculator.getFlappingPenaltyMs(
            setAncestor(makeTask("b", INIT), Tasks.id(ancestor))));
  }

  @Test
  public void testFlappingTasksBackoffTruncation() {
    // Ensures that the reschedule calculator detects penalty truncation and avoids inspecting
    // ancestors once truncated.
    IScheduledTask taskA = setAncestor(makeFlappyTask("a"), "bugIfQueried");
    IScheduledTask taskB = setAncestor(makeFlappyTask("b"), Tasks.id(taskA));
    IScheduledTask taskC = setAncestor(makeFlappyTask("c"), Tasks.id(taskB));
    IScheduledTask taskD = setAncestor(makeFlappyTask("d"), Tasks.id(taskC));

    Map<IScheduledTask, Long> ancestorsAndPenalties = ImmutableMap.of(
        taskD, 100L,
        taskC, 200L,
        taskB, 300L,
        taskA, 300L);

    long lastPenalty = 0L;
    for (Map.Entry<IScheduledTask, Long> taskAndPenalty : ancestorsAndPenalties.entrySet()) {
      storageUtil.expectTaskFetch(
          Query.taskScoped(Tasks.id(taskAndPenalty.getKey())),
          taskAndPenalty.getKey());
      expect(backoff.calculateBackoffMs(lastPenalty)).andReturn(taskAndPenalty.getValue());
      lastPenalty = taskAndPenalty.getValue();
    }

    control.replay();

    IScheduledTask newTask = setAncestor(makeFlappyTask("newTask"), Tasks.id(taskD));
    assertEquals(300L, rescheduleCalculator.getFlappingPenaltyMs(newTask));
  }

  @Test
  public void testNoPenaltyForInterruptedTasks() {
    IScheduledTask ancestor = setEvents(
        makeTask("a", KILLED),
        ImmutableMap.of(INIT, 0L, PENDING, 100L, RUNNING, 200L, KILLING, 300L, KILLED, 400L));
    storageUtil.expectTaskFetch(Query.taskScoped(Tasks.id(ancestor)), ancestor);

    control.replay();

    assertEquals(
        0L,
        rescheduleCalculator.getFlappingPenaltyMs(
            setAncestor(makeTask("b", INIT), Tasks.id(ancestor))));
  }

  private IScheduledTask makeFlappyTask(String taskId) {
    return setEvents(
        makeTask(taskId, FINISHED),
        ImmutableMap.of(INIT, 0L, PENDING, 100L, RUNNING, 200L, FINISHED, 300L));
  }

  private IScheduledTask makeTask(String taskId) {
    return IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setInstanceId(0)
            .setTaskId(taskId)
            .setTask(new TaskConfig()
                .setJobName("job-" + taskId)
                .setOwner(new Identity().setRole("role-" + taskId).setUser("user-" + taskId))
                .setEnvironment("env-" + taskId))));
  }

  private IScheduledTask makeTask(String taskId, ScheduleStatus status) {
    return IScheduledTask.build(makeTask(taskId).newBuilder().setStatus(status));
  }

  private IScheduledTask setAncestor(IScheduledTask task, String ancestorId) {
    return IScheduledTask.build(task.newBuilder().setAncestorId(ancestorId));
  }

  private static final Function<Map.Entry<ScheduleStatus, Long>, TaskEvent> TO_EVENT =
      new Function<Entry<ScheduleStatus, Long>, TaskEvent>() {
        @Override
        public TaskEvent apply(Entry<ScheduleStatus, Long> input) {
          return new TaskEvent().setStatus(input.getKey()).setTimestamp(input.getValue());
        }
      };

  private IScheduledTask setEvents(IScheduledTask task, Map<ScheduleStatus, Long> events) {
    return IScheduledTask.build(task.newBuilder().setTaskEvents(
        FluentIterable.from(events.entrySet()).transform(TO_EVENT).toList()));
  }
}

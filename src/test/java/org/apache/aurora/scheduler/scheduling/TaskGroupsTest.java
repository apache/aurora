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
package org.apache.aurora.scheduler.scheduling;

import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.BackoffStrategy;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.scheduling.TaskGroups.TaskGroupBatchWorker;
import org.apache.aurora.scheduler.scheduling.TaskGroups.TaskGroupsSettings;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeScheduledExecutor;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.ASSIGNED;
import static org.apache.aurora.gen.ScheduleStatus.INIT;
import static org.apache.aurora.scheduler.testing.BatchWorkerUtil.expectBatchExecute;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class TaskGroupsTest extends EasyMockTest {
  private static final Amount<Long, Time> FIRST_SCHEDULE_DELAY = Amount.of(1L, Time.MILLISECONDS);
  private static final Amount<Long, Time> RESCHEDULE_DELAY = FIRST_SCHEDULE_DELAY;
  private static final IJobKey JOB_A = IJobKey.build(new JobKey("role", "test", "jobA"));
  private static final String TASK_A_ID = "a";
  private static final Set<String> SCHEDULED_RESULT = ImmutableSet.of(TASK_A_ID);

  private BackoffStrategy backoffStrategy;
  private TaskScheduler taskScheduler;
  private RateLimiter rateLimiter;
  private FakeScheduledExecutor clock;
  private RescheduleCalculator rescheduleCalculator;
  private TaskGroups taskGroups;
  private TaskGroupBatchWorker batchWorker;
  private StorageTestUtil storageUtil;
  private FakeStatsProvider statsProvider;

  @Before
  public void setUp() throws Exception {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    ScheduledExecutorService executor = createMock(ScheduledExecutorService.class);
    clock = FakeScheduledExecutor.fromScheduledExecutorService(executor);
    backoffStrategy = createMock(BackoffStrategy.class);
    taskScheduler = createMock(TaskScheduler.class);
    rateLimiter = createMock(RateLimiter.class);
    rescheduleCalculator = createMock(RescheduleCalculator.class);
    batchWorker = createMock(TaskGroupBatchWorker.class);
    statsProvider = new FakeStatsProvider();
    taskGroups = new TaskGroups(
        executor,
        new TaskGroupsSettings(FIRST_SCHEDULE_DELAY, backoffStrategy, rateLimiter, 2),
        taskScheduler,
        rescheduleCalculator,
        batchWorker,
        statsProvider);
  }

  @Test
  public void testEvaluatedAfterFirstSchedulePenalty() throws Exception {
    expect(rateLimiter.acquire()).andReturn(0D);
    expect(taskScheduler.schedule(anyObject(), eq(ImmutableSet.of(TASK_A_ID))))
        .andReturn(SCHEDULED_RESULT);
    expectBatchExecute(batchWorker, storageUtil.storage, control, SCHEDULED_RESULT)
        .anyTimes();

    control.replay();

    taskGroups.taskChangedState(TaskStateChange.transition(makeTask(TASK_A_ID), INIT));
    clock.advance(FIRST_SCHEDULE_DELAY);
    assertEquals(0L, statsProvider.getLongValue(TaskGroups.SCHEDULE_ATTEMPTS_BLOCKS));
  }

  @Test
  public void testTaskDeletedBeforeEvaluating() throws Exception {
    final IScheduledTask task = makeTask(TASK_A_ID);
    expect(rateLimiter.acquire()).andReturn(0.5D);
    expect(taskScheduler.schedule(anyObject(), eq(ImmutableSet.of(TASK_A_ID))))
        .andAnswer(() -> {
          // Test a corner case where a task is deleted while it is being evaluated by the task
          // scheduler.  If not handled carefully, this could result in the scheduler trying again
          // later to satisfy the deleted task.
          taskGroups.tasksDeleted(new PubsubEvent.TasksDeleted(ImmutableSet.of(task)));

          return ImmutableSet.of();
        });
    expectBatchExecute(batchWorker, storageUtil.storage, control, ImmutableSet.of())
        .anyTimes();
    expect(backoffStrategy.calculateBackoffMs(FIRST_SCHEDULE_DELAY.as(Time.MILLISECONDS)))
        .andReturn(0L);

    control.replay();

    taskGroups.taskChangedState(TaskStateChange.transition(makeTask(Tasks.id(task)), INIT));
    clock.advance(FIRST_SCHEDULE_DELAY);
    assertEquals(1L, statsProvider.getLongValue(TaskGroups.SCHEDULE_ATTEMPTS_BLOCKS));
  }

  @Test
  public void testEvaluatedOnStartup() throws Exception {
    expect(rateLimiter.acquire()).andReturn(0.000000001D);
    expect(rescheduleCalculator.getStartupScheduleDelayMs(makeTask(TASK_A_ID))).andReturn(1L);
    expect(taskScheduler.schedule(anyObject(), eq(ImmutableSet.of(TASK_A_ID))))
        .andReturn(ImmutableSet.of(TASK_A_ID));
    expectBatchExecute(batchWorker, storageUtil.storage, control, SCHEDULED_RESULT)
        .anyTimes();

    control.replay();

    taskGroups.taskChangedState(TaskStateChange.initialized(makeTask(TASK_A_ID)));
    clock.advance(FIRST_SCHEDULE_DELAY);
    clock.advance(RESCHEDULE_DELAY);
    assertEquals(1L, statsProvider.getLongValue(TaskGroups.SCHEDULE_ATTEMPTS_BLOCKS));
  }

  @Test
  public void testMultipleTasksAndResistStarvation() throws Exception {
    expect(rateLimiter.acquire()).andReturn(0.001D).times(2);
    expect(taskScheduler.schedule(anyObject(), eq(ImmutableSet.of("a0", "a1"))))
        .andReturn(ImmutableSet.of("a0", "a1"));
    expect(taskScheduler.schedule(anyObject(), eq(ImmutableSet.of("b0"))))
        .andReturn(ImmutableSet.of("b0"));
    expectBatchExecute(
        batchWorker,
        storageUtil.storage,
        control,
        ImmutableSet.of("a0", "a1")).anyTimes();
    expectBatchExecute(batchWorker, storageUtil.storage, control, ImmutableSet.of("b0"))
        .anyTimes();

    control.replay();

    taskGroups.taskChangedState(TaskStateChange.transition(makeTask(JOB_A, "a0", 0), INIT));
    taskGroups.taskChangedState(TaskStateChange.transition(makeTask(JOB_A, "a1", 1), INIT));
    taskGroups.taskChangedState(TaskStateChange.transition(makeTask(JOB_A, "a2", 2), INIT));
    taskGroups.taskChangedState(TaskStateChange.transition(
        makeTask(IJobKey.build(JOB_A.newBuilder().setName("jobB")), "b0", 0), INIT));

    clock.advance(FIRST_SCHEDULE_DELAY);
    assertEquals(2L, statsProvider.getLongValue(TaskGroups.SCHEDULE_ATTEMPTS_BLOCKS));
  }

  @Test
  public void testNonPendingIgnored() {
    control.replay();

    IScheduledTask task =
        IScheduledTask.build(makeTask(TASK_A_ID).newBuilder().setStatus(ASSIGNED));
    taskGroups.taskChangedState(TaskStateChange.initialized(task));
  }

  private static IScheduledTask makeTask(String id) {
    return makeTask(JOB_A, id, 0);
  }

  private static IScheduledTask makeTask(IJobKey jobKey, String id, int instanceId) {
    return IScheduledTask.build(new ScheduledTask()
        .setStatus(ScheduleStatus.PENDING)
        .setAssignedTask(new AssignedTask()
            .setInstanceId(instanceId)
            .setTaskId(id)
            .setTask(new TaskConfig()
                .setJob(jobKey.newBuilder()))));
  }
}

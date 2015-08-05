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

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.BackoffStrategy;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.async.DelayExecutor;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.events.PubsubEvent.TasksDeleted;
import org.apache.aurora.scheduler.scheduling.TaskGroups.TaskGroupsSettings;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.testing.FakeScheduledExecutor;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.ASSIGNED;
import static org.apache.aurora.gen.ScheduleStatus.INIT;
import static org.easymock.EasyMock.expect;

public class TaskGroupsTest extends EasyMockTest {
  private static final Amount<Long, Time> FIRST_SCHEDULE_DELAY = Amount.of(1L, Time.MILLISECONDS);
  private static final Amount<Long, Time> RESCHEDULE_DELAY = FIRST_SCHEDULE_DELAY;
  private static final IJobKey JOB_A = IJobKey.build(new JobKey("role", "test", "jobA"));
  private static final String TASK_A_ID = "a";

  private BackoffStrategy backoffStrategy;
  private TaskScheduler taskScheduler;
  private RateLimiter rateLimiter;
  private FakeScheduledExecutor clock;
  private RescheduleCalculator rescheduleCalculator;
  private TaskGroups taskGroups;

  @Before
  public void setUp() throws Exception {
    DelayExecutor executor = createMock(DelayExecutor.class);
    clock = FakeScheduledExecutor.fromDelayExecutor(executor);
    backoffStrategy = createMock(BackoffStrategy.class);
    taskScheduler = createMock(TaskScheduler.class);
    rateLimiter = createMock(RateLimiter.class);
    rescheduleCalculator = createMock(RescheduleCalculator.class);
    taskGroups = new TaskGroups(
        executor,
        new TaskGroupsSettings(FIRST_SCHEDULE_DELAY, backoffStrategy, rateLimiter),
        taskScheduler,
        rescheduleCalculator);
  }

  @Test
  public void testEvaluatedAfterFirstSchedulePenalty() {
    expect(rateLimiter.acquire()).andReturn(0D);
    expect(taskScheduler.schedule(TASK_A_ID)).andReturn(true);

    control.replay();

    taskGroups.taskChangedState(TaskStateChange.transition(makeTask(TASK_A_ID), INIT));
    clock.advance(FIRST_SCHEDULE_DELAY);
  }

  @Test
  public void testTaskDeletedBeforeEvaluating() {
    final IScheduledTask task = makeTask(TASK_A_ID);
    expect(rateLimiter.acquire()).andReturn(0D);
    expect(taskScheduler.schedule(Tasks.id(task))).andAnswer(new IAnswer<Boolean>() {
      @Override
      public Boolean answer() {
        // Test a corner case where a task is deleted while it is being evaluated by the task
        // scheduler.  If not handled carefully, this could result in the scheduler trying again
        // later to satisfy the deleted task.
        taskGroups.tasksDeleted(new TasksDeleted(ImmutableSet.of(task)));

        return false;
      }
    });
    expect(backoffStrategy.calculateBackoffMs(FIRST_SCHEDULE_DELAY.as(Time.MILLISECONDS)))
        .andReturn(0L);

    control.replay();

    taskGroups.taskChangedState(TaskStateChange.transition(makeTask(Tasks.id(task)), INIT));
    clock.advance(FIRST_SCHEDULE_DELAY);
  }

  @Test
  public void testEvaluatedOnStartup() {
    expect(rateLimiter.acquire()).andReturn(0D);
    expect(rescheduleCalculator.getStartupScheduleDelayMs(makeTask(TASK_A_ID))).andReturn(1L);
    expect(taskScheduler.schedule(TASK_A_ID)).andReturn(true);

    control.replay();

    taskGroups.taskChangedState(TaskStateChange.initialized(makeTask(TASK_A_ID)));
    clock.advance(FIRST_SCHEDULE_DELAY);
    clock.advance(RESCHEDULE_DELAY);
  }

  @Test
  public void testResistStarvation() {
    expect(rateLimiter.acquire()).andReturn(0D).times(2);
    expect(taskScheduler.schedule("a0")).andReturn(true);
    expect(taskScheduler.schedule("b0")).andReturn(true);

    control.replay();

    taskGroups.taskChangedState(TaskStateChange.transition(makeTask(JOB_A, "a0", 0), INIT));
    taskGroups.taskChangedState(TaskStateChange.transition(makeTask(JOB_A, "a1", 1), INIT));
    taskGroups.taskChangedState(TaskStateChange.transition(makeTask(JOB_A, "a2", 2), INIT));
    taskGroups.taskChangedState(TaskStateChange.transition(
        makeTask(IJobKey.build(JOB_A.newBuilder().setName("jobB")), "b0", 0), INIT));

    clock.advance(FIRST_SCHEDULE_DELAY);
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

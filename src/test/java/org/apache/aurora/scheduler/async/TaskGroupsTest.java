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

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.BackoffStrategy;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.events.PubsubEvent.TasksDeleted;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.apache.aurora.gen.ScheduleStatus.INIT;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;

public class TaskGroupsTest extends EasyMockTest {

  private static final long FIRST_SCHEDULE_DELAY_MS = 1L;

  private ScheduledExecutorService executor;
  private BackoffStrategy backoffStrategy;
  private TaskScheduler taskScheduler;
  private RateLimiter rateLimiter;

  private TaskGroups taskGroups;

  @Before
  public void setUp() throws Exception {
    executor = createMock(ScheduledExecutorService.class);
    backoffStrategy = createMock(BackoffStrategy.class);
    taskScheduler = createMock(TaskScheduler.class);
    rateLimiter = createMock(RateLimiter.class);
    taskGroups = new TaskGroups(
        executor,
        Amount.of(FIRST_SCHEDULE_DELAY_MS, Time.MILLISECONDS),
        backoffStrategy,
        rateLimiter,
        taskScheduler,
        createMock(RescheduleCalculator.class));
  }

  @Test
  public void testEvaluatedAfterFirstSchedulePenalty() {
    executor.schedule(
        EasyMock.<Runnable>anyObject(),
        EasyMock.eq(FIRST_SCHEDULE_DELAY_MS),
        EasyMock.eq(MILLISECONDS));
    expectLastCall().andAnswer(new IAnswer<ScheduledFuture<Void>>() {
      @Override
      public ScheduledFuture<Void> answer() {
        ((Runnable) EasyMock.getCurrentArguments()[0]).run();
        return null;
      }
    });
    expect(rateLimiter.acquire()).andReturn(0D);
    expect(taskScheduler.schedule("a")).andReturn(true);

    control.replay();

    taskGroups.taskChangedState(TaskStateChange.transition(makeTask("a"), INIT));
  }

  private Capture<Runnable> expectEvaluate() {
    Capture<Runnable> capture = createCapture();
    executor.schedule(
        EasyMock.capture(capture),
        EasyMock.eq(FIRST_SCHEDULE_DELAY_MS),
        EasyMock.eq(MILLISECONDS));
    expectLastCall().andReturn(null);
    return capture;
  }

  @Test
  public void testTaskDeletedBeforeEvaluating() {
    final IScheduledTask task = makeTask("a");

    Capture<Runnable> evaluate = expectEvaluate();

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
    expect(backoffStrategy.calculateBackoffMs(FIRST_SCHEDULE_DELAY_MS)).andReturn(0L);

    control.replay();

    taskGroups.taskChangedState(TaskStateChange.transition(makeTask(Tasks.id(task)), INIT));
    evaluate.getValue().run();
  }

  private static IScheduledTask makeTask(String id) {
    return IScheduledTask.build(new ScheduledTask()
        .setStatus(ScheduleStatus.PENDING)
        .setAssignedTask(new AssignedTask()
            .setTaskId(id)
            .setTask(new TaskConfig()
                .setOwner(new Identity("owner", "owner"))
                .setEnvironment("test")
                .setJobName("job"))));
  }
}

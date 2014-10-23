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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.async.TaskHistoryPruner.HistoryPrunnerSettings;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.ASSIGNED;
import static org.apache.aurora.gen.ScheduleStatus.FINISHED;
import static org.apache.aurora.gen.ScheduleStatus.KILLED;
import static org.apache.aurora.gen.ScheduleStatus.LOST;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.gen.ScheduleStatus.STARTING;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.fail;

public class TaskHistoryPrunerTest extends EasyMockTest {
  private static final String JOB_A = "job-a";
  private static final String TASK_ID = "task_id";
  private static final String SLAVE_HOST = "HOST_A";
  private static final Amount<Long, Time> ONE_MS = Amount.of(1L, Time.MILLISECONDS);
  private static final Amount<Long, Time> ONE_MINUTE = Amount.of(1L, Time.MINUTES);
  private static final Amount<Long, Time> ONE_DAY = Amount.of(1L, Time.DAYS);
  private static final Amount<Long, Time> ONE_HOUR = Amount.of(1L, Time.HOURS);
  private static final int PER_JOB_HISTORY = 2;

  private ScheduledFuture<?> future;
  private ScheduledExecutorService executor;
  private FakeClock clock;
  private StateManager stateManager;
  private StorageTestUtil storageUtil;
  private TaskHistoryPruner pruner;

  @Before
  public void setUp() {
    future = createMock(new Clazz<ScheduledFuture<?>>() { });
    executor = createMock(ScheduledExecutorService.class);
    clock = new FakeClock();
    stateManager = createMock(StateManager.class);
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    pruner = new TaskHistoryPruner(
        executor,
        stateManager,
        clock,
        new HistoryPrunnerSettings(ONE_DAY, ONE_MINUTE, PER_JOB_HISTORY),
        storageUtil.storage);
  }

  @Test
  public void testNoPruning() {
    long taskATimestamp = clock.nowMillis();
    IScheduledTask a = makeTask("a", FINISHED);

    clock.advance(ONE_MS);
    long taskBTimestamp = clock.nowMillis();
    IScheduledTask b = makeTask("b", LOST);

    expectNoImmediatePrune(ImmutableSet.of(a));
    expectOneDelayedPrune(taskATimestamp);
    expectNoImmediatePrune(ImmutableSet.of(a, b));
    expectOneDelayedPrune(taskBTimestamp);

    control.replay();

    pruner.recordStateChange(TaskStateChange.initialized(a));
    pruner.recordStateChange(TaskStateChange.initialized(b));
  }

  @Test
  public void testStorageStartedWithPruning() {
    long taskATimestamp = clock.nowMillis();
    IScheduledTask a = makeTask("a", FINISHED);

    clock.advance(ONE_MINUTE);
    long taskBTimestamp = clock.nowMillis();
    IScheduledTask b = makeTask("b", LOST);

    clock.advance(ONE_MINUTE);
    long taskCTimestamp = clock.nowMillis();
    IScheduledTask c = makeTask("c", FINISHED);

    clock.advance(ONE_MINUTE);
    IScheduledTask d = makeTask("d", FINISHED);
    IScheduledTask e = makeTask("job-x", "e", FINISHED);

    expectNoImmediatePrune(ImmutableSet.of(a));
    expectOneDelayedPrune(taskATimestamp);
    expectNoImmediatePrune(ImmutableSet.of(a, b));
    expectOneDelayedPrune(taskBTimestamp);
    expectImmediatePrune(ImmutableSet.of(a, b, c), a);
    expectOneDelayedPrune(taskCTimestamp);
    expectImmediatePrune(ImmutableSet.of(b, c, d), b);
    expectDefaultDelayedPrune();
    expectNoImmediatePrune(ImmutableSet.of(e));
    expectDefaultDelayedPrune();

    control.replay();

    for (IScheduledTask task : ImmutableList.of(a, b, c, d, e)) {
      pruner.recordStateChange(TaskStateChange.initialized(task));
    }
  }

  @Test
  public void testStateChange() {
    IScheduledTask starting = makeTask("a", STARTING);
    IScheduledTask running = copy(starting, RUNNING);
    IScheduledTask killed = copy(starting, KILLED);

    expectNoImmediatePrune(ImmutableSet.of(killed));
    expectDefaultDelayedPrune();

    control.replay();

    // No future set for non-terminal state transition.
    changeState(starting, running);

    // Future set for terminal state transition.
    changeState(running, killed);
  }

  @Test
  public void testActivateFutureAndExceedHistoryGoal() {
    IScheduledTask running = makeTask("a", RUNNING);
    IScheduledTask killed = copy(running, KILLED);
    expectNoImmediatePrune(ImmutableSet.of(running));
    Capture<Runnable> delayedDelete = expectDefaultDelayedPrune();

    // Expect task "a" to be pruned when future is activated.
    expectDeleteTasks("a");

    control.replay();

    // Capture future for inactive task "a"
    changeState(running, killed);
    clock.advance(ONE_HOUR);
    // Execute future to prune task "a" from the system.
    delayedDelete.getValue().run();
  }

  @Test
  public void testJobHistoryExceeded() {
    IScheduledTask a = makeTask("a", RUNNING);
    clock.advance(ONE_MS);
    IScheduledTask aKilled = copy(a, KILLED);

    IScheduledTask b = makeTask("b", RUNNING);
    clock.advance(ONE_MS);
    IScheduledTask bKilled = copy(b, KILLED);

    IScheduledTask c = makeTask("c", RUNNING);
    clock.advance(ONE_MS);
    IScheduledTask cLost = copy(c, LOST);

    IScheduledTask d = makeTask("d", RUNNING);
    clock.advance(ONE_MS);
    IScheduledTask dLost = copy(d, LOST);

    expectNoImmediatePrune(ImmutableSet.of(a));
    expectDefaultDelayedPrune();
    expectNoImmediatePrune(ImmutableSet.of(a, b));
    expectDefaultDelayedPrune();
    expectNoImmediatePrune(ImmutableSet.of(a, b)); // no pruning yet due to min threshold
    expectDefaultDelayedPrune();
    clock.advance(ONE_HOUR);
    expectImmediatePrune(ImmutableSet.of(a, b, c, d), a, b); // now prune 2 tasks
    expectDefaultDelayedPrune();

    control.replay();

    changeState(a, aKilled);
    changeState(b, bKilled);
    changeState(c, cLost);
    changeState(d, dLost);
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
      @Override
      public void execute() {
        // The goal is to verify that the call does not deadlock. We do not care about the outcome.
        IScheduledTask b = makeTask("b", ASSIGNED);

        changeState(b, STARTING);
      }
    };
    CountDownLatch taskDeleted = expectTaskDeleted(onDeleted, TASK_ID);

    control.replay();

    // Change the task to a terminal state and wait for it to be pruned.
    changeState(makeTask(TASK_ID, RUNNING), KILLED);
    taskDeleted.await();
  }

  private TaskHistoryPruner prunerWithRealExecutor() {
    ScheduledExecutorService realExecutor = Executors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("testThreadSafeEvents-executor")
            .build());
    return new TaskHistoryPruner(
        realExecutor,
        stateManager,
        clock,
        new HistoryPrunnerSettings(Amount.of(1L, Time.MILLISECONDS), ONE_MS, PER_JOB_HISTORY),
        storageUtil.storage);
  }

  private CountDownLatch expectTaskDeleted(final Command onDelete, String taskId) {
    final CountDownLatch deleteCalled = new CountDownLatch(1);
    final CountDownLatch eventDelivered = new CountDownLatch(1);

    Thread eventDispatch = new Thread() {
      @Override
      public void run() {
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
      @Override
      public Void answer() {
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

  private void expectDeleteTasks(String... tasks) {
    stateManager.deleteTasks(ImmutableSet.copyOf(tasks));
  }

  private Capture<Runnable> expectDefaultDelayedPrune() {
    return expectDelayedPrune(ONE_DAY.as(Time.MILLISECONDS), 1);
  }

  private Capture<Runnable> expectOneDelayedPrune(long timestampMillis) {
    return expectDelayedPrune(timestampMillis, 1);
  }

  private void expectNoImmediatePrune(ImmutableSet<IScheduledTask> tasksInJob) {
    expectImmediatePrune(tasksInJob);
  }

  private void expectImmediatePrune(
      ImmutableSet<IScheduledTask> tasksInJob,
      IScheduledTask... pruned) {

    // Expect a deferred prune operation when a new task is being watched.
    executor.submit(EasyMock.<Runnable>anyObject());
    expectLastCall().andAnswer(
        new IAnswer<Future<?>>() {
          @Override
          public Future<?> answer() {
            Runnable work = (Runnable) EasyMock.getCurrentArguments()[0];
            work.run();
            return null;
          }
        }
    );

    IJobKey jobKey = Iterables.getOnlyElement(
        FluentIterable.from(tasksInJob).transform(Tasks.SCHEDULED_TO_JOB_KEY).toSet());
    storageUtil.expectTaskFetch(TaskHistoryPruner.jobHistoryQuery(jobKey), tasksInJob);
    if (pruned.length > 0) {
      stateManager.deleteTasks(Tasks.ids(pruned));
    }
  }

  private Capture<Runnable> expectDelayedPrune(long timestampMillis, int count) {
    Capture<Runnable> capture = createCapture();
    executor.schedule(
        EasyMock.capture(capture),
        eq(pruner.calculateTimeout(timestampMillis)),
        eq(TimeUnit.MILLISECONDS));
    expectLastCall().andReturn(future).times(count);
    return capture;
  }

  private void changeState(IScheduledTask oldStateTask, IScheduledTask newStateTask) {
    pruner.recordStateChange(TaskStateChange.transition(newStateTask, oldStateTask.getStatus()));
  }

  private void changeState(IScheduledTask oldStateTask, ScheduleStatus status) {
    pruner.recordStateChange(
        TaskStateChange.transition(copy(oldStateTask, status), oldStateTask.getStatus()));
  }

  private IScheduledTask copy(IScheduledTask task, ScheduleStatus status) {
    return IScheduledTask.build(task.newBuilder().setStatus(status));
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
            .setJob(new JobKey("role", "staging45", job))
            .setOwner(new Identity().setRole("role").setUser("user"))
            .setEnvironment("staging45")
            .setJobName(job)
            .setExecutorConfig(new ExecutorConfig("aurora", "config")));
  }
}

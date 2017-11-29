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
package org.apache.aurora.scheduler.pruning;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.io.Closer;

import org.apache.aurora.common.application.Lifecycle;
import org.apache.aurora.common.base.Command;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.scheduler.SchedulerModule.TaskEventBatchWorker;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.pruning.TaskHistoryPruner.HistoryPrunerSettings;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.FINISHED;
import static org.apache.aurora.gen.ScheduleStatus.KILLED;
import static org.apache.aurora.gen.ScheduleStatus.LOST;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.gen.ScheduleStatus.STARTING;
import static org.apache.aurora.scheduler.pruning.TaskHistoryPruner.TASKS_PRUNED;
import static org.apache.aurora.scheduler.testing.BatchWorkerUtil.expectBatchExecute;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;

public class TaskHistoryPrunerTest extends EasyMockTest {
  private static final String SLAVE_HOST = "HOST_A";
  private static final Amount<Long, Time> ONE_MS = Amount.of(1L, Time.MILLISECONDS);
  private static final Amount<Long, Time> ONE_MINUTE = Amount.of(1L, Time.MINUTES);
  private static final Amount<Long, Time> ONE_DAY = Amount.of(1L, Time.DAYS);
  private static final Amount<Long, Time> ONE_HOUR = Amount.of(1L, Time.HOURS);
  private static final int PER_JOB_HISTORY = 2;

  private ScheduledExecutorService executor;
  private FakeClock clock;
  private StateManager stateManager;
  private StorageTestUtil storageUtil;
  private TaskHistoryPruner pruner;
  private Closer closer;
  private Command shutdownCommand;
  private FakeStatsProvider statsProvider;

  @Before
  public void setUp() throws Exception {
    executor = createMock(ScheduledExecutorService.class);
    clock = new FakeClock();
    stateManager = createMock(StateManager.class);
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    shutdownCommand = createMock(Command.class);
    TaskEventBatchWorker batchWorker = createMock(TaskEventBatchWorker.class);
    statsProvider = new FakeStatsProvider();
    expectBatchExecute(batchWorker, storageUtil.storage, control).anyTimes();

    pruner = new TaskHistoryPruner(
        executor,
        stateManager,
        clock,
        new HistoryPrunerSettings(ONE_DAY, ONE_MINUTE, PER_JOB_HISTORY),
        storageUtil.storage,
        new Lifecycle(shutdownCommand),
        batchWorker,
        statsProvider);
    closer = Closer.create();
  }

  @After
  public void tearDownCloser() throws Exception {
    closer.close();
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
    IScheduledTask e = makeTask(JobKeys.from("role", "env", "job-x"), "e", FINISHED);

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

    assertEquals(0L, statsProvider.getValue(TASKS_PRUNED));
    for (IScheduledTask task : ImmutableList.of(a, b, c, d, e)) {
      pruner.recordStateChange(TaskStateChange.initialized(task));
    }
    assertEquals(2L, statsProvider.getValue(TASKS_PRUNED));
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
    assertEquals(0L, statsProvider.getValue(TASKS_PRUNED));
    // Execute future to prune task "a" from the system.
    delayedDelete.getValue().run();
    assertEquals(1L, statsProvider.getValue(TASKS_PRUNED));
  }

  @Test
  public void testSuppressEmptyDelete() {
    IScheduledTask running = makeTask("a", RUNNING);
    IScheduledTask killed = copy(running, KILLED);
    expectImmediatePrune(
        ImmutableSet.of(makeTask("b", KILLED), makeTask("c", KILLED), makeTask("d", KILLED)));
    expectDefaultDelayedPrune();

    control.replay();

    changeState(running, killed);
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
    assertEquals(0L, statsProvider.getValue(TASKS_PRUNED));
    changeState(d, dLost);
    assertEquals(2L, statsProvider.getValue(TASKS_PRUNED));
  }

  @Test
  public void serviceShutdownOnFailure() {
    IScheduledTask running = makeTask("a", RUNNING);
    IScheduledTask killed = copy(running, KILLED);
    expectNoImmediatePrune(ImmutableSet.of(running));
    Capture<Runnable> delayedDelete = expectDefaultDelayedPrune();

    expectDeleteTasks("a");
    expectLastCall().andThrow(new RuntimeException("oops"));

    shutdownCommand.execute();

    control.replay();

    changeState(running, killed);
    clock.advance(ONE_HOUR);
    assertEquals(0L, statsProvider.getValue(TASKS_PRUNED));
    delayedDelete.getValue().run();
    assertEquals(0L, statsProvider.getValue(TASKS_PRUNED));
  }

  private void expectDeleteTasks(String... tasks) {
    stateManager.deleteTasks(storageUtil.mutableStoreProvider, ImmutableSet.copyOf(tasks));
  }

  private Capture<Runnable> expectDefaultDelayedPrune() {
    return expectDelayedPrune(ONE_DAY.as(Time.MILLISECONDS));
  }

  private Capture<Runnable> expectOneDelayedPrune(long timestampMillis) {
    return expectDelayedPrune(timestampMillis);
  }

  private void expectNoImmediatePrune(ImmutableSet<IScheduledTask> tasksInJob) {
    expectImmediatePrune(tasksInJob);
  }

  private void expectImmediatePrune(
      ImmutableSet<IScheduledTask> tasksInJob,
      IScheduledTask... pruned) {

    // Expect a deferred prune operation when a new task is being watched.
    executor.execute(EasyMock.<Runnable>anyObject());
    expectLastCall().andAnswer(
        () -> {
          Runnable work = (Runnable) EasyMock.getCurrentArguments()[0];
          work.run();
          return null;
        }
    );

    IJobKey jobKey = Iterables.getOnlyElement(
        FluentIterable.from(tasksInJob).transform(Tasks::getJob).toSet());
    storageUtil.expectTaskFetch(TaskHistoryPruner.jobHistoryQuery(jobKey), tasksInJob);
    if (pruned.length > 0) {
      stateManager.deleteTasks(storageUtil.mutableStoreProvider, Tasks.ids(pruned));
    }
  }

  private Capture<Runnable> expectDelayedPrune(long timestampMillis) {
    Capture<Runnable> capture = createCapture();
    expect(executor.schedule(
        EasyMock.capture(capture),
        eq(pruner.calculateTimeout(timestampMillis)),
        eq(TimeUnit.MILLISECONDS)))
        .andReturn(null);
    return capture;
  }

  private void changeState(IScheduledTask oldStateTask, IScheduledTask newStateTask) {
    pruner.recordStateChange(TaskStateChange.transition(newStateTask, oldStateTask.getStatus()));
  }

  private IScheduledTask copy(IScheduledTask task, ScheduleStatus status) {
    return IScheduledTask.build(task.newBuilder().setStatus(status));
  }

  private IScheduledTask makeTask(
      IJobKey job,
      String taskId,
      ScheduleStatus status) {

    ScheduledTask builder = TaskTestUtil.addStateTransition(
        TaskTestUtil.makeTask(taskId, job), status, clock.nowMillis())
        .newBuilder();
    builder.getAssignedTask().setSlaveHost(SLAVE_HOST);
    return IScheduledTask.build(builder);
  }

  private IScheduledTask makeTask(String taskId, ScheduleStatus status) {
    return makeTask(TaskTestUtil.JOB, taskId, status);
  }

}

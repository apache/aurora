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
package org.apache.aurora.scheduler.state;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.gen.PartitionPolicy;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.PARTITIONED;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;

public class PartitionManagerTest extends EasyMockTest {
  private final FakeClock clock = new FakeClock();
  private PartitionManager partitionManager;
  private StateManager stateManager;
  private ScheduledExecutorService executor;

  private static final IScheduledTask BASE_TASK = TaskTestUtil.makeTask("test", TaskTestUtil.JOB);
  private StorageTestUtil storageUtil;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    stateManager = createMock(StateManager.class);
    executor = createMock(ScheduledExecutorService.class);
    partitionManager = new PartitionManager(storageUtil.storage, stateManager, clock, executor);
  }

  private static TaskStateChange makeStateChange(
      IScheduledTask task,
      Optional<ScheduleStatus> oldState,
      ScheduleStatus newState) {

    IScheduledTask newTask = IScheduledTask.build(task.newBuilder().setStatus(newState));
    if (oldState.isPresent()) {
      return TaskStateChange.transition(newTask, oldState.get());
    }
    return TaskStateChange.initialized(newTask);
  }

  private static TaskStateChange makeStateChange(
      IScheduledTask task,
      ScheduleStatus oldState,
      ScheduleStatus newState) {

    return makeStateChange(task, Optional.of(oldState), newState);
  }

  private static TaskStateChange makeStateChange(IScheduledTask task, ScheduleStatus newState) {
    return makeStateChange(task, Optional.absent(), newState);
  }

  private static IScheduledTask taskWithoutPartitionPolicy() {
    return taskWithPartitionPolicy(null);
  }

  private static IScheduledTask taskWithPartitionPolicy(PartitionPolicy partitionPolicy) {
    return IScheduledTask.build(BASE_TASK.newBuilder()
        .setAssignedTask(BASE_TASK.getAssignedTask().newBuilder().setTask(
            BASE_TASK.getAssignedTask().getTask().newBuilder().setPartitionPolicy(partitionPolicy))
        ));
  }

  private Capture<Callable<Void>> expectExecuteWithDelay(long seconds) {
    Capture<Callable<Void>> callableCapture = createCapture();
    expect(
      executor.schedule(capture(callableCapture), eq(seconds), eq(TimeUnit.SECONDS))
    ).andReturn(createMock(new Clazz<ScheduledFuture<Void>>() { }));
    return callableCapture;
  }

  @Test
  public void testNonPartitionedTransition() {
    control.replay();
    partitionManager.handle(makeStateChange(taskWithoutPartitionPolicy(), RUNNING));
  }

  @Test
  public void testPartitionPolicyNoReschedule() {
    control.replay();
    partitionManager.handle(makeStateChange(
        taskWithPartitionPolicy(new PartitionPolicy().setReschedule(false)),
        RUNNING,
        PARTITIONED));
  }

  @Test
  public void testNoPartitionPolicy() {
    expectExecuteWithDelay(0);
    control.replay();
    partitionManager.handle(makeStateChange(taskWithoutPartitionPolicy(), RUNNING, PARTITIONED));
  }

  @Test
  public void testPartitionPolicyWithDelay() {
    expectExecuteWithDelay(10);
    control.replay();
    partitionManager.handle(makeStateChange(
        taskWithPartitionPolicy(new PartitionPolicy().setReschedule(true).setDelaySecs(10)),
        RUNNING,
        PARTITIONED));
  }

  @Test
  public void testDelayAfterFailover() {
    IScheduledTask failoverTask = TaskTestUtil.addStateTransition(
        taskWithPartitionPolicy(new PartitionPolicy().setReschedule(true).setDelaySecs(10)),
        PARTITIONED,
        0);
    clock.setNowMillis(5000);
    expectExecuteWithDelay(5);
    control.replay();
    partitionManager.handle(makeStateChange(
        failoverTask,
        PARTITIONED));
  }

  @Test
  public void testStateChangeToLostNoModifications() throws Exception {
    IScheduledTask task = taskWithoutPartitionPolicy();
    Capture<Callable<Void>> stateChange = expectExecuteWithDelay(0);
    storageUtil.expectOperations();
    storageUtil.expectTaskFetch(Tasks.id(task), task);
    expect(stateManager.changeState(
        storageUtil.mutableStoreProvider,
        Tasks.id(task),
        Optional.of(PARTITIONED),
        ScheduleStatus.LOST,
        Optional.of(PartitionManager.TRANSITION_MESSAGE))).andReturn(StateChangeResult.SUCCESS);
    control.replay();
    partitionManager.handle(makeStateChange(task, RUNNING, PARTITIONED));
    stateChange.getValue().call();
  }

  @Test
  public void testNoStateChangeAfterTaskTransition() throws Exception {
    IScheduledTask task = taskWithoutPartitionPolicy();
    Capture<Callable<Void>> stateChange = expectExecuteWithDelay(0);
    storageUtil.expectOperations();
    storageUtil.expectTaskFetch(
        Tasks.id(task),
        TaskTestUtil.addStateTransition(task, RUNNING, 10000));
    control.replay();
    partitionManager.handle(makeStateChange(task, RUNNING, PARTITIONED));
    stateChange.getValue().call();
  }
}

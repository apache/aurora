/*
 * Copyright 2013 Twitter, Inc.
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
package com.twitter.aurora.scheduler.state;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.apache.mesos.Protos.SlaveID;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IArgumentMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.gen.AssignedTask;
import com.twitter.aurora.gen.Identity;
import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.ShardUpdateResult;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.gen.TaskEvent;
import com.twitter.aurora.gen.UpdateResult;
import com.twitter.aurora.scheduler.Driver;
import com.twitter.aurora.scheduler.TaskIdGenerator;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.events.PubsubEvent;
import com.twitter.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import com.twitter.aurora.scheduler.events.PubsubEvent.TasksDeleted;
import com.twitter.aurora.scheduler.state.StateManager.InstanceException;
import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.aurora.scheduler.storage.entities.IAssignedTask;
import com.twitter.aurora.scheduler.storage.entities.IJobKey;
import com.twitter.aurora.scheduler.storage.entities.IScheduledTask;
import com.twitter.aurora.scheduler.storage.entities.ITaskConfig;
import com.twitter.aurora.scheduler.storage.mem.MemStorage;
import com.twitter.common.base.Closure;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static com.twitter.aurora.gen.Constants.DEFAULT_ENVIRONMENT;
import static com.twitter.aurora.gen.ScheduleStatus.ASSIGNED;
import static com.twitter.aurora.gen.ScheduleStatus.FINISHED;
import static com.twitter.aurora.gen.ScheduleStatus.INIT;
import static com.twitter.aurora.gen.ScheduleStatus.KILLED;
import static com.twitter.aurora.gen.ScheduleStatus.KILLING;
import static com.twitter.aurora.gen.ScheduleStatus.PENDING;
import static com.twitter.aurora.gen.ScheduleStatus.ROLLBACK;
import static com.twitter.aurora.gen.ScheduleStatus.RUNNING;
import static com.twitter.aurora.gen.ScheduleStatus.STARTING;
import static com.twitter.aurora.gen.ScheduleStatus.UNKNOWN;
import static com.twitter.aurora.gen.ScheduleStatus.UPDATING;
import static com.twitter.aurora.scheduler.state.StateManagerImpl.UpdateException;

public class StateManagerImplTest extends EasyMockTest {

  private static final String HOST_A = "host_a";
  private static final Identity JIM = new Identity("jim", "jim-user");
  private static final String MY_JOB = "myJob";
  private static final IJobKey JOB_KEY = JobKeys.from(JIM.getRole(), DEFAULT_ENVIRONMENT, MY_JOB);

  private Driver driver;
  private TaskIdGenerator taskIdGenerator;
  private Closure<PubsubEvent> eventSink;
  private StateManagerImpl stateManager;
  private final FakeClock clock = new FakeClock();
  private Storage storage;

  @Before
  public void setUp() throws Exception {
    taskIdGenerator = createMock(TaskIdGenerator.class);
    driver = createMock(Driver.class);
    eventSink = createMock(new Clazz<Closure<PubsubEvent>>() { });
    // TODO(William Farner): Use a mocked storage.
    storage = MemStorage.newEmptyStorage();
    stateManager = new StateManagerImpl(storage, clock, driver, taskIdGenerator, eventSink);
  }

  @After
  public void validateCompletion() {
    assertTrue(stateManager.getStorage().getEvents().isEmpty());
  }

  private static class StateChangeMatcher implements IArgumentMatcher {
    private final String taskId;
    private final ScheduleStatus from;
    private final ScheduleStatus to;

    StateChangeMatcher(String taskId, ScheduleStatus from, ScheduleStatus to) {
      this.taskId = taskId;
      this.from = from;
      this.to = to;
    }

    @Override
    public boolean matches(Object argument) {
      if (!(argument instanceof TaskStateChange)) {
        return false;
      }

      TaskStateChange change = (TaskStateChange) argument;
      return taskId.equals(Tasks.id(change.getTask()))
          && (from == change.getOldState())
          && (to == change.getNewState());
    }

    @Override
    public void appendTo(StringBuffer buffer) {
      buffer.append(taskId).append(" ").append(from).append("->").append(to);
    }
  }

  PubsubEvent matchStateChange(String task, ScheduleStatus from, ScheduleStatus to) {
    EasyMock.reportMatcher(new StateChangeMatcher(task, from, to));
    return null;
  }

  private static class DeletedTasksMatcher implements IArgumentMatcher {
    private final Set<String> taskIds;

    DeletedTasksMatcher(String taskId, String... taskIds) {
      this.taskIds = ImmutableSet.<String>builder().add(taskId).add(taskIds).build();
    }

    @Override
    public boolean matches(Object argument) {
      if (!(argument instanceof TasksDeleted)) {
        return false;
      }

      TasksDeleted deleted = (TasksDeleted) argument;
      return taskIds.equals(Tasks.ids(deleted.getTasks()));
    }

    @Override
    public void appendTo(StringBuffer buffer) {
      buffer.append(taskIds);
    }
  }

  PubsubEvent matchTasksDeleted(String id, String... ids) {
    EasyMock.reportMatcher(new DeletedTasksMatcher(id, ids));
    return null;
  }

  @Test
  public void testAddTasks() {
    ITaskConfig task = makeTask(JIM, MY_JOB);
    String taskId = "a";
    expect(taskIdGenerator.generate(task, 3)).andReturn(taskId);
    expectStateTransitions(taskId, INIT, PENDING);

    control.replay();

    insertTask(task, 3);
    ScheduledTask expected = new ScheduledTask()
        .setStatus(PENDING)
        .setTaskEvents(ImmutableList.of(new TaskEvent()
            .setTimestamp(clock.nowMillis())
            .setScheduler(TaskStateMachine.LOCAL_HOST_SUPPLIER.get())
            .setStatus(PENDING)))
        .setAssignedTask(new AssignedTask()
            .setInstanceId(3)
            .setTaskId(taskId)
            .setTask(task.newBuilder()));
    assertEquals(ImmutableSet.of(IScheduledTask.build(expected)),
        Storage.Util.consistentFetchTasks(storage, Query.taskScoped(taskId)));
  }

  @Test
  public void testKillPendingTask() {
    ITaskConfig task = makeTask(JIM, MY_JOB);
    String taskId = "a";
    expect(taskIdGenerator.generate(task, 0)).andReturn(taskId);
    expectStateTransitions(taskId, INIT, PENDING);
    eventSink.execute(matchTasksDeleted(taskId));

    control.replay();

    insertTask(task, 0);
    assertEquals(1, changeState(taskId, KILLING));
    assertEquals(0, changeState(taskId, KILLING));
  }

  @Test
  public void testLostKillingTask() {
    ITaskConfig task = makeTask(JIM, MY_JOB);
    String taskId = "a";
    expect(taskIdGenerator.generate(task, 0)).andReturn(taskId);
    expectStateTransitions(taskId, INIT, PENDING, ASSIGNED, RUNNING, KILLING);
    eventSink.execute(matchTasksDeleted(taskId));

    driver.killTask(EasyMock.<String>anyObject());

    control.replay();

    insertTask(task, 0);

    assignTask(taskId, HOST_A);
    changeState(taskId, RUNNING);
    changeState(taskId, KILLING);
    changeState(taskId, UNKNOWN);
  }

  @Test
  public void testUpdate() throws Exception {
    ITaskConfig taskInfo = makeTask(JIM, MY_JOB);
    String taskId = "a";
    expect(taskIdGenerator.generate(taskInfo, 0)).andReturn(taskId);
    expectStateTransitions(taskId, INIT, PENDING);

    control.replay();

    insertTask(taskInfo, 0);

    try {
      stateManager.finishUpdate(
          JOB_KEY, JIM.getUser(), Optional.<String>absent(), UpdateResult.SUCCESS, true);
      fail();
    } catch (UpdateException e) {
      // expected
    }

    String token = stateManager.registerUpdate(JOB_KEY, ImmutableMap.of(0, taskInfo));
    assertTrue(stateManager.finishUpdate(
        JOB_KEY, JIM.getUser(), Optional.of(token), UpdateResult.SUCCESS, true));
    assertFalse(stateManager.finishUpdate(
        JOB_KEY, JIM.getUser(), Optional.of(token), UpdateResult.SUCCESS, false));
  }

  @Test
  public void testUpdatingPreventCancel() throws Exception {
    // Tests that any tasks in UPDATING or ROLLBACK prevents an update from being finalized.
    // Otherwise, the schedule would lose the persisted update configuration, and fail
    // to reschedule tasks.

    ITaskConfig taskInfo = makeTask(JIM, MY_JOB);
    String id = "a";
    expect(taskIdGenerator.generate(taskInfo, 0)).andReturn("a");
    expectStateTransitions(id, INIT, PENDING, ASSIGNED, UPDATING, FINISHED);

    ITaskConfig updated = ITaskConfig.build(taskInfo.newBuilder().setNumCpus(1000));
    String updatedId = "a-updated";
    expect(taskIdGenerator.generate(updated, 0)).andReturn(updatedId);
    expectStateTransitions(updatedId, INIT, PENDING, ASSIGNED, ROLLBACK, FINISHED);

    String rollbackId = "a-rollback";
    expect(taskIdGenerator.generate(taskInfo, 0)).andReturn(rollbackId);
    expectStateTransitions(rollbackId, INIT, PENDING);

    driver.killTask(EasyMock.<String>anyObject());
    expectLastCall().times(2);

    control.replay();

    insertTask(taskInfo, 0);
    changeState(id, ASSIGNED);

    String token = stateManager.registerUpdate(JOB_KEY, ImmutableMap.of(0, updated));
    stateManager.modifyShards(JOB_KEY, JIM.getUser(), ImmutableSet.of(0), token, true);

    // Since the task is still in UPDATING, it should not be possible to cancel the update.
    try {
      stateManager.finishUpdate(
          JOB_KEY, JIM.getUser(), Optional.<String>absent(), UpdateResult.SUCCESS, true);
      fail("cancel_update should have been prevented");
    } catch (UpdateException e) {
      // expected
    }

    changeState(id, FINISHED);
    changeState(updatedId, ASSIGNED);
    stateManager.modifyShards(JOB_KEY, JIM.getUser(), ImmutableSet.of(0), token, false);
    // Since the task is still in ROLLBACK, it should not be possible to cancel the update.
    try {
      stateManager.finishUpdate(
          JOB_KEY, JIM.getUser(), Optional.<String>absent(), UpdateResult.SUCCESS, true);
      fail("cancel_update should have been prevented");
    } catch (UpdateException e) {
      // expected
    }

    changeState(updatedId, FINISHED);

    stateManager.finishUpdate(
        JOB_KEY, JIM.getUser(), Optional.<String>absent(), UpdateResult.SUCCESS, true);
  }

  @Test
  public void testUpdatingToRollback() throws Exception {
    ITaskConfig taskInfo = makeTask(JIM, MY_JOB);
    String id = "a";
    expect(taskIdGenerator.generate(taskInfo, 0)).andReturn(id);
    expectStateTransitions(id, INIT, PENDING, ASSIGNED, STARTING, RUNNING, UPDATING, ROLLBACK);
    ITaskConfig updated = ITaskConfig.build(taskInfo.newBuilder().setNumCpus(1000));
    driver.killTask(EasyMock.<String>anyObject());
    expectLastCall().times(2);
    control.replay();

    insertTask(taskInfo, 0);
    changeState(id, ASSIGNED);
    changeState(id, STARTING);
    changeState(id, RUNNING);

    String token = stateManager.registerUpdate(JOB_KEY, ImmutableMap.of(0, updated));
    Map<Integer, ShardUpdateResult> result =
        stateManager.modifyShards(JOB_KEY, JIM.getUser(), ImmutableSet.of(0), token, true);
    assertEquals(result, ImmutableMap.of(0, ShardUpdateResult.RESTARTING));

    result = stateManager.modifyShards(JOB_KEY, JIM.getUser(), ImmutableSet.of(0), token, false);
    assertEquals(result, ImmutableMap.of(0, ShardUpdateResult.RESTARTING));
  }

  @Test
  public void testRollbackToUpdating() throws Exception {
    ITaskConfig taskInfo = makeTask(JIM, MY_JOB);
    String id = "a";
    expect(taskIdGenerator.generate(taskInfo, 0)).andReturn(id);
    expectStateTransitions(id, INIT, PENDING, ASSIGNED, STARTING, RUNNING, UPDATING, KILLED);

    ITaskConfig newConfig = ITaskConfig.build(taskInfo.newBuilder().setNumCpus(1000));
    String newTaskId = "a-rescheduled";
    expect(taskIdGenerator.generate(newConfig, 0)).andReturn(newTaskId);
    expectStateTransitions(
        newTaskId, INIT, PENDING, ASSIGNED, STARTING, RUNNING, ROLLBACK, UPDATING);

    driver.killTask(EasyMock.<String>anyObject());
    expectLastCall().times(3);
    control.replay();

    insertTask(taskInfo, 0);
    changeState(id, ASSIGNED);
    changeState(id, STARTING);
    changeState(id, RUNNING);
    String token = stateManager.registerUpdate(JOB_KEY, ImmutableMap.of(0, newConfig));
    changeState(id, UPDATING);
    changeState(id, KILLED);

    assignTask(newTaskId, HOST_A);
    changeState(newTaskId, STARTING);
    changeState(newTaskId, RUNNING);
    changeState(newTaskId, ROLLBACK);

    Map<Integer, ShardUpdateResult> result =
        stateManager.modifyShards(JOB_KEY, JIM.getUser(), ImmutableSet.of(0), token, true);
    assertEquals(result, ImmutableMap.of(0, ShardUpdateResult.RESTARTING));
  }

  @Test
  public void testRollback() throws Exception {
    ITaskConfig taskInfo = makeTaskWithPorts(JIM, MY_JOB, "foo");
    String taskId = "a";
    expect(taskIdGenerator.generate(taskInfo, 0)).andReturn(taskId);
    expectStateTransitions(taskId, INIT, PENDING, ASSIGNED, STARTING, RUNNING, UPDATING, FINISHED);

    String newTaskId = "a-updated";
    expect(taskIdGenerator.generate(taskInfo, 0)).andReturn(newTaskId);
    expectStateTransitions(newTaskId, INIT, PENDING, ASSIGNED, STARTING, ROLLBACK, FINISHED);

    String rolledBackId = "a-rollback";
    expect(taskIdGenerator.generate(taskInfo, 0)).andReturn(rolledBackId);
    expectStateTransitions(rolledBackId, INIT, PENDING, ASSIGNED);

    driver.killTask(EasyMock.<String>anyObject());
    expectLastCall().times(2);

    control.replay();

    insertTask(taskInfo, 0);
    stateManager.assignTask(taskId, HOST_A, SlaveID.newBuilder().setValue(HOST_A).build(),
        ImmutableSet.<Integer>of(50));
    IScheduledTask task = Iterables.getOnlyElement(
        Storage.Util.consistentFetchTasks(storage, Query.roleScoped(JIM.getRole())));
    assertEquals(ImmutableMap.of("foo", 50), task.getAssignedTask().getAssignedPorts());
    assignTask(taskId, HOST_A);
    changeState(taskId, STARTING);
    changeState(taskId, RUNNING);

    stateManager.registerUpdate(JOB_KEY, ImmutableMap.of(0, taskInfo));
    changeState(taskId, UPDATING);
    changeState(taskId, FINISHED);

    IAssignedTask updated = stateManager.assignTask(newTaskId, HOST_A,
        SlaveID.newBuilder().setValue(HOST_A).build(),
        ImmutableSet.<Integer>of(51));

    assertEquals(ImmutableMap.of("foo", 51), updated.getAssignedPorts());
    changeState(newTaskId, STARTING);
    changeState(newTaskId, ROLLBACK);
    changeState(newTaskId, FINISHED);

    IAssignedTask rolledBack = stateManager.assignTask(rolledBackId, HOST_A,
        SlaveID.newBuilder().setValue(HOST_A).build(),
        ImmutableSet.<Integer>of(52));
    assertEquals(ImmutableMap.of("foo", 52), rolledBack.getAssignedPorts());
  }

  @Test
  public void testNestedEvents() {
    String id = "a";
    ITaskConfig task = makeTask(JIM, MY_JOB);
    expect(taskIdGenerator.generate(task, 0)).andReturn(id);

    // Trigger an event that produces a side-effect and a PubSub event .
    eventSink.execute(matchStateChange(id, INIT, PENDING));
    expectLastCall().andAnswer(new IAnswer<Void>() {
      @Override public Void answer() throws Throwable {
        stateManager.changeState(
            Query.unscoped(), ScheduleStatus.ASSIGNED, Optional.<String>absent());
        return null;
      }
    });

    // Final event sink execution that adds no side effect or event.
    expectStateTransitions(id, PENDING, ASSIGNED);

    control.replay();

    insertTask(task, 0);
  }

  @Test
  public void testDeleteTask() {
    ITaskConfig task = makeTask(JIM, MY_JOB);
    String taskId = "a";
    expect(taskIdGenerator.generate(task, 0)).andReturn(taskId);
    expectStateTransitions(taskId, INIT, PENDING);
    eventSink.execute(matchTasksDeleted(taskId));

    control.replay();

    insertTask(task, 0);
    stateManager.deleteTasks(ImmutableSet.of(taskId));
  }

  @Test
  public void testAddInstances() throws Exception {
    ITaskConfig existingTask = makeTask(JIM, MY_JOB);
    String existingId = "a";
    ITaskConfig newTask = makeTask(JIM, MY_JOB);
    String newId = "b";
    expect(taskIdGenerator.generate(existingTask, 0)).andReturn(existingId);
    expectStateTransitions(existingId, INIT, PENDING);
    expect(taskIdGenerator.generate(newTask, 1)).andReturn(newId);
    expectStateTransitions(newId, INIT, PENDING);

    control.replay();

    insertTask(existingTask, 0);
    stateManager.addInstances(JOB_KEY, ImmutableSet.of(1), newTask);
  }

  @Test(expected = InstanceException.class)
  public void testAddInstancesIdCollision() throws Exception {
    ITaskConfig taskInfo = makeTask(JIM, MY_JOB);
    String taskId = "a";
    expect(taskIdGenerator.generate(taskInfo, 0)).andReturn(taskId);
    expectStateTransitions(taskId, INIT, PENDING, ASSIGNED, RUNNING);

    control.replay();

    insertTask(taskInfo, 0);
    assignTask(taskId, HOST_A);
    changeState(taskId, RUNNING);
    stateManager.addInstances(JOB_KEY, ImmutableSet.of(0), taskInfo);
  }

  @Test(expected = InstanceException.class)
  public void testAddInstancesJobDoesNotExist() throws Exception {
    ITaskConfig taskInfo = makeTask(JIM, MY_JOB);

    control.replay();

    stateManager.addInstances(JOB_KEY, ImmutableSet.of(0), taskInfo);
  }

  private void expectStateTransitions(
      String taskId,
      ScheduleStatus initial,
      ScheduleStatus next,
      ScheduleStatus... others) {

    List<ScheduleStatus> statuses = ImmutableList.<ScheduleStatus>builder()
        .add(initial)
        .add(next)
        .add(others)
        .build();
    PeekingIterator<ScheduleStatus> it = Iterators.peekingIterator(statuses.iterator());
    while (it.hasNext()) {
      ScheduleStatus cur = it.next();
      try {
        eventSink.execute(matchStateChange(taskId, cur, it.peek()));
      } catch (NoSuchElementException e) {
        // Expected.
      }
    }
  }

  private void insertTask(ITaskConfig task, int instanceId) {
    stateManager.insertPendingTasks(ImmutableMap.of(instanceId, task));
  }

  private int changeState(String taskId, ScheduleStatus status) {
    return stateManager.changeState(Query.taskScoped(taskId), status, Optional.<String>absent());
  }

  private static ITaskConfig makeTask(Identity owner, String job) {
    return ITaskConfig.build(new TaskConfig()
        .setOwner(owner)
        .setEnvironment(DEFAULT_ENVIRONMENT)
        .setJobName(job)
        .setRequestedPorts(ImmutableSet.<String>of()));
  }

  private static ITaskConfig makeTaskWithPorts(
      Identity owner,
      String job,
      String... requestedPorts) {

    return ITaskConfig.build(makeTask(owner, job).newBuilder()
        .setRequestedPorts(ImmutableSet.<String>builder().add(requestedPorts).build()));
  }

  private void assignTask(String taskId, String host) {
    stateManager.assignTask(taskId, host, SlaveID.newBuilder().setValue(host).build(),
        ImmutableSet.<Integer>of());
  }
}

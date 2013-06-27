package com.twitter.mesos.scheduler;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import com.google.common.base.Function;
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

import com.twitter.common.base.Closure;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.ShardUpdateResult;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.UpdateResult;
import com.twitter.mesos.scheduler.base.Query;
import com.twitter.mesos.scheduler.events.PubsubEvent;
import com.twitter.mesos.scheduler.events.PubsubEvent.TaskStateChange;
import com.twitter.mesos.scheduler.events.PubsubEvent.TasksDeleted;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.mem.MemStorage;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static com.twitter.mesos.gen.Constants.DEFAULT_ENVIRONMENT;
import static com.twitter.mesos.gen.ScheduleStatus.ASSIGNED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.INIT;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLING;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.ROLLBACK;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static com.twitter.mesos.gen.ScheduleStatus.STARTING;
import static com.twitter.mesos.gen.ScheduleStatus.UNKNOWN;
import static com.twitter.mesos.gen.ScheduleStatus.UPDATING;
import static com.twitter.mesos.scheduler.StateManagerImpl.UpdateException;

public class StateManagerImplTest extends EasyMockTest {

  private static final String HOST_A = "host_a";
  private static final Identity JIM = new Identity("jim", "jim-user");
  private static final String MY_JOB = "myJob";

  private Driver driver;
  private Function<TwitterTaskInfo, String> taskIdGenerator;
  private Closure<PubsubEvent> eventSink;
  private StateManagerImpl stateManager;
  private final FakeClock clock = new FakeClock();
  private Storage storage;

  @Before
  public void setUp() throws Exception {
    taskIdGenerator = createMock(new Clazz<Function<TwitterTaskInfo, String>>() { });
    driver = createMock(Driver.class);
    eventSink = createMock(new Clazz<Closure<PubsubEvent>>() { });
    // TODO(William Farner): Use a mocked storage.
    storage = MemStorage.newEmptyStorage();
    stateManager = new StateManagerImpl(storage, clock, driver, taskIdGenerator, eventSink);
  }

  @After
  public void validateCompletion() {
    assertTrue(stateManager.storage.events.isEmpty());
  }

  private static class StateChangeMatcher implements IArgumentMatcher {
    final String taskId;
    final ScheduleStatus from;
    final ScheduleStatus to;

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
    final Set<String> taskIds;

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
  public void testKillPendingTask() {
    TwitterTaskInfo task = makeTask(JIM, MY_JOB, 0);
    String taskId = "a";
    expect(taskIdGenerator.apply(task)).andReturn(taskId);
    expectStateTransitions(taskId, INIT, PENDING);
    eventSink.execute(matchTasksDeleted(taskId));

    control.replay();

    insertTasks(task);
    assertEquals(1, changeState(taskId, KILLING));
    assertEquals(0, changeState(taskId, KILLING));
  }

  @Test
  public void testLostKillingTask() {
    TwitterTaskInfo task = makeTask(JIM, MY_JOB, 0);
    String taskId = "a";
    expect(taskIdGenerator.apply(task)).andReturn(taskId);
    expectStateTransitions(taskId, INIT, PENDING, ASSIGNED, RUNNING, KILLING);
    eventSink.execute(matchTasksDeleted(taskId));

    driver.killTask(EasyMock.<String>anyObject());

    control.replay();

    insertTasks(task);

    assignTask(taskId, HOST_A);
    changeState(taskId, RUNNING);
    changeState(taskId, KILLING);
    changeState(taskId, UNKNOWN);
  }

  @Test
  public void testUpdate() throws Exception {
    TwitterTaskInfo taskInfo = makeTask(JIM, MY_JOB, 0);
    String taskId = "a";
    expect(taskIdGenerator.apply(taskInfo)).andReturn(taskId);
    expectStateTransitions(taskId, INIT, PENDING);

    control.replay();

    insertTasks(taskInfo);

    try {
      stateManager.finishUpdate(
          JIM, MY_JOB, Optional.<String>absent(), UpdateResult.SUCCESS, true);
      fail();
    } catch (UpdateException e) {
      // expected
    }

    String token = stateManager.registerUpdate(JIM.getRole(), MY_JOB, ImmutableSet.of(taskInfo));
    assertTrue(stateManager.finishUpdate(
        JIM, MY_JOB, Optional.of(token), UpdateResult.SUCCESS, true));
    assertFalse(stateManager.finishUpdate(
        JIM, MY_JOB, Optional.of(token), UpdateResult.SUCCESS, false));
  }

  @Test
  public void testUpdatingPreventCancel() throws Exception {
    // Tests that any tasks in UPDATING or ROLLBACK prevents an update from being finalized.
    // Otherwise, the schedule would lose the persisted update configuration, and fail
    // to reschedule tasks.

    TwitterTaskInfo taskInfo = makeTask(JIM, MY_JOB, 0);
    String id = "a";
    expect(taskIdGenerator.apply(taskInfo)).andReturn("a");
    expectStateTransitions(id, INIT, PENDING, ASSIGNED, UPDATING, FINISHED);

    TwitterTaskInfo updated = taskInfo.deepCopy().setNumCpus(1000);
    String updatedId = "a-updated";
    expect(taskIdGenerator.apply(updated)).andReturn(updatedId);
    expectStateTransitions(updatedId, INIT, PENDING, ASSIGNED, ROLLBACK, FINISHED);

    String rollbackId = "a-rollback";
    expect(taskIdGenerator.apply(taskInfo)).andReturn(rollbackId);
    expectStateTransitions(rollbackId, INIT, PENDING);

    driver.killTask(EasyMock.<String>anyObject());
    expectLastCall().times(2);

    control.replay();

    insertTasks(taskInfo);
    changeState(id, ASSIGNED);

    String token = stateManager.registerUpdate(JIM.getRole(), MY_JOB, ImmutableSet.of(updated));
    stateManager.modifyShards(JIM, MY_JOB, ImmutableSet.of(0), token, true);

    // Since the task is still in UPDATING, it should not be possible to cancel the update.
    try {
      stateManager.finishUpdate(
          JIM, MY_JOB, Optional.<String>absent(), UpdateResult.SUCCESS, true);
      fail("cancel_update should have been prevented");
    } catch (UpdateException e) {
      // expected
    }

    changeState(id, FINISHED);
    changeState(updatedId, ASSIGNED);
    stateManager.modifyShards(JIM, MY_JOB, ImmutableSet.of(0), token, false);
    // Since the task is still in ROLLBACK, it should not be possible to cancel the update.
    try {
      stateManager.finishUpdate(
          JIM, MY_JOB, Optional.<String>absent(), UpdateResult.SUCCESS, true);
      fail("cancel_update should have been prevented");
    } catch (UpdateException e) {
      // expected
    }

    changeState(updatedId, FINISHED);

    stateManager.finishUpdate(
        JIM, MY_JOB, Optional.<String>absent(), UpdateResult.SUCCESS, true);
  }

  @Test
  public void testUpdatingToRollback() throws Exception {
    TwitterTaskInfo taskInfo = makeTask(JIM, MY_JOB, 0);
    String id = "a";
    expect(taskIdGenerator.apply(taskInfo)).andReturn(id);
    expectStateTransitions(id, INIT, PENDING, ASSIGNED, STARTING, RUNNING, UPDATING, ROLLBACK);
    TwitterTaskInfo updated = taskInfo.deepCopy().setNumCpus(1000);
    driver.killTask(EasyMock.<String>anyObject());
    expectLastCall().times(2);
    control.replay();

    insertTasks(taskInfo);
    changeState(id, ASSIGNED);
    changeState(id, STARTING);
    changeState(id, RUNNING);

    String token = stateManager.registerUpdate(JIM.getRole(), MY_JOB, ImmutableSet.of(updated));
    Map<Integer, ShardUpdateResult> result =
        stateManager.modifyShards(JIM, MY_JOB, ImmutableSet.of(0), token, true);
    assertEquals(result, ImmutableMap.of(0, ShardUpdateResult.RESTARTING));

    result = stateManager.modifyShards(JIM, MY_JOB, ImmutableSet.of(0), token, false);
    assertEquals(result, ImmutableMap.of(0, ShardUpdateResult.RESTARTING));
  }

  @Test
  public void testRollbackToUpdating() throws Exception {
    TwitterTaskInfo taskInfo = makeTask(JIM, MY_JOB, 0);
    String id = "a";
    expect(taskIdGenerator.apply(taskInfo)).andReturn(id);
    expectStateTransitions(id, INIT, PENDING, ASSIGNED, STARTING, RUNNING, UPDATING, KILLED);

    TwitterTaskInfo newConfig = taskInfo.deepCopy().setNumCpus(1000);
    String newTaskId = "a-rescheduled";
    expect(taskIdGenerator.apply(newConfig)).andReturn(newTaskId);
    expectStateTransitions(
        newTaskId, INIT, PENDING, ASSIGNED, STARTING, RUNNING, ROLLBACK, UPDATING);

    driver.killTask(EasyMock.<String>anyObject());
    expectLastCall().times(3);
    control.replay();

    insertTasks(taskInfo);
    changeState(id, ASSIGNED);
    changeState(id, STARTING);
    changeState(id, RUNNING);
    String token = stateManager.registerUpdate(JIM.getRole(), MY_JOB, ImmutableSet.of(newConfig));
    changeState(id, UPDATING);
    changeState(id, KILLED);

    assignTask(newTaskId, HOST_A);
    changeState(newTaskId, STARTING);
    changeState(newTaskId, RUNNING);
    changeState(newTaskId, ROLLBACK);

    Map<Integer, ShardUpdateResult> result =
        stateManager.modifyShards(JIM, MY_JOB, ImmutableSet.of(0), token, true);
    assertEquals(result, ImmutableMap.of(0, ShardUpdateResult.RESTARTING));
  }

  @Test
  public void testRollback() throws Exception {
    TwitterTaskInfo taskInfo = makeTaskWithPorts(JIM, MY_JOB, 0, "foo");
    String taskId = "a";
    expect(taskIdGenerator.apply(taskInfo)).andReturn(taskId);
    expectStateTransitions(taskId, INIT, PENDING, ASSIGNED, STARTING, RUNNING, UPDATING, FINISHED);

    String newTaskId = "a-updated";
    expect(taskIdGenerator.apply(taskInfo)).andReturn(newTaskId);
    expectStateTransitions(newTaskId, INIT, PENDING, ASSIGNED, STARTING, ROLLBACK, FINISHED);

    String rolledBackId = "a-rollback";
    expect(taskIdGenerator.apply(taskInfo)).andReturn(rolledBackId);
    expectStateTransitions(rolledBackId, INIT, PENDING, ASSIGNED);

    driver.killTask(EasyMock.<String>anyObject());
    expectLastCall().times(2);

    control.replay();

    insertTasks(taskInfo);
    stateManager.assignTask(taskId, HOST_A, SlaveID.newBuilder().setValue(HOST_A).build(),
        ImmutableSet.<Integer>of(50));
    ScheduledTask task =
        Iterables.getOnlyElement(Storage.Util.consistentFetchTasks(storage, Query
            .byRole(JIM.getRole())));
    assertEquals(ImmutableMap.of("foo", 50), task.getAssignedTask().getAssignedPorts());
    assignTask(taskId, HOST_A);
    changeState(taskId, STARTING);
    changeState(taskId, RUNNING);

    stateManager.registerUpdate(JIM.getRole(), MY_JOB, ImmutableSet.of(taskInfo));
    changeState(taskId, UPDATING);
    changeState(taskId, FINISHED);

    AssignedTask updated = stateManager.assignTask(newTaskId, HOST_A,
        SlaveID.newBuilder().setValue(HOST_A).build(),
        ImmutableSet.<Integer>of(51));

    assertEquals(ImmutableMap.of("foo", 51), updated.getAssignedPorts());
    changeState(newTaskId, STARTING);
    changeState(newTaskId, ROLLBACK);
    changeState(newTaskId, FINISHED);

    AssignedTask rolledBack = stateManager.assignTask(rolledBackId, HOST_A,
        SlaveID.newBuilder().setValue(HOST_A).build(),
        ImmutableSet.<Integer>of(52));
    assertEquals(ImmutableMap.of("foo", 52), rolledBack.getAssignedPorts());
  }

  @Test
  public void testNestedEvents() {
    String id = "a";
    TwitterTaskInfo task = makeTask(JIM, MY_JOB, 0);
    expect(taskIdGenerator.apply(task)).andReturn(id);

    // Trigger an event that produces a side-effect and a PubSub event .
    eventSink.execute(matchStateChange(id, INIT, PENDING));
    expectLastCall().andAnswer(new IAnswer<Void>() {
      @Override public Void answer() throws Throwable {
        stateManager.changeState(Query.GET_ALL, ScheduleStatus.ASSIGNED);
        return null;
      }
    });

    // Final event sink execution that adds no side effect or event.
    expectStateTransitions(id, PENDING, ASSIGNED);

    control.replay();

    insertTasks(task);
  }

  @Test
  public void testDeleteTask() {
    TwitterTaskInfo task = makeTask(JIM, MY_JOB, 0);
    String taskId = "a";
    expect(taskIdGenerator.apply(task)).andReturn(taskId);
    expectStateTransitions(taskId, INIT, PENDING);
    eventSink.execute(matchTasksDeleted(taskId));

    control.replay();

    insertTasks(task);
    stateManager.deleteTasks(ImmutableSet.of(taskId));
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

  private Set<String> insertTasks(TwitterTaskInfo... tasks) {
    return stateManager.insertTasks(ImmutableSet.copyOf(tasks));
  }

  private int changeState(String taskId, ScheduleStatus status) {
    return stateManager.changeState(Query.byId(taskId), status);
  }

  private static TwitterTaskInfo makeTask(Identity owner, String job, int shard) {
    return new TwitterTaskInfo()
        .setOwner(owner)
        .setEnvironment(DEFAULT_ENVIRONMENT)
        .setJobName(job)
        .setShardId(shard)
        .setRequestedPorts(ImmutableSet.<String>of());
  }

  private static TwitterTaskInfo makeTaskWithPorts(
      Identity owner,
      String job,
      int shard,
      String... requestedPorts) {

    TwitterTaskInfo task = makeTask(owner, job, shard);
    task.setRequestedPorts(ImmutableSet.<String>builder().add(requestedPorts).build());
    return task;
  }

  private void assignTask(String taskId, String host) {
    stateManager.assignTask(taskId, host, SlaveID.newBuilder().setValue(host).build(),
        ImmutableSet.<Integer>of());
  }
}

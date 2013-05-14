package com.twitter.mesos.scheduler;

import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.mesos.Protos.SlaveID;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.base.Closure;
import com.twitter.common.stats.Stats;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.ShardUpdateResult;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.UpdateResult;
import com.twitter.mesos.scheduler.events.PubsubEvent;
import com.twitter.mesos.scheduler.events.PubsubEvent.TaskStateChange;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.mem.MemStorage;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static com.twitter.mesos.gen.ScheduleStatus.ASSIGNED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLING;
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
    // Flush residual stats from different tests.
    Stats.flush();

    taskIdGenerator = createMock(new Clazz<Function<TwitterTaskInfo, String>>() { });
    driver = createMock(Driver.class);
    eventSink = createMock(new Clazz<Closure<PubsubEvent>>() { });
    // TODO(William Farner): Use a mocked storage.
    storage = MemStorage.newEmptyStorage();
    stateManager = new StateManagerImpl(storage, clock, driver, taskIdGenerator, eventSink);
  }

  @After
  public void validateCompletion() {
    assertTrue(stateManager.txStorage.events.isEmpty());
  }

  private void expectPubSubEvent() {
    // TODO(William Farner): Expect specific pubsub events,
    // write a failing test case to capture currently missing DELETE event.
    eventSink.execute(EasyMock.isA(PubsubEvent.class));
    expectLastCall().anyTimes();
  }

  @Test
  public void testKillPendingTask() {
    TwitterTaskInfo task = makeTask(JIM, MY_JOB, 0);
    String taskId = "a";
    expect(taskIdGenerator.apply(task)).andReturn(taskId);
    expectPubSubEvent();

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
    expectPubSubEvent();

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
    expect(taskIdGenerator.apply(taskInfo)).andReturn("a");
    expectPubSubEvent();

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
    TwitterTaskInfo updated = taskInfo.deepCopy().setNumCpus(1000);
    String updatedId = "a-updated";
    expect(taskIdGenerator.apply(updated)).andReturn(updatedId);
    String rollbackId = "a-updated";
    expect(taskIdGenerator.apply(taskInfo)).andReturn(rollbackId);

    expectPubSubEvent();
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
    expectPubSubEvent();
    driver.killTask(EasyMock.<String>anyObject());
    expectLastCall().times(2);

    control.replay();
    TwitterTaskInfo taskInfo = makeTask(JIM, MY_JOB, 0);

    String id = insertTask(taskInfo);
    changeState(id, ASSIGNED);
    changeState(id, STARTING);
    changeState(id, RUNNING);

    TwitterTaskInfo updated = taskInfo.deepCopy().setNumCpus(1000);
    String token = stateManager.registerUpdate(JIM.getRole(), MY_JOB, ImmutableSet.of(updated));
    Map<Integer, ShardUpdateResult> result =
        stateManager.modifyShards(JIM, MY_JOB, ImmutableSet.of(0), token, true);
    assertEquals(result, ImmutableMap.of(0, ShardUpdateResult.RESTARTING));

    result = stateManager.modifyShards(JIM, MY_JOB, ImmutableSet.of(0), token, false);
    assertEquals(result, ImmutableMap.of(0, ShardUpdateResult.RESTARTING));
  }

  @Test
  public void testRollbackToUpdating() throws Exception {
    expectPubSubEvent();
    driver.killTask(EasyMock.<String>anyObject());
    expectLastCall().times(3);

    control.replay();
    TwitterTaskInfo taskInfo = makeTask(JIM, MY_JOB, 0);

    String id = insertTask(taskInfo);
    changeState(id, ASSIGNED);
    changeState(id, STARTING);
    changeState(id, RUNNING);
    TwitterTaskInfo newConfig = taskInfo.deepCopy().setNumCpus(1000);
    String token = stateManager.registerUpdate(JIM.getRole(), MY_JOB, ImmutableSet.of(newConfig));
    changeState(id, UPDATING);
    changeState(id, KILLED);

    String newTaskId =
        Tasks.id(Iterables.getOnlyElement(stateManager.fetchTasks(Query.byStatus(PENDING))));
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
    String newTaskId = "a-updated";
    expect(taskIdGenerator.apply(taskInfo)).andReturn(newTaskId);
    String rolledBackId = "a-rollback";
    expect(taskIdGenerator.apply(taskInfo)).andReturn(rolledBackId);

    expectPubSubEvent();

    driver.killTask(EasyMock.<String>anyObject());
    expectLastCall().times(2);

    control.replay();

    insertTasks(taskInfo);
    stateManager.assignTask(taskId, HOST_A, SlaveID.newBuilder().setValue(HOST_A).build(),
        ImmutableSet.<Integer>of(50));
    ScheduledTask task =
        Iterables.getOnlyElement(Storage.Util.fetchTasks(storage, Query.byRole(JIM.getRole())));
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
    TwitterTaskInfo task = makeTask(JIM, MY_JOB, 0);
    expect(taskIdGenerator.apply(task)).andReturn("a");

    // Trigger an event that produces a side-effect and a PubSub event .
    eventSink.execute(EasyMock.isA(TaskStateChange.class));
    expectLastCall().andAnswer(new IAnswer<Void>() {
      @Override public Void answer() throws Throwable {
        stateManager.changeState(Query.GET_ALL, ScheduleStatus.ASSIGNED);
        return null;
      }
    });

    // Final event sink execution that adds no side effect or event.
    eventSink.execute(EasyMock.isA(TaskStateChange.class));

    control.replay();

    insertTasks(task);
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

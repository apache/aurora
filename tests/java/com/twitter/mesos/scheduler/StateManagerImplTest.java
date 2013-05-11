package com.twitter.mesos.scheduler;

import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.mesos.Protos.SlaveID;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.twitter.common.base.Closure;
import com.twitter.common.stats.Stats;
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
import com.twitter.mesos.scheduler.events.PubsubEvent;
import com.twitter.mesos.scheduler.events.PubsubEvent.TaskStateChange;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.StorageException;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.mem.MemStorage;

import static org.easymock.EasyMock.expectLastCall;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static com.twitter.mesos.gen.ScheduleStatus.ASSIGNED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
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
  private Closure<PubsubEvent> eventSink;
  private StateManagerImpl stateManager;
  private final FakeClock clock = new FakeClock();
  private Storage storage;

  private int transactionsUntilFailure = 0;

  @Before
  public void setUp() throws Exception {
    resetStats();

    driver = createMock(Driver.class);
    eventSink = createMock(new Clazz<Closure<PubsubEvent>>() { });
    stateManager = createStateManager();
  }

  @After
  public void validateCompletion() {
    assertTrue(stateManager.txStorage.events.isEmpty());
  }

  /**
   * Flush residual stats from different tests, and stats exported by StateManager creation during
   * test setup methods.
   */
  private void resetStats() {
    Stats.flush();
  }

  private StateManagerImpl createStateManager(final Storage wrappedStorage) {
    resetStats();
    this.storage = new Storage() {
      @Override public <T, E extends Exception> T doInTransaction(final Work<T, E> work)
          throws StorageException, E {

        return doInWriteTransaction(new MutateWork<T, E>() {
          @Override public T apply(MutableStoreProvider storeProvider) throws E {
            return work.apply(storeProvider);
          }
        });
      }

      @Override public <T, E extends Exception> T doInWriteTransaction(final MutateWork<T, E> work)
          throws StorageException, E {

        return wrappedStorage.doInWriteTransaction(new MutateWork<T, E>() {
          @Override public T apply(MutableStoreProvider storeProvider) throws E {
            T result = work.apply(storeProvider);

            // Inject the failure after the work is performed in the transaction, so that we can
            // check for unintended side effects remaining.
            if ((transactionsUntilFailure != 0) && (--transactionsUntilFailure == 0)) {
              throw new StorageException("Injected storage failure.") { };
            }
            return result;
          }
        });
      }
    };

    return new StateManagerImpl(storage, clock, driver, eventSink);
  }

  private void expectPubSubEvent() {
    // TODO(William Farner): Rework StateManagerImpl to accept a task ID generator and allow for
    // easy verification of pubsub events.
    eventSink.execute(EasyMock.isA(PubsubEvent.class));
    expectLastCall().anyTimes();
  }

  @Test
  public void testKillPendingTask() {
    expectPubSubEvent();

    control.replay();

    String taskId = insertTask(makeTask(JIM, MY_JOB, 0));
    assertEquals(1, changeState(taskId, KILLING));
    assertEquals(0, changeState(taskId, KILLING));
  }

  @Test
  public void testLostKillingTask() {
    expectPubSubEvent();

    driver.killTask(EasyMock.<String>anyObject());

    control.replay();

    String taskId = insertTask(makeTask(JIM, MY_JOB, 0));

    assignTask(taskId, HOST_A);
    changeState(taskId, RUNNING);
    changeState(taskId, KILLING);
    changeState(taskId, UNKNOWN);
  }

  @Test
  public void testUpdate() throws Exception {
    expectPubSubEvent();

    control.replay();
    TwitterTaskInfo taskInfo = makeTask(JIM, MY_JOB, 0);

    insertTask(taskInfo);

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

    expectPubSubEvent();
    driver.killTask(EasyMock.<String>anyObject());
    expectLastCall().times(2);

    control.replay();
    TwitterTaskInfo taskInfo = makeTask(JIM, MY_JOB, 0);

    String id = insertTask(taskInfo);
    changeState(id, ASSIGNED);

    TwitterTaskInfo updated = taskInfo.deepCopy().setNumCpus(1000);
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
    String updatedId =
        Tasks.id(Iterables.getOnlyElement(stateManager.fetchTasks(Query.byStatus(PENDING))));
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
    expectPubSubEvent();

    driver.killTask(EasyMock.<String>anyObject());
    expectLastCall().times(2);

    control.replay();
    TwitterTaskInfo taskInfo = makeTaskWithPorts(JIM, MY_JOB, 0, "foo");

    String taskId = insertTask(taskInfo);
    stateManager.assignTask(taskId, HOST_A, SlaveID.newBuilder().setValue(HOST_A).build(),
        ImmutableSet.<Integer>of(50));
    ScheduledTask task =
        Iterables.getOnlyElement(stateManager.fetchTasks(Query.byRole(JIM.getRole())));
    assertEquals(ImmutableMap.of("foo", 50), task.getAssignedTask().getAssignedPorts());
    assignTask(taskId, HOST_A);
    changeState(taskId, STARTING);
    changeState(taskId, RUNNING);

    stateManager.registerUpdate(JIM.getRole(), MY_JOB, ImmutableSet.of(taskInfo));
    changeState(taskId, UPDATING);
    changeState(taskId, FINISHED);

    String newTaskId =
        Tasks.id(Iterables.getOnlyElement(stateManager.fetchTasks(Query.byStatus(PENDING))));
    AssignedTask updated = stateManager.assignTask(newTaskId, HOST_A,
        SlaveID.newBuilder().setValue(HOST_A).build(),
        ImmutableSet.<Integer>of(51));

    assertEquals(ImmutableMap.of("foo", 51), updated.getAssignedPorts());
    changeState(newTaskId, STARTING);
    changeState(newTaskId, ROLLBACK);
    changeState(newTaskId, FINISHED);

    String rolledBackId =
        Tasks.id(Iterables.getOnlyElement(stateManager.fetchTasks(Query.byStatus(PENDING))));
    AssignedTask rolledBack = stateManager.assignTask(rolledBackId, HOST_A,
        SlaveID.newBuilder().setValue(HOST_A).build(),
        ImmutableSet.<Integer>of(52));
    assertEquals(ImmutableMap.of("foo", 52), rolledBack.getAssignedPorts());
  }

  @Ignore("TODO(William Farner): Remove this test completely once DbStorage is removed.")
  @Test
  public void testTransactionalStateTransitions() throws Exception {
    expectPubSubEvent();

    control.replay();

    failNthTransaction(1);

    try {
      insertTask(makeTask(JIM, MY_JOB, 0));
      fail("Insert should have failed.");
    } catch (StorageException e) {
      // Expected.
    }

    Set<ScheduledTask> tasks = storage.doInTransaction(new Work.Quiet<Set<ScheduledTask>>() {
      @Override public Set<ScheduledTask> apply(StoreProvider storeProvider) {
        return storeProvider.getTaskStore().fetchTasks(Query.GET_ALL);
      }
    });
    assertTrue(tasks.isEmpty());
    assertTrue(stateManager.fetchTasks(Query.GET_ALL).isEmpty());
  }

  @Test
  public void testNestedEvents() {
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

    insertTask(makeTask(JIM, MY_JOB, 0));
  }

  private String insertTask(TwitterTaskInfo task) {
    return Iterables.getOnlyElement(insertTasks(task));
  }

  private Set<String> insertTasks(TwitterTaskInfo... tasks) {
    return stateManager.insertTasks(ImmutableSet.copyOf(tasks));
  }

  private int changeState(String taskId, ScheduleStatus status) {
    return stateManager.changeState(Query.byId(taskId), status);
  }

  private void failNthTransaction(int n) {
    Preconditions.checkState(transactionsUntilFailure == 0, "Last failure has not yet occurred");
    transactionsUntilFailure = n;
  }

  private StateManagerImpl createStateManager() throws Exception {
    return createStateManager(createStorage());
  }

  private Storage createStorage() throws Exception {
    return MemStorage.newEmptyStorage();
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

package com.twitter.mesos.scheduler;

import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.testing.TearDown;

import org.apache.mesos.Protos.SlaveID;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.twitter.common.base.Closure;
import com.twitter.common.stats.Stat;
import com.twitter.common.stats.Stats;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.UpdateResult;
import com.twitter.mesos.scheduler.StateManagerVars.MutableState;
import com.twitter.mesos.scheduler.events.PubsubEvent;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;
import com.twitter.mesos.scheduler.storage.Storage.StorageException;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.mem.MemStorage;

import static org.easymock.EasyMock.expectLastCall;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static com.twitter.mesos.gen.ScheduleStatus.ASSIGNED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.INIT;
import static com.twitter.mesos.gen.ScheduleStatus.KILLING;
import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.ROLLBACK;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static com.twitter.mesos.gen.ScheduleStatus.STARTING;
import static com.twitter.mesos.gen.ScheduleStatus.UNKNOWN;
import static com.twitter.mesos.gen.ScheduleStatus.UPDATING;
import static com.twitter.mesos.scheduler.StateManagerImpl.UpdateException;

public class StateManagerImplTest extends EasyMockTest {

  private static final String HOST_A = "host_a";

  private Driver driver;
  private Closure<PubsubEvent> eventSink;
  private StateManagerImpl stateManager;
  private MutableState mutableState;
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
    assertTrue(stateManager.txStorage.sideEffects.isEmpty());
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
      @Override public void prepare() {
        wrappedStorage.prepare();
      }

      @Override public void start(MutateWork.NoResult.Quiet initializationLogic) {
        wrappedStorage.start(initializationLogic);
      }

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

      @Override public void stop() {
        wrappedStorage.stop();
      }
    };

    this.mutableState = new MutableState();
    final StateManagerImpl manager =
        new StateManagerImpl(storage, clock, mutableState, driver, eventSink);
    manager.initialize();
    manager.start();
    addTearDown(new TearDown() {
      @Override public void tearDown() {
        manager.stop();
      }
    });
    return manager;
  }

  private void expectPubSubEvent() {
    // TODO(William Farner): Rework StateManagerImpl to accept a task ID generator and allow for
    // easy verification of pubsub events.
    eventSink.execute(EasyMock.isA(PubsubEvent.class));
    expectLastCall().anyTimes();
  }

  @Test
  public void testAbandonRunningTask() {
    expectPubSubEvent();

    control.replay();

    Set<String> taskIds = insertTasks(
        makeTask("jim", "myJob", 0),
        makeTask("jack", "otherJob", 0));
    assertVarCount("jim", "myJob", PENDING, 1);
    assertVarCount("jack", "otherJob", PENDING, 1);
    assertVarCount(PENDING, 2);

    String task1 = Iterables.get(taskIds, 0);
    assignTask(task1, HOST_A);
    assertVarCount("jim", "myJob", PENDING, 0);
    assertVarCount("jim", "myJob", ASSIGNED, 1);
    assertVarCount(PENDING, 1);
    assertVarCount(ASSIGNED, 1);
    changeState(task1, RUNNING);
    assertVarCount("jim", "myJob", ASSIGNED, 0);
    assertVarCount("jim", "myJob", RUNNING, 1);
    assertVarCount(ASSIGNED, 0);
    assertVarCount(RUNNING, 1);
    stateManager.abandonTasks(ImmutableSet.of(task1));
    assertVarCount("jim", "myJob", RUNNING, 0);
    assertVarCount("jim", "myJob", LOST, 0);
    assertVarCount("jim", "myJob", PENDING, 1);
    assertVarCount("jim", "myJob", UNKNOWN, 0);
    assertVarCount(RUNNING, 0);
    assertVarCount(LOST, 0);
    assertVarCount(PENDING, 2);
    assertVarCount(UNKNOWN, 0);
    assertTrue(stateManager.fetchTasks(Query.byId(task1)).isEmpty());
  }

  @Test
  public void testAbandonFinishedTask() {
    expectPubSubEvent();

    control.replay();

    Set<String> taskIds = insertTasks(
        makeTask("jim", "myJob", 0),
        makeTask("jack", "otherJob", 0));
    String task1 = Iterables.get(taskIds, 0);
    assignTask(task1, HOST_A);
    changeState(task1, RUNNING);
    changeState(task1, FINISHED);
    assertVarCount("jim", "myJob", FINISHED, 1);
    assertVarCount(FINISHED, 1);
    stateManager.abandonTasks(ImmutableSet.of(task1));
    assertVarCount("jim", "myJob", FINISHED, 0);
    assertVarCount(FINISHED, 0);
    assertTrue(stateManager.fetchTasks(Query.byId(task1)).isEmpty());
  }

  @Test
  public void testKillPendingTask() {
    expectPubSubEvent();

    control.replay();

    String taskId = insertTask(makeTask("jim", "myJob", 0));
    assertVarCount("jim", "myJob", PENDING, 1);
    assertVarCount(PENDING, 1);
    assertEquals(1, changeState(taskId, KILLING));
    assertVarCount("jim", "myJob", PENDING, 0);
    assertVarCount("jim", "myJob", KILLING, 0);
    assertVarCount("jim", "myJob", UNKNOWN, 0);
    assertVarCount(PENDING, 0);
    assertVarCount(KILLING, 0);
    assertVarCount(UNKNOWN, 0);
    assertEquals(0, changeState(taskId, KILLING));
  }

  @Test
  public void testLostKillingTask() {
    expectPubSubEvent();

    driver.killTask(EasyMock.<String>anyObject());

    control.replay();

    String taskId = insertTask(makeTask("jim", "myJob", 0));

    assignTask(taskId, HOST_A);
    changeState(taskId, RUNNING);
    changeState(taskId, KILLING);
    assertVarCount("jim", "myJob", KILLING, 1);
    assertVarCount(KILLING, 1);
    changeState(taskId, UNKNOWN);
    assertVarCount("jim", "myJob", KILLING, 0);
    assertVarCount("jim", "myJob", UNKNOWN, 0);
    assertVarCount(KILLING, 0);
    assertVarCount(UNKNOWN, 0);
  }

  @Test
  public void testInitNormallyHidden() throws Exception {
    expectPubSubEvent();

    control.replay();

    insertTask(makeTask("jim", "myJob", 0));
    assertVarCount("jim", "myJob", PENDING, 1);
    assertVarCount(INIT, 0);
    assertVarCount(PENDING, 1);
  }

  @Test
  public void testUpdate() throws Exception {
    expectPubSubEvent();

    control.replay();
    TwitterTaskInfo taskInfo = makeTask("jim", "myJob", 0);

    insertTask(taskInfo);

    try {
      stateManager.finishUpdate(
          "jim", "myJob", Optional.<String>absent(), UpdateResult.SUCCESS, true);
    } catch (UpdateException e) {
      // expected
    }

    String token = stateManager.registerUpdate("jim", "myJob", ImmutableSet.of(taskInfo));
    assertTrue(stateManager.finishUpdate(
        "jim", "myJob", Optional.of(token), UpdateResult.SUCCESS, true));
    assertFalse(stateManager.finishUpdate(
        "jim", "myJob", Optional.of(token), UpdateResult.SUCCESS, false));
  }

  @Test
  public void testRollback() throws Exception {
    expectPubSubEvent();

    driver.killTask(EasyMock.<String>anyObject());
    expectLastCall().times(2);

    control.replay();
    TwitterTaskInfo taskInfo = makeTaskWithPorts("jim", "myJob", 0, "foo");

    String taskId = insertTask(taskInfo);
    stateManager.assignTask(taskId, HOST_A, SlaveID.newBuilder().setValue(HOST_A).build(),
        ImmutableSet.<Integer>of(50));
    ScheduledTask task =
        Iterables.getOnlyElement(stateManager.fetchTasks(Query.byRole("jim")));
    assertEquals(ImmutableMap.of("foo", 50), task.getAssignedTask().getAssignedPorts());
    assignTask(taskId, HOST_A);
    changeState(taskId, STARTING);
    changeState(taskId, RUNNING);

    stateManager.registerUpdate("jim", "myJob", ImmutableSet.of(taskInfo));
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

  @Test
  public void testTracksInit() throws Exception {
    expectPubSubEvent();

    final TwitterTaskInfo task = makeTask("jim", "myJob", 0);

    // Insert a task in the INIT state, and restart the state manager.
    storage.doInWriteTransaction(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getTaskStore()
            .saveTasks(ImmutableSet.of(stateManager.getTaskCreator().apply(task)));
      }
    });
    stateManager = createStateManager(storage);

    control.replay();

    assertVarCount("jim", "myJob", INIT, 1);
    assertVarCount(INIT, 1);
  }

  @Test
  public void testTracksUnknown() throws Exception {
    expectPubSubEvent();

    final TwitterTaskInfo task = makeTask("jim", "myJob", 0);

    // Insert a task in the INIT state, and restart the state manager.
    storage.doInWriteTransaction(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        ScheduledTask scheduledTask = stateManager.getTaskCreator().apply(task);
        scheduledTask.setStatus(UNKNOWN);
        storeProvider.getTaskStore()
            .saveTasks(ImmutableSet.of(scheduledTask));
      }
    });
    stateManager = createStateManager(storage);

    control.replay();

    assertVarCount("jim", "myJob", UNKNOWN, 1);
    assertVarCount(UNKNOWN, 1);
  }

  @Ignore("TODO(William Farner): Remove this test completely once DbStorage is removed.")
  @Test
  public void testTransactionalStateTransitions() throws Exception {
    expectPubSubEvent();

    control.replay();

    failNthTransaction(1);

    try {
      insertTask(makeTask("jim", "myJob", 0));
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

    assertVarCount("jim", "myJob", INIT, 0);
    assertVarCount("jim", "myJob", PENDING, 0);
    assertVarCount(INIT, 0);
    assertVarCount(PENDING, 0);
  }

  @Test
  public void testDelayedStatExport() throws Exception {
    expectPubSubEvent();

    resetStats();

    control.replay();

    mutableState = new MutableState();
    stateManager = new StateManagerImpl(storage, clock, mutableState, driver, eventSink);

    // The database has not yet been loaded, so stats should be missing.
    for (ScheduleStatus status : ScheduleStatus.values()) {
      assertNull(Stats.getVariable(mutableState.getVars().getVarName(status)));
    }
    stateManager.initialize();

    // Now that we are initialized, all stats should be present.
    for (ScheduleStatus status : ScheduleStatus.values()) {
      assertVarCount(status, 0);
    }
  }

  @Test
  public void testNestedEvents() {
    // Trigger an event that triggers a side-effect and a PubSub event twice.
    eventSink.execute(EasyMock.<PubsubEvent>anyObject());
    expectLastCall().andAnswer(new IAnswer<Void>() {
      @Override public Void answer() throws Throwable {
        stateManager.deleteTasks(ImmutableSet.of("taskA"));
        return null;
      }
    }).times(2);

    // Final event sink execution that adds no side effect or event.
    eventSink.execute(EasyMock.isA(PubsubEvent.class));
    expectLastCall();

    control.replay();

    insertTask(makeTask("jim", "myJob", 0));
  }

  private String insertTask(TwitterTaskInfo task) {
    return Iterables.getOnlyElement(insertTasks(task));
  }

  private Set<String> insertTasks(TwitterTaskInfo... tasks) {
    return stateManager.insertTasks(ImmutableSet.copyOf(tasks));
  }

  private void assertVarCount(String owner, String job, ScheduleStatus status, long expected) {
    Stat<?> stat = getVar(mutableState.getVars().getVarName(Tasks.jobKey(owner, job), status));
    if ((expected != 0) || (stat != null)) {
      assertEquals(expected, stat.read());
    }
  }

  private void assertVarCount(ScheduleStatus status, long expected) {
    assertEquals(expected, getVar(mutableState.getVars().getVarName(status)).read());
  }

  private Stat<?> getVar(String name) {
    return Stats.getVariable(Stats.normalizeName(name));
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

  private static TwitterTaskInfo makeTask(String owner, String job, int shard) {
    return new TwitterTaskInfo()
        .setOwner(new Identity().setRole(owner).setUser(owner))
        .setJobName(job)
        .setShardId(shard)
        .setRequestedPorts(ImmutableSet.<String>of());
  }

  private static TwitterTaskInfo makeTaskWithPorts(
      String owner,
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

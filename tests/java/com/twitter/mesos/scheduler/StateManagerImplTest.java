package com.twitter.mesos.scheduler;

import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.testing.TearDown;

import org.apache.mesos.Protos.SlaveID;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.springframework.transaction.TransactionException;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stat;
import com.twitter.common.stats.Stats;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.UpdateResult;
import com.twitter.mesos.scheduler.StateManagerVars.MutableState;
import com.twitter.mesos.scheduler.db.testing.DbStorageTestUtil;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.StorageException;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.Storage.Work.NoResult.Quiet;

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
import static com.twitter.mesos.gen.ScheduleStatus.PREEMPTING;
import static com.twitter.mesos.gen.ScheduleStatus.RESTARTING;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static com.twitter.mesos.gen.ScheduleStatus.STARTING;
import static com.twitter.mesos.gen.ScheduleStatus.UNKNOWN;
import static com.twitter.mesos.scheduler.StateManagerImpl.UpdateException;

/**
 * @author William Farner
 */
public class StateManagerImplTest extends EasyMockTest {

  private static final String HOST_A = "host_a";

  private Driver driver;
  private StateManagerImpl stateManager;
  private MutableState mutableState;
  private FakeClock clock = new FakeClock();
  private Storage storage;

  private int transactionsUntilFailure = 0;

  @Before
  public void setUp() throws Exception {
    resetStats();

    driver = createMock(Driver.class);
    stateManager = createStateManager();
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

      @Override public void start(Quiet initilizationLogic) {
        wrappedStorage.start(initilizationLogic);
      }

      @Override public <T, E extends Exception> T doInTransaction(final Work<T, E> work)
          throws StorageException, E {
        return wrappedStorage.doInTransaction(new Work<T, E>() {
          @Override public T apply(StoreProvider storeProvider) throws E {
            T result = work.apply(storeProvider);

            // Inject the failure after the work is performed in the transaction, so that we can
            // check for unintended side effects remaining.
            if ((transactionsUntilFailure != 0) && (--transactionsUntilFailure == 0)) {
              throw new TransactionException("Injected storage failure.") { };
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
    final StateManagerImpl manager = new StateManagerImpl(storage, clock, mutableState, driver);
    manager.initialize();
    manager.start();
    addTearDown(new TearDown() {
      @Override public void tearDown() {
        manager.stop();
      }
    });
    return manager;
  }

  @Test
  public void testAbandonRunningTask() {
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
  public void testTimedoutTask() {
    Multimap<ScheduleStatus, ScheduleStatus> testCases =
        ImmutableMultimap.<ScheduleStatus, ScheduleStatus>builder()
            .putAll(ASSIGNED, PENDING)
            .putAll(STARTING, ASSIGNED)
            .putAll(PREEMPTING, ASSIGNED, RUNNING)
            .putAll(RESTARTING, ASSIGNED, RUNNING)
            .putAll(KILLING, ASSIGNED, RUNNING)
            .build();

    driver.killTask(EasyMock.<String>anyObject());
    // Three extra kills that are encountered while transition during test prep:
    // PREEMPTING, RESTARTING, KILLING.
    expectLastCall().times(testCases.keySet().size() + 3);

    control.replay();

    for (ScheduleStatus finalState : testCases.keySet()) {
      String taskId = insertTask(makeTask("jim", "lost_" + finalState, 0));

      for (ScheduleStatus prepState : testCases.get(finalState)) {
        changeState(taskId, prepState);
      }

      changeState(taskId, finalState);

      clock.advance(StateManagerImpl.MISSING_TASK_GRACE_PERIOD.get());
      clock.advance(Amount.of(1L, Time.MILLISECONDS));
      stateManager.scanOutstandingTasks();
    }
  }

  @Test
  public void testInitNormallyHidden() throws Exception {
    control.replay();

    insertTask(makeTask("jim", "myJob", 0));
    assertVarCount("jim", "myJob", PENDING, 1);
    assertVarCount(INIT, 0);
    assertVarCount(PENDING, 1);
  }

  @Test
  public void testUpdate() throws Exception {
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
  public void testTracksInit() throws Exception {
    final TwitterTaskInfo task = makeTask("jim", "myJob", 0);

    // Insert a task in the INIT state, and restart the state manager.
    storage.doInTransaction(new Work.NoResult.Quiet() {
      @Override
      protected void execute(StoreProvider storeProvider) {
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
    final TwitterTaskInfo task = makeTask("jim", "myJob", 0);

    // Insert a task in the INIT state, and restart the state manager.
    storage.doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(StoreProvider storeProvider) {
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

  @Test
  public void testTransactionalStateTransitions() throws Exception {
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
    resetStats();

    control.replay();

    mutableState = new MutableState();
    stateManager = new StateManagerImpl(storage, clock, mutableState, driver);

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
    return DbStorageTestUtil.setupStorage(this);
  }

  private static TwitterTaskInfo makeTask(String owner, String job, int shard) {
    return new TwitterTaskInfo()
        .setOwner(new Identity().setRole(owner).setUser(owner))
        .setJobName(job)
        .setShardId(shard)
        .setStartCommand("echo")
        .setRequestedPorts(ImmutableSet.<String>of());
  }

  private void assignTask(String taskId, String host) {
    stateManager.assignTask(taskId, host, SlaveID.newBuilder().setValue(host).build(),
        ImmutableSet.<Integer>of());
  }
}

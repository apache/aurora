package com.twitter.mesos.scheduler.storage.log;

import java.sql.SQLException;

import com.google.common.collect.ImmutableSet;
import com.google.common.testing.junit4.TearDownTestCase;

import org.junit.Before;
import org.junit.Test;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.TearDownRegistry;
import com.twitter.common.util.Clock;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.storage.Op;
import com.twitter.mesos.gen.storage.SaveFrameworkId;
import com.twitter.mesos.gen.storage.SaveTasks;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.db.testing.DbTestUtil;
import com.twitter.mesos.scheduler.db.testing.DbTestUtil.DbAccess;
import com.twitter.mesos.scheduler.log.Log.Position;
import com.twitter.mesos.scheduler.log.db.DbLogStream;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.db.DbStorage;
import com.twitter.mesos.scheduler.storage.log.LogManager.StreamManager.StreamTransaction;

import static com.google.common.testing.junit4.JUnitAsserts.assertNotEqual;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * @author John Sirois
 */
public class LogStorageIT extends TearDownTestCase {

  private static final Amount<Long, Time> NO_TIME = Amount.of(0L, Time.MILLISECONDS);

  private static final String INITIALIZED_FRAMEWORK_ID = "started";

  private static final Work.NoResult.Quiet INITIALIZATION_LOGIC = new Work.NoResult.Quiet() {
    @Override protected void execute(StoreProvider storeProvider) {
      storeProvider.getSchedulerStore().saveFrameworkId(INITIALIZED_FRAMEWORK_ID);
    }
  };

  private static final Work.NoResult.Quiet NOOP = new Work.NoResult.Quiet() {
    @Override protected void execute(StoreProvider storeProvider) {
      // nooop
    }
  };

  private LogStorage logStorage;
  private LogManager logManager;
  private Clock clock;
  private TearDownRegistry shutdownRegistry;
  private DbStorage dbStorage;
  private DbLogStream log;

  @Before
  public void setUp() throws SQLException {
    DbAccess dbAccess = DbTestUtil.setupStorage(this);
    dbStorage = new DbStorage(dbAccess.jdbcTemplate, dbAccess.transactionTemplate, 0);
    log = new DbLogStream(dbAccess.transactionTemplate, dbAccess.jdbcTemplate);

    shutdownRegistry = new TearDownRegistry(this);
    logManager = new LogManager(log, shutdownRegistry);
    clock = new FakeClock();
    logStorage = createLogStorage();

    logStorage.start(INITIALIZATION_LOGIC);
  }

  private LogStorage createLogStorage() {
    return new LogStorage(logManager,
        clock,
        shutdownRegistry,
        NO_TIME,
        dbStorage,
        NO_TIME,
        NO_TIME,
        dbStorage, dbStorage, dbStorage, dbStorage, dbStorage);
  }

  @Test
  public void testStart() {
    assertEquals(INITIALIZED_FRAMEWORK_ID, logStorage.fetchFrameworkId());
  }

  @Test
  public void testFullRecovery() throws Exception {
    commitTransaction(Op.saveFrameworkId(new SaveFrameworkId("1")));

    ImmutableSet<ScheduledTask> tasks = ImmutableSet.of(createTask("task1"));
    Position commit = commitTransaction(Op.saveTasks(new SaveTasks(tasks)));

    LogStorage logStorage2 = createLogStorage();
    logStorage2.start(NOOP);
    assertEquals("1", logStorage2.fetchFrameworkId());
    assertEquals(tasks, logStorage.fetchTasks(Query.GET_ALL));

    assertNoCheckpoint();

    logStorage2.acceptCheckpoint();
    assertCheckpoint(commit);
  }

  @Test
  public void testCheckpointedRecovery() throws Exception {
    logStorage.saveFrameworkId("1");
    logStorage.saveFrameworkId("2");
    logStorage.saveFrameworkId("3");

    Position commit = commitTransaction(
        Op.saveFrameworkId(new SaveFrameworkId("4")),
        Op.saveFrameworkId(new SaveFrameworkId("5")));

    assertEquals("3", logStorage.fetchFrameworkId());

    assertNoCheckpoint();
    logStorage.acceptCheckpoint();
    byte[] checkpoint = dbStorage.fetchCheckpoint();
    assertNotNull(checkpoint);
    assertNotEqual(log.position(checkpoint), commit);

    LogStorage logStorage2 = createLogStorage();
    logStorage2.start(NOOP);
    logStorage2.acceptCheckpoint();

    assertEquals("5", logStorage2.fetchFrameworkId());
    assertCheckpoint(commit);
  }

  @Test
  public void testSnapshotting() throws CodingException {
    logStorage.saveFrameworkId("pre-snapshot");
    logStorage.snapshot();
    logStorage.saveFrameworkId("post-snapshot");

    LogStorage logStorage2 = createLogStorage();
    logStorage2.start(NOOP);

    assertEquals("post-snapshot", logStorage2.fetchFrameworkId());
  }

  private Position commitTransaction(Op... ops) throws Exception {
    StreamTransaction transaction = logManager.open().startTransaction();
    for (Op op : ops) {
      transaction.add(op);
    }
    return transaction.commit();
  }

  private ScheduledTask createTask(String taskId) {
    TwitterTaskInfo taskInfo =
        new TwitterTaskInfo()
            .setOwner(new Identity("jake", "jake"))
            .setJobName("spin")
            .setShardId(42);
    AssignedTask assignedTask =
        new AssignedTask().setTaskId(taskId).setTask(taskInfo).setSlaveHost("localhost");
    return new ScheduledTask().setAssignedTask(assignedTask).setStatus(ScheduleStatus.STARTING);
  }

  private void assertNoCheckpoint() {
    byte[] checkpoint = dbStorage.fetchCheckpoint();
    assertNull(String.format("Expected no checkpoint but found %s", safeGetPosition(checkpoint)),
        checkpoint);
  }

  private void assertCheckpoint(Position position) {
    byte[] checkpoint = dbStorage.fetchCheckpoint();
    assertArrayEquals(
        String.format("Expected checkpoint at %s but found it at %s",
            position, safeGetPosition(checkpoint)),
        position.identity(), checkpoint);
  }

  private Position safeGetPosition(byte[] checkpoint) {
    return checkpoint == null ? null : log.position(checkpoint);
  }
}

package com.twitter.mesos.scheduler.storage.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.application.ActionRegistry;
import com.twitter.common.base.Closure;
import com.twitter.common.base.Closures;
import com.twitter.common.base.Command;
import com.twitter.common.base.ExceptionalCommand;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.storage.LogEntry;
import com.twitter.mesos.gen.storage.Op;
import com.twitter.mesos.gen.storage.RemoveJob;
import com.twitter.mesos.gen.storage.RemoveJobUpdate;
import com.twitter.mesos.gen.storage.RemoveQuota;
import com.twitter.mesos.gen.storage.RemoveTasks;
import com.twitter.mesos.gen.storage.SaveAcceptedJob;
import com.twitter.mesos.gen.storage.SaveFrameworkId;
import com.twitter.mesos.gen.storage.SaveJobUpdate;
import com.twitter.mesos.gen.storage.SaveQuota;
import com.twitter.mesos.gen.storage.SaveTasks;
import com.twitter.mesos.gen.storage.Snapshot;
import com.twitter.mesos.gen.storage.TaskUpdateConfiguration;
import com.twitter.mesos.gen.storage.Transaction;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.log.Log;
import com.twitter.mesos.scheduler.log.Log.Entry;
import com.twitter.mesos.scheduler.log.Log.Position;
import com.twitter.mesos.scheduler.log.Log.Stream;
import com.twitter.mesos.scheduler.storage.CheckpointStore;
import com.twitter.mesos.scheduler.storage.JobStore;
import com.twitter.mesos.scheduler.storage.QuotaStore;
import com.twitter.mesos.scheduler.storage.SchedulerStore;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.TaskStore;
import com.twitter.mesos.scheduler.storage.UpdateStore;
import com.twitter.mesos.scheduler.storage.log.LogStorage.SchedulingService;

import static org.easymock.EasyMock.aryEq;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.notNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author John Sirois
 */
public class LogStorageTest extends EasyMockTest {

  private static final Amount<Long, Time> CHECKPOINT_INTERVAL = Amount.of(1L, Time.SECONDS);
  private static final Amount<Long, Time> SNAPSHOT_INTERVAL = Amount.of(1L, Time.MINUTES);
  private static final long NOW = 42L;

  private LogStorage logStorage;
  private Log log;
  private Stream stream;
  private ActionRegistry shutdownRegistry;
  private FakeClock clock;
  private SchedulingService schedulingService;
  private CheckpointStore checkpointStore;
  private Storage storage;
  private SchedulerStore schedulerStore;
  private JobStore jobStore;
  private TaskStore taskStore;
  private UpdateStore updateStore;
  private StoreProvider storeProvider;
  private QuotaStore quotaStore;

  @Before
  public void setUp() {
    log = createMock(Log.class);

    shutdownRegistry = createMock(ActionRegistry.class);
    LogManager logManager = new LogManager(log, shutdownRegistry);

    clock = new FakeClock();
    clock.setNowMillis(NOW);

    schedulingService = createMock(SchedulingService.class);
    checkpointStore = createMock(CheckpointStore.class);
    storage = createMock(Storage.class);
    schedulerStore = createMock(SchedulerStore.class);
    jobStore = createMock(JobStore.class);
    taskStore = createMock(TaskStore.class);
    updateStore = createMock(UpdateStore.class);
    storeProvider = createMock(StoreProvider.class);
    quotaStore = createMock(QuotaStore.class);

    logStorage =
        new LogStorage(logManager,
            clock,
            schedulingService,
            checkpointStore,
            CHECKPOINT_INTERVAL,
            SNAPSHOT_INTERVAL,
            storage,
            schedulerStore,
            jobStore,
            taskStore,
            updateStore,
            quotaStore);

    stream = createMock(Stream.class);
  }

  @Test
  public void testStart() throws Exception {
    // We should open the log and arrange for its clean shutdown.
    expect(log.open()).andReturn(stream);

    Capture<ExceptionalCommand<IOException>> shutdownStream = createCapture();
    shutdownRegistry.addAction(capture(shutdownStream));
    stream.close();

    // Our start should recover the log and then forward to the underlying storage start of the
    // supplied initialization logic.
    final AtomicBoolean initialized = new AtomicBoolean(false);
    Work.NoResult.Quiet initializationLogic = new Work.NoResult.Quiet() {
      @Override protected void execute(StoreProvider storeProvider) {
        // Creating a mock and expecting apply(storeProvider) does not work here for whatever
        // reason.
        initialized.set(true);
      }
    };

    final Capture<Work.NoResult.Quiet> recoverAndInitializeWork = createCapture();
    storage.start(capture(recoverAndInitializeWork));
    expectLastCall().andAnswer(new IAnswer<Void>() {
      @Override public Void answer() throws Throwable {
        recoverAndInitializeWork.getValue().apply(storeProvider);
        return null;
      }
    });

    // The last known good log entry should be read and skipped
    Entry lastKnownGood = createMock(Entry.class);
    byte[] checkpoint = "last-known-good".getBytes();
    expect(checkpointStore.fetchCheckpoint()).andReturn(checkpoint);
    Position checkpointPosition = createMock(Position.class);
    expect(stream.position(checkpoint)).andReturn(checkpointPosition);

    // But the next entry should be read and applied.
    Entry recovered = createMock(Entry.class);
    Position recoveredEntryPosition = createMock(Position.class);
    byte[] recoveredEntryIdentity = "recovered-entry".getBytes();
    expect(recoveredEntryPosition.identity()).andReturn(recoveredEntryIdentity);
    expect(recovered.position()).andReturn(recoveredEntryPosition);
    String frameworkId = "bob";
    LogEntry recoveredLogEntry =
        createTransaction(Op.saveFrameworkId(new SaveFrameworkId(frameworkId)));
    expect(recovered.contents()).andReturn(ThriftBinaryCodec.encodeNonNull(recoveredLogEntry));
    expect(stream.readFrom(checkpointPosition))
        .andReturn(Iterators.<Entry>forArray(lastKnownGood, recovered));

    final Capture<Work<Void, RuntimeException>> recoveryWork = createCapture();
    expect(storage.doInTransaction(capture(recoveryWork))).andAnswer(new IAnswer<Void>() {
      @Override public Void answer() {
        recoveryWork.getValue().apply(storeProvider);
        return null;
      }
    });
    schedulerStore.saveFrameworkId(frameworkId);

    final Capture<Work<Void, RuntimeException>> initializationWork = createCapture();
    expect(storage.doInTransaction(capture(initializationWork))).andAnswer(new IAnswer<Void>() {
      @Override public Void answer() {
        initializationWork.getValue().apply(storeProvider);
        return null;
      }
    });

    // We should pick up a checkpoint for the recovered entry when the checkpoint thread runs.
    Capture<Runnable> checkpointAction = createCapture();
    schedulingService.doEvery(eq(CHECKPOINT_INTERVAL), capture(checkpointAction));
    checkpointStore.checkpoint(recoveredEntryIdentity);

    // We should perform a snapshot when the snapshot thread runs.
    Capture<Runnable> snapshotAction = createCapture();
    schedulingService.doEvery(eq(SNAPSHOT_INTERVAL), capture(snapshotAction));
    byte[] snapshotContents = "snapshot".getBytes();
    expect(checkpointStore.createSnapshot()).andReturn(snapshotContents);
    Position snapshotPosition = createMock(Position.class);
    LogEntry snapshot =
        LogEntry.snapshot(new Snapshot(NOW, ByteBuffer.wrap(snapshotContents)));
    expect(stream.append(aryEq(ThriftBinaryCodec.encodeNonNull(snapshot))))
        .andReturn(snapshotPosition);
    stream.truncateBefore(snapshotPosition);
    final Capture<Work<Void, RuntimeException>> snapshotWork = createCapture();
    expect(storage.doInTransaction(capture(snapshotWork))).andAnswer(new IAnswer<Void>() {
      @Override public Void answer() {
        snapshotWork.getValue().apply(storeProvider);
        return null;
      }
    });

    control.replay();

    logStorage.start(initializationLogic);
    assertTrue(initialized.get());

    assertTrue(checkpointAction.hasCaptured());
    // Run the checkpoint thread.
    checkpointAction.getValue().run();

    assertTrue(snapshotAction.hasCaptured());
    // Run the snapshot thread.
    snapshotAction.getValue().run();

    assertTrue(shutdownStream.hasCaptured());
    shutdownStream.getValue().execute();
  }

  abstract class MutationFixture {
    public void runTest() throws Exception {
      // Expect basic start operations.

      // Open the log stream.
      expect(log.open()).andReturn(stream);
      shutdownRegistry.addAction(EasyMock.<Command>notNull());

      // Replay the log and perform and supplied initializationWork.
      // Simulate NOOP initialization work
      Work.NoResult.Quiet initializationLogic = new Work.NoResult.Quiet() {
        @Override protected void execute(StoreProvider storeProvider) {
          // Creating a mock and expecting apply(storeProvider) does not work here for whatever
          // reason.
        }
      };

      final Capture<Work.NoResult.Quiet> recoverAndInitializeWork = createCapture();
      storage.start(capture(recoverAndInitializeWork));
      expectLastCall().andAnswer(new IAnswer<Void>() {
        @Override public Void answer() throws Throwable {
          recoverAndInitializeWork.getValue().apply(storeProvider);
          return null;
        }
      });

      expect(checkpointStore.fetchCheckpoint()).andReturn(null);
      Position beginning = createMock(Position.class);
      expect(stream.beginning()).andReturn(beginning);
      expect(stream.readFrom(beginning)).andReturn(Iterators.<Entry>emptyIterator());
      final Capture<Work<Void, RuntimeException>> recoveryWork = createCapture();
      expect(storage.doInTransaction(capture(recoveryWork))).andAnswer(new IAnswer<Void>() {
        @Override public Void answer() {
          recoveryWork.getValue().apply(storeProvider);
          return null;
        }
      });

      // Schedule checkpoints and snapshots.
      schedulingService.doEvery(eq(CHECKPOINT_INTERVAL), notNull(Runnable.class));
      schedulingService.doEvery(eq(SNAPSHOT_INTERVAL), notNull(Runnable.class));

      // Setup custom test expectations.
      setupExpectations();

      control.replay();

      // Start the system.
      logStorage.start(initializationLogic);

      // Exercise the system.
      performMutations();
    }

    protected abstract void setupExpectations() throws Exception;
    protected abstract void performMutations();
  }

  @Test
  public void testSaveFrameworkId() throws Exception {
    new MutationFixture() {
      private String frameworkId = "bob";

      @Override protected void setupExpectations() throws CodingException {
        expectStorageTransactionNoResult();
        schedulerStore.saveFrameworkId(frameworkId);
        expectStreamTransaction(Op.saveFrameworkId(new SaveFrameworkId(frameworkId)));
      }

      @Override protected void performMutations() {
        logStorage.saveFrameworkId(frameworkId);
      }
    }.runTest();
  }

  @Test
  public void testSaveAcceptedJob() throws Exception {
    new MutationFixture() {
      private JobConfiguration jobConfig = new JobConfiguration().setName("jake");
      private String managerId = "CRON";

      @Override protected void setupExpectations() throws Exception {
        expectStorageTransactionNoResult();
        jobStore.saveAcceptedJob(managerId, jobConfig);
        expectStreamTransaction(Op.saveAcceptedJob(new SaveAcceptedJob(managerId, jobConfig)));
      }

      @Override protected void performMutations() {
        logStorage.saveAcceptedJob(managerId, jobConfig);
      }
    }.runTest();
  }

  @Test
  public void testRemoveJob() throws Exception {
    new MutationFixture() {
      private String jobKey = "job/key";

      @Override protected void setupExpectations() throws Exception {
        expectStorageTransactionNoResult();
        jobStore.removeJob(jobKey);
        expectStreamTransaction(Op.removeJob(new RemoveJob(jobKey)));
      }

      @Override protected void performMutations() {
        logStorage.removeJob(jobKey);
      }
    }.runTest();
  }

  @Test
  public void testSaveTasks() throws Exception {
    new MutationFixture() {
      private Set<ScheduledTask> tasks =
          ImmutableSet.of(new ScheduledTask().setStatus(ScheduleStatus.INIT));

      @Override protected void setupExpectations() throws Exception {
        expectStorageTransactionNoResult();
        taskStore.saveTasks(tasks);
        expectStreamTransaction(Op.saveTasks(new SaveTasks(tasks)));
      }

      @Override protected void performMutations() {
        logStorage.saveTasks(tasks);
      }
    }.runTest();
  }

  @Test
  public void testMutateTasks() throws Exception {
    new MutationFixture() {

      private Query query = Query.byId("fred");
      private Closure<ScheduledTask> mutation = Closures.noop();
      private ImmutableSet<ScheduledTask> mutated =
          ImmutableSet.of(new ScheduledTask().setStatus(ScheduleStatus.STARTING));

      @Override protected void setupExpectations() throws Exception {
        expectStorageTransactionWithResult(mutated);
        expect(taskStore.mutateTasks(query, mutation)).andReturn(mutated);
        expectStreamTransaction(Op.saveTasks(new SaveTasks(mutated)));
      }

      @Override protected void performMutations() {
        assertEquals(mutated, logStorage.mutateTasks(query, mutation));
      }
    }.runTest();
  }

  @Test
  public void testRemoveTasks_query() throws Exception {
    new MutationFixture() {
      private Set<String> taskIds = ImmutableSet.of("42");

      @Override protected void setupExpectations() throws Exception {
        expectStorageTransactionNoResult();
        expectStorageTransactionNoResult(); // nested
        expect(taskStore.fetchTaskIds(Query.GET_ALL)).andReturn(taskIds);
        taskStore.removeTasks(taskIds);
        expectStreamTransaction(Op.removeTasks(new RemoveTasks(taskIds)));
      }

      @Override protected void performMutations() {
        logStorage.removeTasks(Query.GET_ALL);
      }
    }.runTest();
  }

  @Test
  public void testRemoveTasks_ids() throws Exception {
    new MutationFixture() {
      private Set<String> taskIds = ImmutableSet.of("42");

      @Override protected void setupExpectations() throws Exception {
        expectStorageTransactionNoResult();
        taskStore.removeTasks(taskIds);
        expectStreamTransaction(Op.removeTasks(new RemoveTasks(taskIds)));
      }

      @Override protected void performMutations() {
        logStorage.removeTasks(taskIds);
      }
    }.runTest();
  }

  @Test
  public void testSaveShardUpdateConfigs() throws Exception {
    new MutationFixture() {
      private String role = "role";
      private String job = "job";
      private String updateToken = "update-ok";
      private ImmutableSet<TaskUpdateConfiguration> updateConfiguration =
          ImmutableSet.of(
              new TaskUpdateConfiguration().setNewConfig(new TwitterTaskInfo().setShardId(42)));

      @Override protected void setupExpectations() throws Exception {
        expectStorageTransactionNoResult();
        updateStore.saveShardUpdateConfigs(role, job, updateToken, updateConfiguration);
        expectStreamTransaction(
            Op.saveJobUpdate(new SaveJobUpdate(role, job, updateToken, updateConfiguration)));
      }

      @Override protected void performMutations() {
        logStorage.saveShardUpdateConfigs(role, job, updateToken, updateConfiguration);
      }
    }.runTest();
  }

  @Test
  public void testRemoveShardUpdateConfigs() throws Exception {
    new MutationFixture() {
      private String role = "role";
      private String job = "job";

      @Override protected void setupExpectations() throws Exception {
        expectStorageTransactionNoResult();
        updateStore.removeShardUpdateConfigs(role, job);
        expectStreamTransaction(Op.removeJobUpdate(new RemoveJobUpdate(role, job)));
      }

      @Override protected void performMutations() {
        logStorage.removeShardUpdateConfigs(role, job);
      }
    }.runTest();
  }

  @Test
  public void testSaveQuota() throws Exception {
    new MutationFixture() {
      private String role = "role";
      private Quota quota = new Quota(1.0, 128L, 1024L);

      @Override protected void setupExpectations() throws Exception {
        expectStorageTransactionNoResult();
        quotaStore.saveQuota(role, quota);
        expectStreamTransaction(Op.saveQuota(new SaveQuota(role, quota)));
      }

      @Override protected void performMutations() {
        logStorage.saveQuota(role, quota);
      }
    }.runTest();
  }

  @Test
  public void testRemoveQuota() throws Exception {
    new MutationFixture() {
      private String role = "role";

      @Override protected void setupExpectations() throws Exception {
        expectStorageTransactionNoResult();
        quotaStore.removeQuota(role);
        expectStreamTransaction(Op.removeQuota(new RemoveQuota(role)));
      }

      @Override protected void performMutations() {
        logStorage.removeQuota(role);
      }
    }.runTest();
  }

  private void expectStreamTransaction(Op... ops) throws CodingException {
    expect(stream.append(EasyMock.aryEq(ThriftBinaryCodec.encode(createTransaction(ops)))))
        .andReturn(null);
  }

  private LogEntry createTransaction(Op... ops) {
    return LogEntry.transaction(new Transaction(ImmutableList.copyOf(ops)));
  }

  private void expectStorageTransactionNoResult() {
    final Capture<Work<Void, RuntimeException>> wrappedWork = createCapture();
    expect(storage.doInTransaction(capture(wrappedWork))).andAnswer(new IAnswer<Void>() {
      @Override public Void answer() throws Throwable {
        wrappedWork.getValue().apply(storeProvider);
        return null;
      }
    });
  }

  private <T> void expectStorageTransactionWithResult(final T expectedResult) {
    final Capture<Work<T, RuntimeException>> wrappedWork = createCapture();
    expect(storage.doInTransaction(capture(wrappedWork))).andAnswer(new IAnswer<T>() {
      @Override public T answer() throws Throwable {
        T result = wrappedWork.getValue().apply(storeProvider);
        assertEquals(expectedResult, result);
        return result;
      }
    });
  }
}

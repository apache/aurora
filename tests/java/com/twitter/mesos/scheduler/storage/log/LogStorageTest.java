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

import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.base.Closure;
import com.twitter.common.base.Closures;
import com.twitter.common.base.Command;
import com.twitter.common.base.ExceptionalCommand;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.HostAttributes;
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
import com.twitter.mesos.scheduler.storage.AttributeStore;
import com.twitter.mesos.scheduler.storage.JobStore;
import com.twitter.mesos.scheduler.storage.QuotaStore;
import com.twitter.mesos.scheduler.storage.SchedulerStore;
import com.twitter.mesos.scheduler.storage.SnapshotStore;
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

  private static final Amount<Long, Time> SNAPSHOT_INTERVAL = Amount.of(1L, Time.MINUTES);
  private static final long NOW = 42L;

  private LogStorage logStorage;
  private Log log;
  private Stream stream;
  private ShutdownRegistry shutdownRegistry;
  private SchedulingService schedulingService;
  private SnapshotStore<Snapshot> snapshotStore;
  private Storage storage;
  private SchedulerStore schedulerStore;
  private JobStore jobStore;
  private TaskStore taskStore;
  private UpdateStore updateStore;
  private StoreProvider storeProvider;
  private QuotaStore quotaStore;
  private AttributeStore attributeStore;

  @Before
  public void setUp() {
    log = createMock(Log.class);

    shutdownRegistry = createMock(ShutdownRegistry.class);
    LogManager logManager = new LogManager(log, Amount.of(1, Data.GB), shutdownRegistry);

    schedulingService = createMock(SchedulingService.class);
    snapshotStore = createMock(new Clazz<SnapshotStore<Snapshot>>() {});
    storage = createMock(Storage.class);
    schedulerStore = createMock(SchedulerStore.class);
    jobStore = createMock(JobStore.class);
    taskStore = createMock(TaskStore.class);
    updateStore = createMock(UpdateStore.class);
    storeProvider = createMock(StoreProvider.class);
    quotaStore = createMock(QuotaStore.class);
    attributeStore = createMock(AttributeStore.class);

    logStorage =
        new LogStorage(logManager,
            schedulingService,
            snapshotStore,
            SNAPSHOT_INTERVAL,
            storage,
            schedulerStore,
            jobStore,
            taskStore,
            updateStore,
            quotaStore,
            attributeStore);

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

    Entry entry1 = createMock(Entry.class);
    Entry entry2 = createMock(Entry.class);
    String frameworkId1 = "bob";
    LogEntry recoveredEntry1 =
        createTransaction(Op.saveFrameworkId(new SaveFrameworkId(frameworkId1)));
    String frameworkId2 = "jim";
    LogEntry recoveredEntry2 =
        createTransaction(Op.saveFrameworkId(new SaveFrameworkId(frameworkId2)));
    expect(entry1.contents()).andReturn(ThriftBinaryCodec.encodeNonNull(recoveredEntry1));
    expect(entry2.contents()).andReturn(ThriftBinaryCodec.encodeNonNull(recoveredEntry2));
    expect(stream.readAll()).andReturn(Iterators.<Entry>forArray(entry1, entry2));

    final Capture<Work<Void, RuntimeException>> recoveryWork = createCapture();
    expect(storage.doInTransaction(capture(recoveryWork))).andAnswer(new IAnswer<Void>() {
      @Override public Void answer() {
        recoveryWork.getValue().apply(storeProvider);
        return null;
      }
    });
    schedulerStore.saveFrameworkId(frameworkId1);
    schedulerStore.saveFrameworkId(frameworkId2);

    final Capture<Work<Void, RuntimeException>> initializationWork = createCapture();
    expect(storage.doInTransaction(capture(initializationWork))).andAnswer(new IAnswer<Void>() {
      @Override public Void answer() {
        initializationWork.getValue().apply(storeProvider);
        return null;
      }
    });

    // We should perform a snapshot when the snapshot thread runs.
    Capture<Runnable> snapshotAction = createCapture();
    schedulingService.doEvery(eq(SNAPSHOT_INTERVAL), capture(snapshotAction));
    Snapshot snapshotContents = new Snapshot(NOW,
        ByteBuffer.wrap("snapshot".getBytes()), ImmutableSet.<HostAttributes>of());
    expect(snapshotStore.createSnapshot()).andReturn(snapshotContents);
    Position snapshotPosition = createMock(Position.class);
    LogEntry snapshot = LogEntry.snapshot(snapshotContents);
    expect(stream.append(aryEq(ThriftBinaryCodec.encodeNonNull(snapshot))))
        .andReturn(snapshotPosition);
    stream.truncateBefore(snapshotPosition);
    final Capture<Work<Void, RuntimeException>> snapshotWork = createCapture();
    expect(storage.doInTransaction(capture(snapshotWork))).andAnswer(new IAnswer<Void>() {
      @Override public Void answer() {
        snapshotWork.getValue().apply(storeProvider);
        return null;
      }
    }).times(2);

    control.replay();

    logStorage.prepare();
    logStorage.start(initializationLogic);
    assertTrue(initialized.get());

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
      // Creating a mock and expecting apply(storeProvider) does not work here for whatever
      // reason.
      Work.NoResult.Quiet initializationLogic = Work.NOOP;

      final Capture<Work.NoResult.Quiet> recoverAndInitializeWork = createCapture();
      storage.start(capture(recoverAndInitializeWork));
      expectLastCall().andAnswer(new IAnswer<Void>() {
        @Override public Void answer() throws Throwable {
          recoverAndInitializeWork.getValue().apply(storeProvider);
          return null;
        }
      });

      expect(stream.readAll()).andReturn(Iterators.<Entry>emptyIterator());
      final Capture<Work<Void, RuntimeException>> recoveryWork = createCapture();
      expect(storage.doInTransaction(capture(recoveryWork))).andAnswer(new IAnswer<Void>() {
        @Override public Void answer() {
          recoveryWork.getValue().apply(storeProvider);
          return null;
        }
      });

      // Schedule snapshots.
      schedulingService.doEvery(eq(SNAPSHOT_INTERVAL), notNull(Runnable.class));

      // Setup custom test expectations.
      setupExpectations();

      control.replay();

      // Start the system.
      logStorage.prepare();
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
          ImmutableSet.of(task("a", ScheduleStatus.INIT));

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
          ImmutableSet.of(task("a", ScheduleStatus.STARTING));

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
  public void testNestedTransactions() throws Exception {
    new MutationFixture() {

      private Query query = Query.byId("fred");
      private Closure<ScheduledTask> mutation = Closures.noop();
      private ImmutableSet<ScheduledTask> mutated =
          ImmutableSet.of(task("a", ScheduleStatus.STARTING));
      private ImmutableSet<String> tasksToRemove = ImmutableSet.of("b");

      @Override protected void setupExpectations() throws Exception {
        expectStorageTransactionNoResult();
        expectStorageTransactionWithResult(mutated);
        expect(taskStore.mutateTasks(query, mutation)).andReturn(mutated);

        expectStorageTransactionNoResult();
        expectStorageTransactionNoResult();
        taskStore.removeTasks(tasksToRemove);

        expectStreamTransaction(Op.saveTasks(new SaveTasks(mutated)),
            Op.removeTasks(new RemoveTasks(tasksToRemove)));
      }

      @Override protected void performMutations() {
        logStorage.doInTransaction(new Work.NoResult.Quiet() {
          @Override protected void execute(StoreProvider storeProvider) {
            assertEquals(mutated, storeProvider.getTaskStore().mutateTasks(query, mutation));
            logStorage.doInTransaction(new Work.NoResult.Quiet() {
              @Override protected void execute(StoreProvider storeProvider) {
                storeProvider.getTaskStore().removeTasks(tasksToRemove);
              }
            });
          }
        });
      }
    }.runTest();
  }

  @Test
  public void testSaveAndMutateTasks() throws Exception {
    new MutationFixture() {

      private Query query = Query.byId("fred");
      private Closure<ScheduledTask> mutation = Closures.noop();
      private Set<ScheduledTask> saved = ImmutableSet.of(task("a", ScheduleStatus.INIT));
      private ImmutableSet<ScheduledTask> mutated =
          ImmutableSet.of(task("a", ScheduleStatus.PENDING));

      @Override protected void setupExpectations() throws Exception {
        // Outer transaction.
        expectStorageTransactionNoResult();

        // Nested transaction and save.
        expectStorageTransactionNoResult();
        taskStore.saveTasks(saved);

        // Nested transaction with result.
        expectStorageTransactionWithResult(mutated);
        expect(taskStore.mutateTasks(query, mutation)).andReturn(mutated);

        // Resulting stream operation.
        expectStreamTransaction(Op.saveTasks(new SaveTasks(mutated)));
      }

      @Override protected void performMutations() {
        logStorage.doInTransaction(new Work.NoResult.Quiet() {
          @Override protected void execute(StoreProvider storeProvider) {
            logStorage.saveTasks(saved);
            assertEquals(mutated, logStorage.mutateTasks(query, mutation));
          }
        });
      }
    }.runTest();
  }

  @Test
  public void testSaveAndMutateTasks_noCoalesceUniqueIds() throws Exception {
    new MutationFixture() {

      private Query query = Query.byId("fred");
      private Closure<ScheduledTask> mutation = Closures.noop();
      private Set<ScheduledTask> saved = ImmutableSet.of(task("b", ScheduleStatus.INIT));
      private ImmutableSet<ScheduledTask> mutated =
          ImmutableSet.of(task("a", ScheduleStatus.PENDING));

      @Override protected void setupExpectations() throws Exception {
        // Outer transaction.
        expectStorageTransactionNoResult();

        // Nested transaction and save.
        expectStorageTransactionNoResult();
        taskStore.saveTasks(saved);

        // Nested transaction with result.
        expectStorageTransactionWithResult(mutated);
        expect(taskStore.mutateTasks(query, mutation)).andReturn(mutated);

        // Resulting stream operation.
        expectStreamTransaction(Op.saveTasks(new SaveTasks(
            ImmutableSet.<ScheduledTask>builder().addAll(saved).addAll(mutated).build())));
      }

      @Override protected void performMutations() {
        logStorage.doInTransaction(new Work.NoResult.Quiet() {
          @Override protected void execute(StoreProvider storeProvider) {
            logStorage.saveTasks(saved);
            assertEquals(mutated, logStorage.mutateTasks(query, mutation));
          }
        });
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

  private static ScheduledTask task(String id, ScheduleStatus status) {
    return new ScheduledTask()
        .setStatus(status)
        .setAssignedTask(new AssignedTask()
            .setTaskId(id));
  }
}

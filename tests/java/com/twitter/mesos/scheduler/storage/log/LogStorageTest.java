package com.twitter.mesos.scheduler.storage.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Optional;
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
import com.twitter.mesos.gen.Attribute;
import com.twitter.mesos.gen.HostAttributes;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.storage.JobUpdateConfiguration;
import com.twitter.mesos.gen.storage.LogEntry;
import com.twitter.mesos.gen.storage.Op;
import com.twitter.mesos.gen.storage.RemoveJob;
import com.twitter.mesos.gen.storage.RemoveJobUpdate;
import com.twitter.mesos.gen.storage.RemoveQuota;
import com.twitter.mesos.gen.storage.RemoveTasks;
import com.twitter.mesos.gen.storage.SaveAcceptedJob;
import com.twitter.mesos.gen.storage.SaveFrameworkId;
import com.twitter.mesos.gen.storage.SaveHostAttributes;
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
import com.twitter.mesos.scheduler.storage.SnapshotStore;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;
import com.twitter.mesos.scheduler.storage.log.LogStorage.SchedulingService;
import com.twitter.mesos.scheduler.storage.testing.StorageTestUtil;

import static org.easymock.EasyMock.aryEq;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.notNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LogStorageTest extends EasyMockTest {

  private static final Amount<Long, Time> SNAPSHOT_INTERVAL = Amount.of(1L, Time.MINUTES);
  private static final long NOW = 42L;

  private LogStorage logStorage;
  private Log log;
  private Stream stream;
  private ShutdownRegistry shutdownRegistry;
  private SchedulingService schedulingService;
  private SnapshotStore<Snapshot> snapshotStore;
  private StorageTestUtil storageUtil;

  @Before
  public void setUp() {
    log = createMock(Log.class);

    shutdownRegistry = createMock(ShutdownRegistry.class);
    LogManager logManager = new LogManager(log, Amount.of(1, Data.GB), shutdownRegistry);

    schedulingService = createMock(SchedulingService.class);
    snapshotStore = createMock(new Clazz<SnapshotStore<Snapshot>>() { });
    storageUtil = new StorageTestUtil(this);

    logStorage =
        new LogStorage(logManager,
            schedulingService,
            snapshotStore,
            SNAPSHOT_INTERVAL,
            storageUtil.storage,
            storageUtil.schedulerStore,
            storageUtil.jobStore,
            storageUtil.taskStore,
            storageUtil.updateStore,
            storageUtil.quotaStore,
            storageUtil.attributeStore);

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
    MutateWork.NoResult.Quiet initializationLogic = new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider provider) {
        // Creating a mock and expecting apply(storeProvider) does not work here for whatever
        // reason.
        initialized.set(true);
      }
    };

    final Capture<MutateWork.NoResult.Quiet> recoverAndInitializeWork = createCapture();
    storageUtil.storage.start(capture(recoverAndInitializeWork));
    expectLastCall().andAnswer(new IAnswer<Void>() {
      @Override public Void answer() throws Throwable {
        recoverAndInitializeWork.getValue().apply(storageUtil.mutableStoreProvider);
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

    final Capture<MutateWork<Void, RuntimeException>> recoveryWork = createCapture();
    expect(storageUtil.storage.doInWriteTransaction(capture(recoveryWork))).andAnswer(
        new IAnswer<Void>() {
          @Override public Void answer() {
            recoveryWork.getValue().apply(storageUtil.mutableStoreProvider);
            return null;
          }
        });
    storageUtil.schedulerStore.saveFrameworkId(frameworkId1);
    storageUtil.schedulerStore.saveFrameworkId(frameworkId2);

    final Capture<MutateWork<Void, RuntimeException>> initializationWork = createCapture();
    expect(storageUtil.storage.doInWriteTransaction(capture(initializationWork))).andAnswer(
        new IAnswer<Void>() {
          @Override public Void answer() {
            initializationWork.getValue().apply(storageUtil.mutableStoreProvider);
            return null;
          }
        });

    // We should perform a snapshot when the snapshot thread runs.
    Capture<Runnable> snapshotAction = createCapture();
    schedulingService.doEvery(eq(SNAPSHOT_INTERVAL), capture(snapshotAction));
    Snapshot snapshotContents = new Snapshot()
        .setTimestamp(NOW)
        .setDataDEPRECATED(ByteBuffer.wrap("snapshot".getBytes()));
    expect(snapshotStore.createSnapshot()).andReturn(snapshotContents);
    Position snapshotPosition = createMock(Position.class);
    LogEntry snapshot = LogEntry.snapshot(snapshotContents);
    expect(stream.append(aryEq(ThriftBinaryCodec.encodeNonNull(snapshot))))
        .andReturn(snapshotPosition);
    stream.truncateBefore(snapshotPosition);
    final Capture<MutateWork<Void, RuntimeException>> snapshotWork = createCapture();
    expect(storageUtil.storage.doInWriteTransaction(capture(snapshotWork))).andAnswer(
        new IAnswer<Void>() {
          @Override public Void answer() {
            snapshotWork.getValue().apply(storageUtil.mutableStoreProvider);
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
    MutationFixture() throws Exception {
      // Expect basic start operations.

      // Open the log stream.
      expect(log.open()).andReturn(stream);
      shutdownRegistry.addAction(EasyMock.<Command>notNull());

      // Replay the log and perform and supplied initializationWork.
      // Simulate NOOP initialization work
      // Creating a mock and expecting apply(storeProvider) does not work here for whatever
      // reason.
      MutateWork.NoResult.Quiet initializationLogic = MutateWork.NOOP;

      final Capture<MutateWork.NoResult.Quiet> recoverAndInitializeWork = createCapture();
      storageUtil.storage.start(capture(recoverAndInitializeWork));
      expectLastCall().andAnswer(new IAnswer<Void>() {
        @Override public Void answer() throws Throwable {
          recoverAndInitializeWork.getValue().apply(storageUtil.mutableStoreProvider);
          return null;
        }
      });

      expect(stream.readAll()).andReturn(Iterators.<Entry>emptyIterator());
      final Capture<MutateWork<Void, RuntimeException>> recoveryWork = createCapture();
      expect(storageUtil.storage.doInWriteTransaction(capture(recoveryWork))).andAnswer(
          new IAnswer<Void>() {
            @Override public Void answer() {
              recoveryWork.getValue().apply(storageUtil.mutableStoreProvider);
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
    final String frameworkId = "bob";
    new MutationFixture() {
      @Override protected void setupExpectations() throws CodingException {
        storageUtil.expectTransactions();
        storageUtil.schedulerStore.saveFrameworkId(frameworkId);
        expectStreamTransaction(Op.saveFrameworkId(new SaveFrameworkId(frameworkId)));
      }

      @Override protected void performMutations() {
        logStorage.saveFrameworkId(frameworkId);
      }
    };
  }

  @Test
  public void testSaveAcceptedJob() throws Exception {
    final JobConfiguration jobConfig = new JobConfiguration().setName("jake");
    final String managerId = "CRON";
    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectTransactions();
        storageUtil.jobStore.saveAcceptedJob(managerId, jobConfig);
        expectStreamTransaction(Op.saveAcceptedJob(new SaveAcceptedJob(managerId, jobConfig)));
      }

      @Override protected void performMutations() {
        logStorage.saveAcceptedJob(managerId, jobConfig);
      }
    };
  }

  @Test
  public void testRemoveJob() throws Exception {
    final String jobKey = "job/key";
    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectTransactions();
        storageUtil.jobStore.removeJob(jobKey);
        expectStreamTransaction(Op.removeJob(new RemoveJob(jobKey)));
      }

      @Override protected void performMutations() {
        logStorage.removeJob(jobKey);
      }
    };
  }

  @Test
  public void testSaveTasks() throws Exception {
    final Set<ScheduledTask> tasks = ImmutableSet.of(task("a", ScheduleStatus.INIT));
    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectTransactions();
        storageUtil.taskStore.saveTasks(tasks);
        expectStreamTransaction(Op.saveTasks(new SaveTasks(tasks)));
      }

      @Override protected void performMutations() {
        logStorage.saveTasks(tasks);
      }
    };
  }

  @Test
  public void testMutateTasks() throws Exception {
    final TaskQuery query = Query.byId("fred");
    final Closure<ScheduledTask> mutation = Closures.noop();
    final ImmutableSet<ScheduledTask> mutated = ImmutableSet.of(task("a", ScheduleStatus.STARTING));
    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectTransactions();
        expect(storageUtil.taskStore.mutateTasks(query, mutation)).andReturn(mutated);
        expectStreamTransaction(Op.saveTasks(new SaveTasks(mutated)));
      }

      @Override protected void performMutations() {
        assertEquals(mutated, logStorage.mutateTasks(query, mutation));
      }
    };
  }

  @Test
  public void testNestedTransactions() throws Exception {
    final TaskQuery query = Query.byId("fred");
    final Closure<ScheduledTask> mutation = Closures.noop();
    final ImmutableSet<ScheduledTask> mutated =
        ImmutableSet.of(task("a", ScheduleStatus.STARTING));
    final ImmutableSet<String> tasksToRemove = ImmutableSet.of("b");

    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectTransactions();
        expect(storageUtil.taskStore.mutateTasks(query, mutation)).andReturn(mutated);

        storageUtil.taskStore.deleteTasks(tasksToRemove);

        expectStreamTransaction(Op.saveTasks(new SaveTasks(mutated)),
            Op.removeTasks(new RemoveTasks(tasksToRemove)));
      }

      @Override protected void performMutations() {
        logStorage.doInWriteTransaction(new MutateWork.NoResult.Quiet() {
          @Override protected void execute(MutableStoreProvider provider) {
            assertEquals(mutated, provider.getTaskStore().mutateTasks(query, mutation));
            logStorage.doInWriteTransaction(new MutateWork.NoResult.Quiet() {
              @Override protected void execute(MutableStoreProvider innerProvider) {
                innerProvider.getTaskStore().deleteTasks(tasksToRemove);
              }
            });
          }
        });
      }
    };
  }

  @Test
  public void testSaveAndMutateTasks() throws Exception {
    final TaskQuery query = Query.byId("fred");
    final Closure<ScheduledTask> mutation = Closures.noop();
    final Set<ScheduledTask> saved = ImmutableSet.of(task("a", ScheduleStatus.INIT));
    final ImmutableSet<ScheduledTask> mutated = ImmutableSet.of(task("a", ScheduleStatus.PENDING));

    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectTransactions();
        storageUtil.taskStore.saveTasks(saved);

        // Nested transaction with result.
        expect(storageUtil.taskStore.mutateTasks(query, mutation)).andReturn(mutated);

        // Resulting stream operation.
        expectStreamTransaction(Op.saveTasks(new SaveTasks(mutated)));
      }

      @Override protected void performMutations() {
        logStorage.doInWriteTransaction(new MutateWork.NoResult.Quiet() {
          @Override protected void execute(MutableStoreProvider provider) {
            logStorage.saveTasks(saved);
            assertEquals(mutated, logStorage.mutateTasks(query, mutation));
          }
        });
      }
    };
  }

  @Test
  public void testSaveAndMutateTasksNoCoalesceUniqueIds() throws Exception {
    final TaskQuery query = Query.byId("fred");
    final Closure<ScheduledTask> mutation = Closures.noop();
    final Set<ScheduledTask> saved = ImmutableSet.of(task("b", ScheduleStatus.INIT));
    final ImmutableSet<ScheduledTask> mutated = ImmutableSet.of(task("a", ScheduleStatus.PENDING));

    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectTransactions();
        storageUtil.taskStore.saveTasks(saved);

        // Nested transaction with result.
        expect(storageUtil.taskStore.mutateTasks(query, mutation)).andReturn(mutated);

        // Resulting stream operation.
        expectStreamTransaction(Op.saveTasks(new SaveTasks(
            ImmutableSet.<ScheduledTask>builder().addAll(saved).addAll(mutated).build())));
      }

      @Override protected void performMutations() {
        logStorage.doInWriteTransaction(new MutateWork.NoResult.Quiet() {
          @Override protected void execute(MutableStoreProvider provider) {
            logStorage.saveTasks(saved);
            assertEquals(mutated, logStorage.mutateTasks(query, mutation));
          }
        });
      }
    };
  }

  @Test
  public void testRemoveTasksQuery() throws Exception {
    final Set<String> taskIds = ImmutableSet.of("42");
    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectTransactions();
        expect(storageUtil.taskStore.fetchTaskIds(Query.GET_ALL)).andReturn(taskIds);
        storageUtil.taskStore.deleteTasks(taskIds);
        expectStreamTransaction(Op.removeTasks(new RemoveTasks(taskIds)));
      }

      @Override protected void performMutations() {
        logStorage.deleteAllTasks();
      }
    };
  }

  @Test
  public void testRemoveTasksIds() throws Exception {
    final Set<String> taskIds = ImmutableSet.of("42");
    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectTransactions();
        storageUtil.taskStore.deleteTasks(taskIds);
        expectStreamTransaction(Op.removeTasks(new RemoveTasks(taskIds)));
      }

      @Override protected void performMutations() {
        logStorage.deleteTasks(taskIds);
      }
    };
  }

  @Test
  public void testSaveShardUpdateConfigs() throws Exception {
    final String role = "role";
    final String job = "job";
    final String updateToken = "update-ok";
    final ImmutableSet<TaskUpdateConfiguration> updateConfiguration =
        ImmutableSet.of(
            new TaskUpdateConfiguration().setNewConfig(new TwitterTaskInfo().setShardId(42)));
    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectTransactions();
        storageUtil.updateStore.saveJobUpdateConfig(
            new JobUpdateConfiguration(role, job, updateToken, updateConfiguration));
        expectStreamTransaction(
            Op.saveJobUpdate(new SaveJobUpdate(role, job, updateToken, updateConfiguration)));
      }

      @Override protected void performMutations() {
        logStorage.saveJobUpdateConfig(
            new JobUpdateConfiguration(role, job, updateToken, updateConfiguration));
      }
    };
  }

  @Test
  public void testRemoveShardUpdateConfigs() throws Exception {
    final String role = "role";
    final String job = "job";
    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectTransactions();
        storageUtil.updateStore.removeShardUpdateConfigs(role, job);
        expectStreamTransaction(Op.removeJobUpdate(new RemoveJobUpdate(role, job)));
      }

      @Override protected void performMutations() {
        logStorage.removeShardUpdateConfigs(role, job);
      }
    };
  }

  @Test
  public void testSaveQuota() throws Exception {
    final String role = "role";
    final Quota quota = new Quota(1.0, 128L, 1024L);
    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectTransactions();
        storageUtil.quotaStore.saveQuota(role, quota);
        expectStreamTransaction(Op.saveQuota(new SaveQuota(role, quota)));
      }

      @Override protected void performMutations() {
        logStorage.saveQuota(role, quota);
      }
    };
  }

  @Test
  public void testRemoveQuota() throws Exception {
    final String role = "role";
    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectTransactions();
        storageUtil.quotaStore.removeQuota(role);
        expectStreamTransaction(Op.removeQuota(new RemoveQuota(role)));
      }

      @Override protected void performMutations() {
        logStorage.removeQuota(role);
      }
    };
  }

  @Test
  public void testSaveHostAttributes() throws Exception {
    final String host = "hostname";
    final Set<Attribute> attributes =
        ImmutableSet.of(new Attribute().setName("attr").setValues(ImmutableSet.of("value")));
    final Optional<HostAttributes> hostAttributes = Optional.of(new HostAttributes()
        .setHost(host)
        .setAttributes(attributes));

    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectTransactions();
        expect(storageUtil.attributeStore.getHostAttributes(host))
            .andReturn(Optional.<HostAttributes>absent());

        // Each logStorage save invokes get, save, get to the underlying attribute store.
        storageUtil.attributeStore.saveHostAttributes(hostAttributes.get());
        expect(storageUtil.attributeStore.getHostAttributes(host)).andReturn(hostAttributes);
        expectStreamTransaction(
            Op.saveHostAttributes(new SaveHostAttributes(hostAttributes.get())));
        expect(storageUtil.attributeStore.getHostAttributes(host)).andReturn(hostAttributes);

        expect(storageUtil.attributeStore.getHostAttributes(host)).andReturn(hostAttributes);
        storageUtil.attributeStore.saveHostAttributes(hostAttributes.get());
        expect(storageUtil.attributeStore.getHostAttributes(host)).andReturn(hostAttributes);
        expect(storageUtil.attributeStore.getHostAttributes(host)).andReturn(hostAttributes);
      }

      @Override protected void performMutations() {
        logStorage.saveHostAttributes(hostAttributes.get());
        assertEquals(hostAttributes, logStorage.getHostAttributes(host));
        logStorage.saveHostAttributes(hostAttributes.get());
        assertEquals(hostAttributes, logStorage.getHostAttributes(host));
      }
    };
  }

  private void expectStreamTransaction(Op... ops) throws CodingException {
    expect(stream.append(EasyMock.aryEq(ThriftBinaryCodec.encode(createTransaction(ops)))))
        .andReturn(null);
  }

  private LogEntry createTransaction(Op... ops) {
    return LogEntry.transaction(new Transaction(ImmutableList.copyOf(ops)));
  }

  private static ScheduledTask task(String id, ScheduleStatus status) {
    return new ScheduledTask()
        .setStatus(status)
        .setAssignedTask(new AssignedTask()
            .setTaskId(id));
  }
}

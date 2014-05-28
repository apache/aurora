/**
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
package org.apache.aurora.scheduler.storage.log;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.testing.TearDown;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.codec.ThriftBinaryCodec;
import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.storage.LogEntry;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.RemoveJob;
import org.apache.aurora.gen.storage.RemoveLock;
import org.apache.aurora.gen.storage.RemoveQuota;
import org.apache.aurora.gen.storage.RemoveTasks;
import org.apache.aurora.gen.storage.RewriteTask;
import org.apache.aurora.gen.storage.SaveAcceptedJob;
import org.apache.aurora.gen.storage.SaveFrameworkId;
import org.apache.aurora.gen.storage.SaveHostAttributes;
import org.apache.aurora.gen.storage.SaveLock;
import org.apache.aurora.gen.storage.SaveQuota;
import org.apache.aurora.gen.storage.SaveTasks;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.gen.storage.Transaction;
import org.apache.aurora.gen.storage.storageConstants;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.log.Log;
import org.apache.aurora.scheduler.log.Log.Entry;
import org.apache.aurora.scheduler.log.Log.Position;
import org.apache.aurora.scheduler.log.Log.Stream;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.SnapshotStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.ILockKey;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.log.LogStorage.SchedulingService;
import org.apache.aurora.scheduler.storage.log.testing.LogOpMatcher;
import org.apache.aurora.scheduler.storage.log.testing.LogOpMatcher.StreamMatcher;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.easymock.Capture;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

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
  private Position position;
  private StreamMatcher streamMatcher;
  private SchedulingService schedulingService;
  private SnapshotStore<Snapshot> snapshotStore;
  private StorageTestUtil storageUtil;

  @Before
  public void setUp() {
    log = createMock(Log.class);

    LogManager logManager = new LogManager(log, Amount.of(1, Data.GB), false);

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
            storageUtil.lockStore,
            storageUtil.quotaStore,
            storageUtil.attributeStore);

    stream = createMock(Stream.class);
    streamMatcher = LogOpMatcher.matcherFor(stream);
    position = createMock(Position.class);

    storageUtil.storage.prepare();
  }

  @Test
  public void testStart() throws Exception {
    // We should open the log and arrange for its clean shutdown.
    expect(log.open()).andReturn(stream);

    // Our start should recover the log and then forward to the underlying storage start of the
    // supplied initialization logic.
    final AtomicBoolean initialized = new AtomicBoolean(false);
    MutateWork.NoResult.Quiet initializationLogic = new MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(MutableStoreProvider provider) {
        // Creating a mock and expecting apply(storeProvider) does not work here for whatever
        // reason.
        initialized.set(true);
      }
    };

    final Capture<MutateWork.NoResult.Quiet> recoverAndInitializeWork = createCapture();
    storageUtil.storage.write(capture(recoverAndInitializeWork));
    expectLastCall().andAnswer(new IAnswer<Void>() {
      @Override
      public Void answer() throws Throwable {
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
    expect(stream.readAll()).andReturn(Iterators.forArray(entry1, entry2));

    final Capture<MutateWork<Void, RuntimeException>> recoveryWork = createCapture();
    expect(storageUtil.storage.write(capture(recoveryWork))).andAnswer(
        new IAnswer<Void>() {
          @Override
          public Void answer() {
            recoveryWork.getValue().apply(storageUtil.mutableStoreProvider);
            return null;
          }
        });
    storageUtil.schedulerStore.saveFrameworkId(frameworkId1);
    storageUtil.schedulerStore.saveFrameworkId(frameworkId2);

    final Capture<MutateWork<Void, RuntimeException>> initializationWork = createCapture();
    expect(storageUtil.storage.write(capture(initializationWork))).andAnswer(
        new IAnswer<Void>() {
          @Override
          public Void answer() {
            initializationWork.getValue().apply(storageUtil.mutableStoreProvider);
            return null;
          }
        });

    // We should perform a snapshot when the snapshot thread runs.
    Capture<Runnable> snapshotAction = createCapture();
    schedulingService.doEvery(eq(SNAPSHOT_INTERVAL), capture(snapshotAction));
    Snapshot snapshotContents = new Snapshot()
        .setTimestamp(NOW)
        .setTasks(ImmutableSet.of(
            new ScheduledTask()
                .setStatus(ScheduleStatus.RUNNING)
                .setAssignedTask(new AssignedTask().setTaskId("task_id"))));
    expect(snapshotStore.createSnapshot()).andReturn(snapshotContents);
    streamMatcher.expectSnapshot(snapshotContents).andReturn(position);
    stream.truncateBefore(position);
    final Capture<MutateWork<Void, RuntimeException>> snapshotWork = createCapture();
    expect(storageUtil.storage.write(capture(snapshotWork))).andAnswer(
        new IAnswer<Void>() {
          @Override
          public Void answer() {
            snapshotWork.getValue().apply(storageUtil.mutableStoreProvider);
            return null;
          }
        }).times(2);

    control.replay();

    logStorage.prepare();
    logStorage.start(initializationLogic);
    assertTrue(initialized.get());

    // Run the snapshot thread.
    snapshotAction.getValue().run();
  }

  abstract class StorageTestFixture {
    private final AtomicBoolean runCalled = new AtomicBoolean(false);

    StorageTestFixture() {
      // Prevent otherwise silent noop tests that forget to call run().
      addTearDown(new TearDown() {
        @Override
        public void tearDown() {
          assertTrue(runCalled.get());
        }
      });
    }

    void run() throws Exception {
      runCalled.set(true);

      // Expect basic start operations.

      // Open the log stream.
      expect(log.open()).andReturn(stream);

      // Replay the log and perform and supplied initializationWork.
      // Simulate NOOP initialization work
      // Creating a mock and expecting apply(storeProvider) does not work here for whatever
      // reason.
      MutateWork.NoResult.Quiet initializationLogic = new NoResult.Quiet() {
        @Override
        protected void execute(Storage.MutableStoreProvider storeProvider) {
          // No-op.
        }
      };

      final Capture<MutateWork.NoResult.Quiet> recoverAndInitializeWork = createCapture();
      storageUtil.storage.write(capture(recoverAndInitializeWork));
      expectLastCall().andAnswer(new IAnswer<Void>() {
        @Override
        public Void answer() throws Throwable {
          recoverAndInitializeWork.getValue().apply(storageUtil.mutableStoreProvider);
          return null;
        }
      });

      expect(stream.readAll()).andReturn(Iterators.<Entry>emptyIterator());
      final Capture<MutateWork<Void, RuntimeException>> recoveryWork = createCapture();
      expect(storageUtil.storage.write(capture(recoveryWork))).andAnswer(
          new IAnswer<Void>() {
            @Override
            public Void answer() {
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
      runTest();
    }

    protected void setupExpectations() throws Exception {
      // Default to no expectations.
    }

    protected abstract void runTest();
  }

  abstract class MutationFixture extends StorageTestFixture {
    @Override
    protected void runTest() {
      logStorage.write(new MutateWork.NoResult.Quiet() {
        @Override
        protected void execute(MutableStoreProvider storeProvider) {
          performMutations(storeProvider);
        }
      });
    }

    protected abstract void performMutations(MutableStoreProvider storeProvider);
  }

  @Test
  public void testSaveFrameworkId() throws Exception {
    final String frameworkId = "bob";
    new MutationFixture() {
      @Override
      protected void setupExpectations() throws CodingException {
        storageUtil.expectWriteOperation();
        storageUtil.schedulerStore.saveFrameworkId(frameworkId);
        streamMatcher.expectTransaction(Op.saveFrameworkId(new SaveFrameworkId(frameworkId)))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getSchedulerStore().saveFrameworkId(frameworkId);
      }
    }.run();
  }

  @Test
  public void testSaveAcceptedJob() throws Exception {
    final IJobConfiguration jobConfig =
        IJobConfiguration.build(new JobConfiguration().setKey(new JobKey("owner", "env", "jake")));
    final String managerId = "CRON";
    new MutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWriteOperation();
        storageUtil.jobStore.saveAcceptedJob(managerId, jobConfig);
        streamMatcher.expectTransaction(
            Op.saveAcceptedJob(new SaveAcceptedJob(managerId, jobConfig.newBuilder())))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getJobStore().saveAcceptedJob(managerId, jobConfig);
      }
    }.run();
  }

  @Test
  public void testRemoveJob() throws Exception {
    final IJobKey jobKey = JobKeys.from("role", "env", "name");
    new MutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWriteOperation();
        storageUtil.jobStore.removeJob(jobKey);
        streamMatcher.expectTransaction(
            Op.removeJob(new RemoveJob().setJobKey(jobKey.newBuilder())))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getJobStore().removeJob(jobKey);
      }
    }.run();
  }

  @Test
  public void testSaveTasks() throws Exception {
    final Set<IScheduledTask> tasks = ImmutableSet.of(task("a", ScheduleStatus.INIT));
    new MutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWriteOperation();
        storageUtil.taskStore.saveTasks(tasks);
        streamMatcher.expectTransaction(
            Op.saveTasks(new SaveTasks(IScheduledTask.toBuildersSet(tasks))))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(tasks);
      }
    }.run();
  }

  @Test
  public void testMutateTasks() throws Exception {
    final Query.Builder query = Query.taskScoped("fred");
    final Function<IScheduledTask, IScheduledTask> mutation = Functions.identity();
    final ImmutableSet<IScheduledTask> mutated =
        ImmutableSet.of(task("a", ScheduleStatus.STARTING));
    new MutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWriteOperation();
        expect(storageUtil.taskStore.mutateTasks(query, mutation)).andReturn(mutated);
        streamMatcher.expectTransaction(
            Op.saveTasks(new SaveTasks(IScheduledTask.toBuildersSet(mutated))))
            .andReturn(null);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        assertEquals(mutated, storeProvider.getUnsafeTaskStore().mutateTasks(query, mutation));
      }
    }.run();
  }

  @Test
  public void testUnsafeModifyInPlace() throws Exception {
    final String taskId = "wilma";
    final String taskId2 = "barney";
    final ITaskConfig updatedConfig =
        task(taskId, ScheduleStatus.RUNNING).getAssignedTask().getTask();
    new MutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWriteOperation();
        expect(storageUtil.taskStore.unsafeModifyInPlace(taskId2, updatedConfig)).andReturn(false);
        expect(storageUtil.taskStore.unsafeModifyInPlace(taskId, updatedConfig)).andReturn(true);
        streamMatcher.expectTransaction(
            Op.rewriteTask(new RewriteTask(taskId, updatedConfig.newBuilder())))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().unsafeModifyInPlace(taskId2, updatedConfig);
        storeProvider.getUnsafeTaskStore().unsafeModifyInPlace(taskId, updatedConfig);
      }
    }.run();
  }

  @Test
  public void testNestedTransactions() throws Exception {
    final Query.Builder query = Query.taskScoped("fred");
    final Function<IScheduledTask, IScheduledTask> mutation = Functions.identity();
    final ImmutableSet<IScheduledTask> mutated =
        ImmutableSet.of(task("a", ScheduleStatus.STARTING));
    final ImmutableSet<String> tasksToRemove = ImmutableSet.of("b");

    new MutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWriteOperation();
        expect(storageUtil.taskStore.mutateTasks(query, mutation)).andReturn(mutated);

        storageUtil.taskStore.deleteTasks(tasksToRemove);

        streamMatcher.expectTransaction(
            Op.saveTasks(new SaveTasks(IScheduledTask.toBuildersSet(mutated))),
            Op.removeTasks(new RemoveTasks(tasksToRemove)))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        assertEquals(mutated, storeProvider.getUnsafeTaskStore().mutateTasks(query, mutation));

        logStorage.write(new MutateWork.NoResult.Quiet() {
          @Override
          protected void execute(MutableStoreProvider innerProvider) {
            innerProvider.getUnsafeTaskStore().deleteTasks(tasksToRemove);
          }
        });
      }
    }.run();
  }

  @Test
  public void testSaveAndMutateTasks() throws Exception {
    final Query.Builder query = Query.taskScoped("fred");
    final Function<IScheduledTask, IScheduledTask> mutation = Functions.identity();
    final Set<IScheduledTask> saved = ImmutableSet.of(task("a", ScheduleStatus.INIT));
    final ImmutableSet<IScheduledTask> mutated = ImmutableSet.of(task("a", ScheduleStatus.PENDING));

    new MutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWriteOperation();
        storageUtil.taskStore.saveTasks(saved);

        // Nested transaction with result.
        expect(storageUtil.taskStore.mutateTasks(query, mutation)).andReturn(mutated);

        // Resulting stream operation.
        streamMatcher.expectTransaction(Op.saveTasks(
            new SaveTasks(IScheduledTask.toBuildersSet(mutated))))
            .andReturn(null);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(saved);
        assertEquals(mutated, storeProvider.getUnsafeTaskStore().mutateTasks(query, mutation));
      }
    }.run();
  }

  @Test
  public void testSaveAndMutateTasksNoCoalesceUniqueIds() throws Exception {
    final Query.Builder query = Query.taskScoped("fred");
    final Function<IScheduledTask, IScheduledTask> mutation = Functions.identity();
    final Set<IScheduledTask> saved = ImmutableSet.of(task("b", ScheduleStatus.INIT));
    final ImmutableSet<IScheduledTask> mutated = ImmutableSet.of(task("a", ScheduleStatus.PENDING));

    new MutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWriteOperation();
        storageUtil.taskStore.saveTasks(saved);

        // Nested transaction with result.
        expect(storageUtil.taskStore.mutateTasks(query, mutation)).andReturn(mutated);

        // Resulting stream operation.
        streamMatcher.expectTransaction(
            Op.saveTasks(new SaveTasks(
                ImmutableSet.<ScheduledTask>builder()
                    .addAll(IScheduledTask.toBuildersList(saved))
                    .addAll(IScheduledTask.toBuildersList(mutated))
                    .build())))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(saved);
        assertEquals(mutated, storeProvider.getUnsafeTaskStore().mutateTasks(query, mutation));
      }
    }.run();
  }

  @Test
  public void testRemoveTasksQuery() throws Exception {
    final IScheduledTask task = task("a", ScheduleStatus.FINISHED);
    final Set<String> taskIds = Tasks.ids(task);
    new MutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWriteOperation();
        storageUtil.taskStore.deleteTasks(taskIds);
        streamMatcher.expectTransaction(Op.removeTasks(new RemoveTasks(taskIds)))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().deleteTasks(taskIds);
      }
    }.run();
  }

  @Test
  public void testRemoveTasksIds() throws Exception {
    final Set<String> taskIds = ImmutableSet.of("42");
    new MutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWriteOperation();
        storageUtil.taskStore.deleteTasks(taskIds);
        streamMatcher.expectTransaction(Op.removeTasks(new RemoveTasks(taskIds)))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().deleteTasks(taskIds);
      }
    }.run();
  }

  @Test
  public void testSaveQuota() throws Exception {
    final String role = "role";
    final IResourceAggregate quota =
        IResourceAggregate.build(new ResourceAggregate(1.0, 128L, 1024L));

    new MutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWriteOperation();
        storageUtil.quotaStore.saveQuota(role, quota);
        streamMatcher.expectTransaction(Op.saveQuota(new SaveQuota(role, quota.newBuilder())))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getQuotaStore().saveQuota(role, quota);
      }
    }.run();
  }

  @Test
  public void testRemoveQuota() throws Exception {
    final String role = "role";
    new MutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWriteOperation();
        storageUtil.quotaStore.removeQuota(role);
        streamMatcher.expectTransaction(Op.removeQuota(new RemoveQuota(role))).andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getQuotaStore().removeQuota(role);
      }
    }.run();
  }

  @Test
  public void testSaveLock() throws Exception {
    final ILock lock = ILock.build(new Lock()
        .setKey(LockKey.job(JobKeys.from("testRole", "testEnv", "testJob").newBuilder()))
        .setToken("testLockId")
        .setUser("testUser")
        .setTimestampMs(12345L));
    new MutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWriteOperation();
        storageUtil.lockStore.saveLock(lock);
        streamMatcher.expectTransaction(Op.saveLock(new SaveLock(lock.newBuilder())))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getLockStore().saveLock(lock);
      }
    }.run();
  }

  @Test
  public void testRemoveLock() throws Exception {
    final ILockKey lockKey =
        ILockKey.build(LockKey.job(JobKeys.from("testRole", "testEnv", "testJob").newBuilder()));
    new MutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWriteOperation();
        storageUtil.lockStore.removeLock(lockKey);
        streamMatcher.expectTransaction(Op.removeLock(new RemoveLock(lockKey.newBuilder())))
            .andReturn(position);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getLockStore().removeLock(lockKey);
      }
    }.run();
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
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWriteOperation();
        expect(storageUtil.attributeStore.getHostAttributes(host))
            .andReturn(Optional.<HostAttributes>absent());

        // Each logStorage save invokes get, save, get to the underlying attribute store.
        storageUtil.attributeStore.saveHostAttributes(hostAttributes.get());
        expect(storageUtil.attributeStore.getHostAttributes(host)).andReturn(hostAttributes);
        streamMatcher.expectTransaction(
            Op.saveHostAttributes(new SaveHostAttributes(hostAttributes.get())))
            .andReturn(position);
        expect(storageUtil.attributeStore.getHostAttributes(host)).andReturn(hostAttributes);

        expect(storageUtil.attributeStore.getHostAttributes(host)).andReturn(hostAttributes);
        storageUtil.attributeStore.saveHostAttributes(hostAttributes.get());
        expect(storageUtil.attributeStore.getHostAttributes(host)).andReturn(hostAttributes);
        expect(storageUtil.attributeStore.getHostAttributes(host)).andReturn(hostAttributes);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        AttributeStore.Mutable store = storeProvider.getAttributeStore();
        store.saveHostAttributes(hostAttributes.get());
        assertEquals(hostAttributes, store.getHostAttributes(host));
        store.saveHostAttributes(hostAttributes.get());
        assertEquals(hostAttributes, store.getHostAttributes(host));
      }
    }.run();
  }

  private LogEntry createTransaction(Op... ops) {
    return LogEntry.transaction(
        new Transaction(ImmutableList.copyOf(ops), storageConstants.CURRENT_SCHEMA_VERSION));
  }

  private static IScheduledTask task(String id, ScheduleStatus status) {
    return IScheduledTask.build(new ScheduledTask()
        .setStatus(status)
        .setAssignedTask(new AssignedTask()
            .setTaskId(id)
            .setTask(new TaskConfig())));
  }
}

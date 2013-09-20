/*
 * Copyright 2013 Twitter, Inc.
 *
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
package com.twitter.aurora.scheduler.storage.log;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.testing.TearDown;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.codec.ThriftBinaryCodec;
import com.twitter.aurora.codec.ThriftBinaryCodec.CodingException;
import com.twitter.aurora.gen.AssignedTask;
import com.twitter.aurora.gen.Attribute;
import com.twitter.aurora.gen.HostAttributes;
import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.JobKey;
import com.twitter.aurora.gen.JobUpdateConfiguration;
import com.twitter.aurora.gen.Quota;
import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.gen.TaskUpdateConfiguration;
import com.twitter.aurora.gen.storage.LogEntry;
import com.twitter.aurora.gen.storage.Op;
import com.twitter.aurora.gen.storage.RemoveJob;
import com.twitter.aurora.gen.storage.RemoveJobUpdate;
import com.twitter.aurora.gen.storage.RemoveQuota;
import com.twitter.aurora.gen.storage.RemoveTasks;
import com.twitter.aurora.gen.storage.RewriteTask;
import com.twitter.aurora.gen.storage.SaveAcceptedJob;
import com.twitter.aurora.gen.storage.SaveFrameworkId;
import com.twitter.aurora.gen.storage.SaveHostAttributes;
import com.twitter.aurora.gen.storage.SaveJobUpdate;
import com.twitter.aurora.gen.storage.SaveQuota;
import com.twitter.aurora.gen.storage.SaveTasks;
import com.twitter.aurora.gen.storage.Snapshot;
import com.twitter.aurora.gen.storage.Transaction;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.log.Log;
import com.twitter.aurora.scheduler.log.Log.Entry;
import com.twitter.aurora.scheduler.log.Log.Position;
import com.twitter.aurora.scheduler.log.Log.Stream;
import com.twitter.aurora.scheduler.storage.SnapshotStore;
import com.twitter.aurora.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.aurora.scheduler.storage.Storage.MutateWork;
import com.twitter.aurora.scheduler.storage.log.LogStorage.SchedulingService;
import com.twitter.aurora.scheduler.storage.log.testing.LogOpMatcher;
import com.twitter.aurora.scheduler.storage.log.testing.LogOpMatcher.StreamMatcher;
import com.twitter.aurora.scheduler.storage.testing.StorageTestUtil;
import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.base.Closure;
import com.twitter.common.base.Closures;
import com.twitter.common.base.Command;
import com.twitter.common.base.ExceptionalCommand;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;

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
  private ShutdownRegistry shutdownRegistry;
  private SchedulingService schedulingService;
  private SnapshotStore<Snapshot> snapshotStore;
  private StorageTestUtil storageUtil;

  @Before
  public void setUp() {
    log = createMock(Log.class);

    shutdownRegistry = createMock(ShutdownRegistry.class);
    LogManager logManager = new LogManager(log, Amount.of(1, Data.GB), false, shutdownRegistry);

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
    streamMatcher = LogOpMatcher.matcherFor(stream);
    position = createMock(Position.class);
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
    storageUtil.storage.write(capture(recoverAndInitializeWork));
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
    expect(storageUtil.storage.write(capture(recoveryWork))).andAnswer(
        new IAnswer<Void>() {
          @Override public Void answer() {
            recoveryWork.getValue().apply(storageUtil.mutableStoreProvider);
            return null;
          }
        });
    storageUtil.schedulerStore.saveFrameworkId(frameworkId1);
    storageUtil.schedulerStore.saveFrameworkId(frameworkId2);

    final Capture<MutateWork<Void, RuntimeException>> initializationWork = createCapture();
    expect(storageUtil.storage.write(capture(initializationWork))).andAnswer(
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
          @Override public Void answer() {
            snapshotWork.getValue().apply(storageUtil.mutableStoreProvider);
            return null;
          }
        }).times(2);
    storageUtil.storage.snapshot();

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
    private final AtomicBoolean runCalled = new AtomicBoolean(false);

    MutationFixture() {
      // Prevent otherwise silent noop tests that forget to call run().
      addTearDown(new TearDown() {
        @Override public void tearDown() {
          assertTrue(runCalled.get());
        }
      });
    }

    void run() throws Exception {
      runCalled.set(true);

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
      storageUtil.storage.write(capture(recoverAndInitializeWork));
      expectLastCall().andAnswer(new IAnswer<Void>() {
        @Override public Void answer() throws Throwable {
          recoverAndInitializeWork.getValue().apply(storageUtil.mutableStoreProvider);
          return null;
        }
      });

      expect(stream.readAll()).andReturn(Iterators.<Entry>emptyIterator());
      final Capture<MutateWork<Void, RuntimeException>> recoveryWork = createCapture();
      expect(storageUtil.storage.write(capture(recoveryWork))).andAnswer(
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
        storageUtil.expectOperations();
        storageUtil.schedulerStore.saveFrameworkId(frameworkId);
        streamMatcher.expectTransaction(Op.saveFrameworkId(new SaveFrameworkId(frameworkId)))
            .andReturn(position);
      }

      @Override protected void performMutations() {
        logStorage.saveFrameworkId(frameworkId);
      }
    }.run();
  }

  @Test
  public void testSaveAcceptedJob() throws Exception {
    final JobConfiguration jobConfig = new JobConfiguration().setName("jake");
    final String managerId = "CRON";
    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectOperations();
        storageUtil.jobStore.saveAcceptedJob(managerId, jobConfig);
        streamMatcher.expectTransaction(
            Op.saveAcceptedJob(new SaveAcceptedJob(managerId, jobConfig)))
            .andReturn(position);
      }

      @Override protected void performMutations() {
        logStorage.saveAcceptedJob(managerId, jobConfig);
      }
    }.run();
  }

  @Test
  public void testRemoveJob() throws Exception {
    final JobKey jobKey = JobKeys.from("role", "env", "name");
    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectOperations();
        storageUtil.jobStore.removeJob(jobKey);
        streamMatcher.expectTransaction(Op.removeJob(new RemoveJob().setJobKey(jobKey)))
            .andReturn(position);
      }

      @Override protected void performMutations() {
        logStorage.removeJob(jobKey);
      }
    }.run();
  }

  @Test
  public void testSaveTasks() throws Exception {
    final Set<ScheduledTask> tasks = ImmutableSet.of(task("a", ScheduleStatus.INIT));
    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectOperations();
        storageUtil.taskStore.saveTasks(tasks);
        streamMatcher.expectTransaction(Op.saveTasks(new SaveTasks(tasks))).andReturn(position);
      }

      @Override protected void performMutations() {
        logStorage.saveTasks(tasks);
      }
    }.run();
  }

  @Test
  public void testMutateTasks() throws Exception {
    final Query.Builder query = Query.taskScoped("fred");
    final Closure<ScheduledTask> mutation = Closures.noop();
    final ImmutableSet<ScheduledTask> mutated = ImmutableSet.of(task("a", ScheduleStatus.STARTING));
    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectOperations();
        expect(storageUtil.taskStore.mutateTasks(query, mutation)).andReturn(mutated);
        streamMatcher.expectTransaction(Op.saveTasks(new SaveTasks(mutated))).andReturn(null);
      }

      @Override protected void performMutations() {
        assertEquals(mutated, logStorage.mutateTasks(query, mutation));
      }
    }.run();
  }

  @Test
  public void testUnsafeModifyInPlace() throws Exception {
    final String taskId = "wilma";
    final String taskId2 = "barney";
    final TaskConfig updatedConfig =
        task(taskId, ScheduleStatus.RUNNING).getAssignedTask().getTask();
    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectOperations();
        expect(storageUtil.taskStore.unsafeModifyInPlace(taskId2, updatedConfig)).andReturn(false);
        expect(storageUtil.taskStore.unsafeModifyInPlace(taskId, updatedConfig)).andReturn(true);
        streamMatcher.expectTransaction(Op.rewriteTask(new RewriteTask(taskId, updatedConfig)))
            .andReturn(position);
      }

      @Override protected void performMutations() {
        logStorage.unsafeModifyInPlace(taskId2, updatedConfig);
        logStorage.unsafeModifyInPlace(taskId, updatedConfig);
      }
    }.run();
  }

  @Test
  public void testNestedTransactions() throws Exception {
    final Query.Builder query = Query.taskScoped("fred");
    final Closure<ScheduledTask> mutation = Closures.noop();
    final ImmutableSet<ScheduledTask> mutated =
        ImmutableSet.of(task("a", ScheduleStatus.STARTING));
    final ImmutableSet<String> tasksToRemove = ImmutableSet.of("b");

    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectOperations();
        expect(storageUtil.taskStore.mutateTasks(query, mutation)).andReturn(mutated);

        storageUtil.taskStore.deleteTasks(tasksToRemove);

        streamMatcher.expectTransaction(
            Op.saveTasks(new SaveTasks(mutated)),
            Op.removeTasks(new RemoveTasks(tasksToRemove)))
            .andReturn(position);
      }

      @Override protected void performMutations() {
        logStorage.write(new MutateWork.NoResult.Quiet() {
          @Override protected void execute(MutableStoreProvider provider) {
            assertEquals(mutated, provider.getUnsafeTaskStore().mutateTasks(query, mutation));
            logStorage.write(new MutateWork.NoResult.Quiet() {
              @Override protected void execute(MutableStoreProvider innerProvider) {
                innerProvider.getUnsafeTaskStore().deleteTasks(tasksToRemove);
              }
            });
          }
        });
      }
    }.run();
  }

  @Test
  public void testSaveAndMutateTasks() throws Exception {
    final Query.Builder query = Query.taskScoped("fred");
    final Closure<ScheduledTask> mutation = Closures.noop();
    final Set<ScheduledTask> saved = ImmutableSet.of(task("a", ScheduleStatus.INIT));
    final ImmutableSet<ScheduledTask> mutated = ImmutableSet.of(task("a", ScheduleStatus.PENDING));

    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectOperations();
        storageUtil.taskStore.saveTasks(saved);

        // Nested transaction with result.
        expect(storageUtil.taskStore.mutateTasks(query, mutation)).andReturn(mutated);

        // Resulting stream operation.
        streamMatcher.expectTransaction(Op.saveTasks(new SaveTasks(mutated))).andReturn(null);
      }

      @Override protected void performMutations() {
        logStorage.write(new MutateWork.NoResult.Quiet() {
          @Override protected void execute(MutableStoreProvider provider) {
            logStorage.saveTasks(saved);
            assertEquals(mutated, logStorage.mutateTasks(query, mutation));
          }
        });
      }
    }.run();
  }

  @Test
  public void testSaveAndMutateTasksNoCoalesceUniqueIds() throws Exception {
    final Query.Builder query = Query.taskScoped("fred");
    final Closure<ScheduledTask> mutation = Closures.noop();
    final Set<ScheduledTask> saved = ImmutableSet.of(task("b", ScheduleStatus.INIT));
    final ImmutableSet<ScheduledTask> mutated = ImmutableSet.of(task("a", ScheduleStatus.PENDING));

    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectOperations();
        storageUtil.taskStore.saveTasks(saved);

        // Nested transaction with result.
        expect(storageUtil.taskStore.mutateTasks(query, mutation)).andReturn(mutated);

        // Resulting stream operation.
        streamMatcher.expectTransaction(
            Op.saveTasks(new SaveTasks(
                ImmutableSet.<ScheduledTask>builder().addAll(saved).addAll(mutated).build())))
            .andReturn(position);
      }

      @Override protected void performMutations() {
        logStorage.write(new MutateWork.NoResult.Quiet() {
          @Override protected void execute(MutableStoreProvider provider) {
            logStorage.saveTasks(saved);
            assertEquals(mutated, logStorage.mutateTasks(query, mutation));
          }
        });
      }
    }.run();
  }

  @Test
  public void testRemoveTasksQuery() throws Exception {
    final ScheduledTask task = task("a", ScheduleStatus.FINISHED);
    final Set<String> taskIds = Tasks.ids(task);
    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectOperations();
        expect(storageUtil.taskStore.fetchTasks(Query.unscoped()))
            .andReturn(ImmutableSet.<ScheduledTask>of(task));
        storageUtil.taskStore.deleteTasks(taskIds);
        streamMatcher.expectTransaction(Op.removeTasks(new RemoveTasks(taskIds)))
            .andReturn(position);
      }

      @Override protected void performMutations() {
        logStorage.deleteAllTasks();
      }
    }.run();
  }

  @Test
  public void testRemoveTasksIds() throws Exception {
    final Set<String> taskIds = ImmutableSet.of("42");
    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectOperations();
        storageUtil.taskStore.deleteTasks(taskIds);
        streamMatcher.expectTransaction(Op.removeTasks(new RemoveTasks(taskIds)))
            .andReturn(position);
      }

      @Override protected void performMutations() {
        logStorage.deleteTasks(taskIds);
      }
    }.run();
  }

  @Test
  public void testSaveShardUpdateConfigs() throws Exception {
    final JobKey jobKey = JobKeys.from("role", "env", "job");
    final String updateToken = "update-ok";
    final ImmutableSet<TaskUpdateConfiguration> updateConfiguration =
        ImmutableSet.of(
            new TaskUpdateConfiguration().setNewConfig(new TaskConfig().setShardId(42)));
    final JobUpdateConfiguration config =
        new JobUpdateConfiguration(jobKey, updateToken, updateConfiguration);
    final SaveJobUpdate saveOp = new SaveJobUpdate(jobKey, updateToken, updateConfiguration);

    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectOperations();
        storageUtil.updateStore.saveJobUpdateConfig(config);
        streamMatcher.expectTransaction(Op.saveJobUpdate(saveOp)).andReturn(position);
      }

      @Override protected void performMutations() {
        logStorage.saveJobUpdateConfig(config);
      }
    }.run();
  }

  @Test
  public void testRemoveShardUpdateConfigs() throws Exception {
    final JobKey jobKey = JobKeys.from("role", "env", "job");
    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectOperations();
        storageUtil.updateStore.removeShardUpdateConfigs(jobKey);
        streamMatcher.expectTransaction(Op.removeJobUpdate(new RemoveJobUpdate(jobKey)))
            .andReturn(position);
      }

      @Override protected void performMutations() {
        logStorage.removeShardUpdateConfigs(jobKey);
      }
    }.run();
  }

  @Test
  public void testSaveQuota() throws Exception {
    final String role = "role";
    final Quota quota = new Quota(1.0, 128L, 1024L);
    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectOperations();
        storageUtil.quotaStore.saveQuota(role, quota);
        streamMatcher.expectTransaction(Op.saveQuota(new SaveQuota(role, quota)))
            .andReturn(position);
      }

      @Override protected void performMutations() {
        logStorage.saveQuota(role, quota);
      }
    }.run();
  }

  @Test
  public void testRemoveQuota() throws Exception {
    final String role = "role";
    new MutationFixture() {
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectOperations();
        storageUtil.quotaStore.removeQuota(role);
        streamMatcher.expectTransaction(Op.removeQuota(new RemoveQuota(role))).andReturn(position);
      }

      @Override protected void performMutations() {
        logStorage.removeQuota(role);
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
      @Override protected void setupExpectations() throws Exception {
        storageUtil.expectOperations();
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

      @Override protected void performMutations() {
        logStorage.saveHostAttributes(hostAttributes.get());
        assertEquals(hostAttributes, logStorage.getHostAttributes(host));
        logStorage.saveHostAttributes(hostAttributes.get());
        assertEquals(hostAttributes, logStorage.getHostAttributes(host));
      }
    }.run();
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

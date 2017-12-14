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
package org.apache.aurora.scheduler.storage.durability;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateAction;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.PruneJobUpdateHistory;
import org.apache.aurora.gen.storage.RemoveJob;
import org.apache.aurora.gen.storage.RemoveJobUpdates;
import org.apache.aurora.gen.storage.RemoveLock;
import org.apache.aurora.gen.storage.RemoveQuota;
import org.apache.aurora.gen.storage.RemoveTasks;
import org.apache.aurora.gen.storage.SaveCronJob;
import org.apache.aurora.gen.storage.SaveFrameworkId;
import org.apache.aurora.gen.storage.SaveHostAttributes;
import org.apache.aurora.gen.storage.SaveJobInstanceUpdateEvent;
import org.apache.aurora.gen.storage.SaveJobUpdate;
import org.apache.aurora.gen.storage.SaveJobUpdateEvent;
import org.apache.aurora.gen.storage.SaveLock;
import org.apache.aurora.gen.storage.SaveQuota;
import org.apache.aurora.gen.storage.SaveTasks;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.resources.ResourceTestUtil;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult.Quiet;
import org.apache.aurora.scheduler.storage.durability.Persistence.Edit;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.Resource.diskMb;
import static org.apache.aurora.gen.Resource.numCpus;
import static org.apache.aurora.gen.Resource.ramMb;
import static org.apache.aurora.scheduler.base.TaskTestUtil.makeConfig;
import static org.apache.aurora.scheduler.base.TaskTestUtil.makeTask;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DurableStorageTest extends EasyMockTest {

  private static final IJobKey JOB_KEY = JobKeys.from("role", "env", "name");
  private static final IJobUpdateKey UPDATE_ID =
      IJobUpdateKey.build(new JobUpdateKey(JOB_KEY.newBuilder(), "testUpdateId"));

  private DurableStorage durableStorage;
  private Persistence persistence;
  private StorageTestUtil storageUtil;
  private EventSink eventSink;

  @Before
  public void setUp() {
    persistence = createMock(Persistence.class);
    storageUtil = new StorageTestUtil(this);
    eventSink = createMock(EventSink.class);

    durableStorage = new DurableStorage(
        persistence,
        storageUtil.storage,
        storageUtil.schedulerStore,
        storageUtil.jobStore,
        storageUtil.taskStore,
        storageUtil.quotaStore,
        storageUtil.attributeStore,
        storageUtil.jobUpdateStore,
        eventSink,
        new ReentrantLock(),
        TaskTestUtil.THRIFT_BACKFILL);

    storageUtil.storage.prepare();
  }

  @Test
  public void testStart() throws Exception {
    // We should initialize persistence.
    persistence.prepare();

    // Our start should recover persistence and then forward to the underlying storage start of the
    // supplied initialization logic.
    AtomicBoolean initialized = new AtomicBoolean(false);
    MutateWork.NoResult.Quiet initializationLogic = provider -> {
      // Creating a mock and expecting apply(storeProvider) does not work here for whatever
      // reason.
      initialized.set(true);
    };

    Capture<MutateWork.NoResult.Quiet> recoverAndInitializeWork = createCapture();
    storageUtil.storage.write(capture(recoverAndInitializeWork));
    expectLastCall().andAnswer(() -> {
      recoverAndInitializeWork.getValue().apply(storageUtil.mutableStoreProvider);
      return null;
    });

    Capture<MutateWork<Void, RuntimeException>> initializationWork = createCapture();
    expect(storageUtil.storage.write(capture(initializationWork))).andAnswer(
        () -> {
          initializationWork.getValue().apply(storageUtil.mutableStoreProvider);
          return null;
        });

    // Populate all Op types.
    buildReplayOps();

    storageUtil.expectStoreAccesses();

    control.replay();

    durableStorage.prepare();
    durableStorage.start(initializationLogic);
    assertTrue(initialized.get());
  }

  private void buildReplayOps() throws Exception {
    ImmutableSet.Builder<Edit> builder = ImmutableSet.builder();

    builder.add(Edit.op(Op.saveFrameworkId(new SaveFrameworkId("bob"))));
    storageUtil.schedulerStore.saveFrameworkId("bob");

    JobConfiguration actualJob = new JobConfiguration().setTaskConfig(nonBackfilledConfig());
    JobConfiguration expectedJob =
        new JobConfiguration().setTaskConfig(makeConfig(JOB_KEY).newBuilder());
    SaveCronJob cronJob = new SaveCronJob().setJobConfig(actualJob);
    builder.add(Edit.op(Op.saveCronJob(cronJob)));
    storageUtil.jobStore.saveAcceptedJob(IJobConfiguration.build(expectedJob));

    RemoveJob removeJob = new RemoveJob(JOB_KEY.newBuilder());
    builder.add(Edit.op(Op.removeJob(removeJob)));
    storageUtil.jobStore.removeJob(JOB_KEY);

    ScheduledTask actualTask = makeTask("id", JOB_KEY).newBuilder();
    actualTask.getAssignedTask().setTask(nonBackfilledConfig());
    IScheduledTask expectedTask = makeTask("id", JOB_KEY);
    SaveTasks saveTasks = new SaveTasks(ImmutableSet.of(actualTask));
    builder.add(Edit.op(Op.saveTasks(saveTasks)));
    storageUtil.taskStore.saveTasks(ImmutableSet.of(expectedTask));

    // Side-effects from a storage reset, caused by a snapshot.
    builder.add(Edit.deleteAll());
    storageUtil.jobStore.deleteJobs();
    storageUtil.taskStore.deleteAllTasks();
    storageUtil.quotaStore.deleteQuotas();
    storageUtil.attributeStore.deleteHostAttributes();
    storageUtil.jobUpdateStore.deleteAllUpdates();

    RemoveTasks removeTasks = new RemoveTasks(ImmutableSet.of("taskId1"));
    builder.add(Edit.op(Op.removeTasks(removeTasks)));
    storageUtil.taskStore.deleteTasks(removeTasks.getTaskIds());

    ResourceAggregate nonBackfilled = new ResourceAggregate()
        .setNumCpus(1.0)
        .setRamMb(32)
        .setDiskMb(64);
    SaveQuota saveQuota = new SaveQuota(JOB_KEY.getRole(), nonBackfilled);
    builder.add(Edit.op(Op.saveQuota(saveQuota)));
    storageUtil.quotaStore.saveQuota(
        saveQuota.getRole(),
        IResourceAggregate.build(nonBackfilled.deepCopy()
            .setResources(ImmutableSet.of(numCpus(1.0), ramMb(32), diskMb(64)))));

    builder.add(Edit.op(Op.removeQuota(new RemoveQuota(JOB_KEY.getRole()))));
    storageUtil.quotaStore.removeQuota(JOB_KEY.getRole());

    // This entry lacks a slave ID, and should therefore be discarded.
    SaveHostAttributes hostAttributes1 = new SaveHostAttributes(new HostAttributes()
        .setHost("host1")
        .setMode(MaintenanceMode.DRAINED));
    builder.add(Edit.op(Op.saveHostAttributes(hostAttributes1)));

    SaveHostAttributes hostAttributes2 = new SaveHostAttributes(new HostAttributes()
        .setHost("host2")
        .setSlaveId("slave2")
        .setMode(MaintenanceMode.DRAINED));
    builder.add(Edit.op(Op.saveHostAttributes(hostAttributes2)));
    expect(storageUtil.attributeStore.saveHostAttributes(
        IHostAttributes.build(hostAttributes2.getHostAttributes()))).andReturn(true);

    builder.add(Edit.op(Op.saveLock(new SaveLock())));
    // TODO(jly): Deprecated, this is a no-op to be removed in 0.21. See AURORA-1959.

    builder.add(Edit.op(Op.removeLock(new RemoveLock())));
    // TODO(jly): Deprecated, this is a no-op to be removed in 0.21. See AURORA-1959.

    JobUpdate actualUpdate = new JobUpdate()
        .setSummary(new JobUpdateSummary().setKey(UPDATE_ID.newBuilder()))
        .setInstructions(new JobUpdateInstructions()
            .setInitialState(
                ImmutableSet.of(new InstanceTaskConfig().setTask(nonBackfilledConfig())))
            .setDesiredState(new InstanceTaskConfig().setTask(nonBackfilledConfig())));
    JobUpdate expectedUpdate = actualUpdate.deepCopy();
    expectedUpdate.getInstructions().getDesiredState().setTask(makeConfig(JOB_KEY).newBuilder());
    expectedUpdate.getInstructions().getInitialState()
        .forEach(e -> e.setTask(makeConfig(JOB_KEY).newBuilder()));
    SaveJobUpdate saveUpdate = new SaveJobUpdate().setJobUpdate(actualUpdate);
    builder.add(Edit.op(Op.saveJobUpdate(saveUpdate)));
    storageUtil.jobUpdateStore.saveJobUpdate(IJobUpdate.build(expectedUpdate));

    SaveJobUpdateEvent saveUpdateEvent =
        new SaveJobUpdateEvent(new JobUpdateEvent(), UPDATE_ID.newBuilder());
    builder.add(Edit.op(Op.saveJobUpdateEvent(saveUpdateEvent)));
    storageUtil.jobUpdateStore.saveJobUpdateEvent(
        UPDATE_ID,
        IJobUpdateEvent.build(saveUpdateEvent.getEvent()));

    SaveJobInstanceUpdateEvent saveInstanceEvent = new SaveJobInstanceUpdateEvent(
        new JobInstanceUpdateEvent(),
        UPDATE_ID.newBuilder());
    builder.add(Edit.op(Op.saveJobInstanceUpdateEvent(saveInstanceEvent)));
    storageUtil.jobUpdateStore.saveJobInstanceUpdateEvent(
        UPDATE_ID,
        IJobInstanceUpdateEvent.build(saveInstanceEvent.getEvent()));

    builder.add(Edit.op(Op.pruneJobUpdateHistory(new PruneJobUpdateHistory(5, 10L))));
    // No expectation - this op is ignored.

    builder.add(Edit.op(Op.removeJobUpdate(
        new RemoveJobUpdates().setKeys(ImmutableSet.of(UPDATE_ID.newBuilder())))));
    storageUtil.jobUpdateStore.removeJobUpdates(ImmutableSet.of(UPDATE_ID));

    expect(persistence.recover()).andReturn(builder.build().stream());
  }

  private TaskConfig nonBackfilledConfig() {
    // When more fields have to be backfilled
    // modify this method.
    return makeConfig(JOB_KEY).newBuilder();
  }

  abstract class AbstractStorageFixture {
    private final AtomicBoolean runCalled = new AtomicBoolean(false);

    AbstractStorageFixture() {
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

      // Initialize persistence.
      persistence.prepare();

      // Replay the ops and perform and supplied initializationWork.
      // Simulate NOOP initialization work
      // Creating a mock and expecting apply(storeProvider) does not work here for whatever
      // reason.
      MutateWork.NoResult.Quiet initializationLogic = storeProvider -> {
        // No-op.
      };

      Capture<MutateWork.NoResult.Quiet> recoverAndInitializeWork = createCapture();
      storageUtil.storage.write(capture(recoverAndInitializeWork));
      expectLastCall().andAnswer(() -> {
        recoverAndInitializeWork.getValue().apply(storageUtil.mutableStoreProvider);
        return null;
      });

      expect(persistence.recover()).andReturn(Stream.empty());
      Capture<MutateWork<Void, RuntimeException>> recoveryWork = createCapture();
      expect(storageUtil.storage.write(capture(recoveryWork))).andAnswer(
          () -> {
            recoveryWork.getValue().apply(storageUtil.mutableStoreProvider);
            return null;
          });

      // Setup custom test expectations.
      setupExpectations();

      control.replay();

      // Start the system.
      durableStorage.prepare();
      durableStorage.start(initializationLogic);

      // Exercise the system.
      runTest();
    }

    protected void setupExpectations() throws Exception {
      // Default to no expectations.
    }

    protected abstract void runTest();
  }

  abstract class AbstractMutationFixture extends AbstractStorageFixture {
    @Override
    protected void runTest() {
      durableStorage.write((Quiet) AbstractMutationFixture.this::performMutations);
    }

    protected abstract void performMutations(MutableStoreProvider storeProvider);
  }

  private void expectPersist(Op op, Op... ops) {
    try {
      // Workaround for comparing streams.
      persistence.persist(anyObject());
      expectLastCall().andAnswer((IAnswer<Void>) () -> {
        assertEquals(
            ImmutableList.<Op>builder().add(op).add(ops).build(),
            ((Stream<Op>) EasyMock.getCurrentArguments()[0]).collect(Collectors.toList()));

        return null;
      });
    } catch (Persistence.PersistenceException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testSaveFrameworkId() throws Exception {
    String frameworkId = "bob";
    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.schedulerStore.saveFrameworkId(frameworkId);
        expectPersist(Op.saveFrameworkId(new SaveFrameworkId(frameworkId)));
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getSchedulerStore().saveFrameworkId(frameworkId);
      }
    }.run();
  }

  @Test
  public void testSaveAcceptedJob() throws Exception {
    IJobConfiguration jobConfig =
        IJobConfiguration.build(new JobConfiguration().setKey(JOB_KEY.newBuilder()));
    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.jobStore.saveAcceptedJob(jobConfig);
        expectPersist(Op.saveCronJob(new SaveCronJob(jobConfig.newBuilder())));
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getCronJobStore().saveAcceptedJob(jobConfig);
      }
    }.run();
  }

  @Test
  public void testRemoveJob() throws Exception {
    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.jobStore.removeJob(JOB_KEY);
        expectPersist(Op.removeJob(new RemoveJob().setJobKey(JOB_KEY.newBuilder())));
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getCronJobStore().removeJob(JOB_KEY);
      }
    }.run();
  }

  @Test
  public void testSaveTasks() throws Exception {
    Set<IScheduledTask> tasks = ImmutableSet.of(task("a", ScheduleStatus.INIT));
    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.taskStore.saveTasks(tasks);
        expectPersist(Op.saveTasks(new SaveTasks(IScheduledTask.toBuildersSet(tasks))));
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(tasks);
      }
    }.run();
  }

  @Test
  public void testMutateTasks() throws Exception {
    String taskId = "fred";
    Function<IScheduledTask, IScheduledTask> mutation = Functions.identity();
    Optional<IScheduledTask> mutated = Optional.of(task("a", ScheduleStatus.STARTING));
    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        expect(storageUtil.taskStore.mutateTask(taskId, mutation)).andReturn(mutated);
        expectPersist(Op.saveTasks(new SaveTasks(ImmutableSet.of(mutated.get().newBuilder()))));
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        assertEquals(mutated, storeProvider.getUnsafeTaskStore().mutateTask(taskId, mutation));
      }
    }.run();
  }

  @Test
  public void testNestedTransactions() throws Exception {
    String taskId = "fred";
    Function<IScheduledTask, IScheduledTask> mutation = Functions.identity();
    Optional<IScheduledTask> mutated = Optional.of(task("a", ScheduleStatus.STARTING));
    ImmutableSet<String> tasksToRemove = ImmutableSet.of("b");

    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        expect(storageUtil.taskStore.mutateTask(taskId, mutation)).andReturn(mutated);

        storageUtil.taskStore.deleteTasks(tasksToRemove);

        expectPersist(
            Op.saveTasks(new SaveTasks(ImmutableSet.of(mutated.get().newBuilder()))),
            Op.removeTasks(new RemoveTasks(tasksToRemove)));
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        assertEquals(mutated, storeProvider.getUnsafeTaskStore().mutateTask(taskId, mutation));

        durableStorage.write((NoResult.Quiet)
            innerProvider -> innerProvider.getUnsafeTaskStore().deleteTasks(tasksToRemove));
      }
    }.run();
  }

  @Test
  public void testSaveAndMutateTasks() throws Exception {
    String taskId = "fred";
    Function<IScheduledTask, IScheduledTask> mutation = Functions.identity();
    Set<IScheduledTask> saved = ImmutableSet.of(task("a", ScheduleStatus.INIT));
    Optional<IScheduledTask> mutated = Optional.of(task("a", ScheduleStatus.PENDING));

    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.taskStore.saveTasks(saved);

        // Nested transaction with result.
        expect(storageUtil.taskStore.mutateTask(taskId, mutation)).andReturn(mutated);

        // Resulting stream operation.
        expectPersist(Op.saveTasks(new SaveTasks(ImmutableSet.of(mutated.get().newBuilder()))));
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(saved);
        assertEquals(mutated, storeProvider.getUnsafeTaskStore().mutateTask(taskId, mutation));
      }
    }.run();
  }

  @Test
  public void testSaveAndMutateTasksNoCoalesceUniqueIds() throws Exception {
    String taskId = "fred";
    Function<IScheduledTask, IScheduledTask> mutation = Functions.identity();
    Set<IScheduledTask> saved = ImmutableSet.of(task("b", ScheduleStatus.INIT));
    Optional<IScheduledTask> mutated = Optional.of(task("a", ScheduleStatus.PENDING));

    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.taskStore.saveTasks(saved);

        // Nested transaction with result.
        expect(storageUtil.taskStore.mutateTask(taskId, mutation)).andReturn(mutated);

        // Resulting stream operation.
        expectPersist(Op.saveTasks(new SaveTasks(
                ImmutableSet.<ScheduledTask>builder()
                    .addAll(IScheduledTask.toBuildersList(saved))
                    .add(mutated.get().newBuilder())
                    .build())));
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(saved);
        assertEquals(mutated, storeProvider.getUnsafeTaskStore().mutateTask(taskId, mutation));
      }
    }.run();
  }

  @Test
  public void testRemoveTasksQuery() throws Exception {
    IScheduledTask task = task("a", ScheduleStatus.FINISHED);
    Set<String> taskIds = Tasks.ids(task);
    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.taskStore.deleteTasks(taskIds);
        expectPersist(Op.removeTasks(new RemoveTasks(taskIds)));
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().deleteTasks(taskIds);
      }
    }.run();
  }

  @Test
  public void testRemoveTasksIds() throws Exception {
    Set<String> taskIds = ImmutableSet.of("42");
    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.taskStore.deleteTasks(taskIds);
        expectPersist(Op.removeTasks(new RemoveTasks(taskIds)));
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().deleteTasks(taskIds);
      }
    }.run();
  }

  @Test
  public void testSaveQuota() throws Exception {
    String role = "role";
    IResourceAggregate quota = ResourceTestUtil.aggregate(1.0, 128L, 1024L);

    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.quotaStore.saveQuota(role, quota);
        expectPersist(Op.saveQuota(new SaveQuota(role, quota.newBuilder())));
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getQuotaStore().saveQuota(role, quota);
      }
    }.run();
  }

  @Test
  public void testRemoveQuota() throws Exception {
    String role = "role";
    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.quotaStore.removeQuota(role);
        expectPersist(Op.removeQuota(new RemoveQuota(role)));
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getQuotaStore().removeQuota(role);
      }
    }.run();
  }

  @Test
  public void testSaveHostAttributes() throws Exception {
    String host = "hostname";
    Set<Attribute> attributes =
        ImmutableSet.of(new Attribute().setName("attr").setValues(ImmutableSet.of("value")));
    Optional<IHostAttributes> hostAttributes = Optional.of(
        IHostAttributes.build(new HostAttributes()
            .setHost(host)
            .setAttributes(attributes)));

    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        expect(storageUtil.attributeStore.getHostAttributes(host))
            .andReturn(Optional.absent());

        expect(storageUtil.attributeStore.getHostAttributes(host)).andReturn(hostAttributes);

        expect(storageUtil.attributeStore.saveHostAttributes(hostAttributes.get())).andReturn(true);
        eventSink.post(new PubsubEvent.HostAttributesChanged(hostAttributes.get()));
        expectPersist(
            Op.saveHostAttributes(new SaveHostAttributes(hostAttributes.get().newBuilder())));

        expect(storageUtil.attributeStore.saveHostAttributes(hostAttributes.get()))
            .andReturn(false);

        expect(storageUtil.attributeStore.getHostAttributes(host)).andReturn(hostAttributes);
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        AttributeStore.Mutable store = storeProvider.getAttributeStore();
        assertEquals(Optional.absent(), store.getHostAttributes(host));

        assertTrue(store.saveHostAttributes(hostAttributes.get()));

        assertEquals(hostAttributes, store.getHostAttributes(host));

        assertFalse(store.saveHostAttributes(hostAttributes.get()));

        assertEquals(hostAttributes, store.getHostAttributes(host));
      }
    }.run();
  }

  @Test
  public void testSaveUpdate() throws Exception {
    IJobUpdate update = IJobUpdate.build(new JobUpdate()
        .setSummary(new JobUpdateSummary()
            .setKey(UPDATE_ID.newBuilder())
            .setUser("user"))
        .setInstructions(new JobUpdateInstructions()
            .setDesiredState(new InstanceTaskConfig()
                .setTask(new TaskConfig())
                .setInstances(ImmutableSet.of(new Range(0, 3))))
            .setInitialState(ImmutableSet.of(new InstanceTaskConfig()
                .setTask(new TaskConfig())
                .setInstances(ImmutableSet.of(new Range(0, 3)))))
            .setSettings(new JobUpdateSettings())));

    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.jobUpdateStore.saveJobUpdate(update);
        expectPersist(Op.saveJobUpdate(new SaveJobUpdate().setJobUpdate(update.newBuilder())));
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getJobUpdateStore().saveJobUpdate(update);
      }
    }.run();
  }

  @Test
  public void testSaveJobUpdateEvent() throws Exception {
    IJobUpdateEvent event = IJobUpdateEvent.build(new JobUpdateEvent()
        .setStatus(JobUpdateStatus.ROLLING_BACK)
        .setTimestampMs(12345L));

    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.jobUpdateStore.saveJobUpdateEvent(UPDATE_ID, event);
        expectPersist(Op.saveJobUpdateEvent(new SaveJobUpdateEvent(
            event.newBuilder(),
            UPDATE_ID.newBuilder())));
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getJobUpdateStore().saveJobUpdateEvent(UPDATE_ID, event);
      }
    }.run();
  }

  @Test
  public void testSaveJobInstanceUpdateEvent() throws Exception {
    IJobInstanceUpdateEvent event = IJobInstanceUpdateEvent.build(new JobInstanceUpdateEvent()
        .setAction(JobUpdateAction.INSTANCE_ROLLING_BACK)
        .setTimestampMs(12345L)
        .setInstanceId(0));

    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.jobUpdateStore.saveJobInstanceUpdateEvent(UPDATE_ID, event);
        expectPersist(Op.saveJobInstanceUpdateEvent(
            new SaveJobInstanceUpdateEvent(
                event.newBuilder(),
                UPDATE_ID.newBuilder())));
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getJobUpdateStore().saveJobInstanceUpdateEvent(UPDATE_ID, event);
      }
    }.run();
  }

  @Test
  public void testRemoveJobUpdates() throws Exception {
    IJobUpdateKey key = IJobUpdateKey.build(new JobUpdateKey()
        .setJob(JOB_KEY.newBuilder())
        .setId("update-id"));

    new AbstractMutationFixture() {
      @Override
      protected void setupExpectations() throws Exception {
        storageUtil.expectWrite();
        storageUtil.jobUpdateStore.removeJobUpdates(ImmutableSet.of(key));

        // No transaction is generated since this version is currently in 'read-only'
        // compatibility mode for this operation type.
      }

      @Override
      protected void performMutations(MutableStoreProvider storeProvider) {
        storeProvider.getJobUpdateStore().removeJobUpdates(ImmutableSet.of(key));
      }
    }.run();
  }

  private static IScheduledTask task(String id, ScheduleStatus status) {
    return IScheduledTask.build(new ScheduledTask()
        .setStatus(status)
        .setAssignedTask(new AssignedTask()
            .setTaskId(id)
            .setTask(new TaskConfig())));
  }
}

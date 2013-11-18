package com.twitter.aurora.scheduler.storage;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.gen.AssignedTask;
import com.twitter.aurora.gen.Constraint;
import com.twitter.aurora.gen.ExecutorConfig;
import com.twitter.aurora.gen.Identity;
import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.LimitConstraint;
import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.gen.TaskConstraint;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.aurora.scheduler.storage.Storage.MutateWork;
import com.twitter.aurora.scheduler.storage.entities.IJobConfiguration;
import com.twitter.aurora.scheduler.storage.entities.IJobKey;
import com.twitter.aurora.scheduler.storage.entities.IScheduledTask;
import com.twitter.aurora.scheduler.storage.mem.MemStorage;
import com.twitter.common.util.testing.FakeClock;

import static org.junit.Assert.assertEquals;

public class StorageBackfillTest {

  private Storage storage;
  private FakeClock clock;

  @Before
  public void setUp() {
    storage = MemStorage.newEmptyStorage();
    clock = new FakeClock();
  }

  private static IScheduledTask makeTask(
      String id,
      Optional<Integer> oldInstanceId,
      Optional<Integer> newInstanceId) {

    return makeTask(
        id,
        oldInstanceId,
        newInstanceId,
        Optional.of(new byte[]{}),
        Optional.of(new ExecutorConfig("", "")));
  }

  private static IScheduledTask makeTask(
      String id,
      Optional<Integer> oldInstanceId,
      Optional<Integer> newInstanceId,
      Optional<byte[]> thermosConfig,
      Optional<ExecutorConfig> executorConfig) {

    TaskConfig config = new TaskConfig()
        .setOwner(new Identity("user", "role"))
        .setEnvironment("test")
        .setJobName("jobName")
        .setProduction(false)
        .setConstraints(ImmutableSet.of(
            new Constraint("host", TaskConstraint.limit(new LimitConstraint(1)))))
        .setRequestedPorts(ImmutableSet.<String>of())
        .setMaxTaskFailures(1)
        .setTaskLinks(ImmutableMap.<String, String>of());
    ScheduledTask task = new ScheduledTask().setAssignedTask(
        new AssignedTask().setTask(config));
    task.getAssignedTask().setTaskId(id);
    if (thermosConfig.isPresent()) {
      task.getAssignedTask().getTask().setThermosConfig(thermosConfig.get());
    }
    if (executorConfig.isPresent()) {
      task.getAssignedTask().getTask().setExecutorConfig(executorConfig.get());
    }
    if (oldInstanceId.isPresent()) {
      task.getAssignedTask().getTask().setInstanceIdDEPRECATED(oldInstanceId.get());
    }
    if (newInstanceId.isPresent()) {
      task.getAssignedTask().setInstanceId(newInstanceId.get());
    }
    return IScheduledTask.build(task);
  }

  @Test
  public void testInstanceIdBackfill() {
    final String bothId = "both";
    final String oldId = "old";
    final String newId = "new";
    final IScheduledTask bothFields = makeTask(bothId, Optional.of(5), Optional.of(5));
    final IScheduledTask oldField = makeTask(oldId, Optional.of(6), Optional.<Integer>absent());
    final IScheduledTask newField = makeTask(newId, Optional.<Integer>absent(), Optional.of(7));

    storage.write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(
            ImmutableSet.of(bothFields, oldField, newField));
        StorageBackfill.backfill(storeProvider, clock);
      }
    });

    assertEquals(makeTask(bothId, Optional.of(5), Optional.of(5)), getTask(bothId));
    assertEquals(makeTask(oldId, Optional.of(6), Optional.of(6)), getTask(oldId));
    assertEquals(makeTask(newId, Optional.of(7), Optional.of(7)), getTask(newId));
  }

  @Test
  public void testExecutorConfigTaskBackfill() {
    final String bothId = "bothTaskId";
    final String oldId = "oldTaskId";
    final String newId = "newTaskId";
    final String auroraExecutor = "AuroraExecutor";
    final String thermosConfigString = "thermosConfig";
    final byte[] thermosConfigBytes = thermosConfigString.getBytes(Charsets.UTF_8);
    final IScheduledTask bothTask = makeTask(
        bothId,
        Optional.of(5),
        Optional.of(5),
        Optional.of(thermosConfigBytes),
        Optional.of(new ExecutorConfig(auroraExecutor, thermosConfigString)));
    final IScheduledTask oldTask = makeTask(
        oldId,
        Optional.of(5),
        Optional.of(5),
        Optional.of(thermosConfigBytes),
        Optional.<ExecutorConfig>absent());
    final IScheduledTask newTask = makeTask(
        newId,
        Optional.of(5),
        Optional.of(5),
        Optional.<byte[]>absent(),
        Optional.of(new ExecutorConfig(auroraExecutor, thermosConfigString)));

    storage.write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(
            ImmutableSet.of(bothTask, oldTask, newTask));
        StorageBackfill.backfill(storeProvider, clock);
      }
    });

    IScheduledTask expectedTask = makeTask(
        bothId,
        Optional.of(5),
        Optional.of(5),
        Optional.of(thermosConfigBytes),
        Optional.of(new ExecutorConfig(auroraExecutor, thermosConfigString)));

    assertEquals(expectedTask, getTask(bothId));

    expectedTask = makeTask(
        oldId,
        Optional.of(5),
        Optional.of(5),
        Optional.of(thermosConfigBytes),
        Optional.of(new ExecutorConfig(auroraExecutor, thermosConfigString)));

    assertEquals(expectedTask, getTask(oldId));

    expectedTask = makeTask(
        newId,
        Optional.of(5),
        Optional.of(5),
        Optional.of(thermosConfigBytes),
        Optional.of(new ExecutorConfig(auroraExecutor, thermosConfigString)));

    assertEquals(expectedTask, getTask(newId));
  }

  @Test
  public void testRewriteThrottledState() {
    final IScheduledTask savedTask =
        IScheduledTask.build(makeTask("id", Optional.<Integer>absent(), Optional.of(0)).newBuilder()
            .setStatus(ScheduleStatus.THROTTLED));

    storage.write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(savedTask));
        StorageBackfill.backfill(storeProvider, clock);
      }
    });

    assertEquals(
        IScheduledTask.build(savedTask.newBuilder().setStatus(ScheduleStatus.PENDING)),
        getTask("id"));
  }

  @Test
  public void testExecutorConfigJobConfigBackfill() {
    final String bothId = "bothTaskId";
    final String oldId = "oldTaskId";
    final String newId = "newTaskId";
    final String mid = "mid";
    final String auroraExecutor = "AuroraExecutor";
    final String thermosConfigString = "thermosConfig";
    final byte[] thermosConfigBytes = thermosConfigString.getBytes(Charsets.UTF_8);
    final IScheduledTask bothTask = makeTask(
        bothId,
        Optional.of(5),
        Optional.of(5),
        Optional.of(thermosConfigBytes),
        Optional.of(new ExecutorConfig(auroraExecutor, thermosConfigString)));
    final IScheduledTask oldTask = makeTask(
        oldId,
        Optional.of(5),
        Optional.of(5),
        Optional.of(thermosConfigBytes),
        Optional.<ExecutorConfig>absent());
    final IScheduledTask newTask = makeTask(
        newId,
        Optional.of(5),
        Optional.of(5),
        Optional.<byte[]>absent(),
        Optional.of(new ExecutorConfig(auroraExecutor, thermosConfigString)));

    final IJobKey jobKey = JobKeys.from("role", "env", "name");
    final JobConfiguration bothJob = new JobConfiguration()
        .setKey(jobKey.newBuilder())
        .setTaskConfig(bothTask.getAssignedTask().getTask().newBuilder());
    final JobConfiguration oldJob = new JobConfiguration()
        .setKey(jobKey.newBuilder())
        .setTaskConfig(oldTask.getAssignedTask().getTask().newBuilder());
    final JobConfiguration newJob = new JobConfiguration()
        .setKey(jobKey.newBuilder())
        .setTaskConfig(newTask.getAssignedTask().getTask().newBuilder());

    storage.write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getJobStore().saveAcceptedJob(mid, IJobConfiguration.build(bothJob));
        storeProvider.getJobStore().saveAcceptedJob(mid, IJobConfiguration.build(newJob));
        storeProvider.getJobStore().saveAcceptedJob(mid, IJobConfiguration.build(oldJob));
        StorageBackfill.backfill(storeProvider, clock);
      }
    });

    IScheduledTask expectedTask = makeTask(
        bothId,
        Optional.of(5),
        Optional.of(5),
        Optional.of(thermosConfigBytes),
        Optional.of(new ExecutorConfig(auroraExecutor, thermosConfigString)));

    JobConfiguration expectedJob = new JobConfiguration()
        .setKey(jobKey.newBuilder())
        .setTaskConfig(expectedTask.getAssignedTask().getTask().newBuilder());

    assertEquals(IJobConfiguration.build(expectedJob), getJob(mid, jobKey));

    expectedTask = makeTask(
        oldId,
        Optional.of(5),
        Optional.of(5),
        Optional.of(thermosConfigBytes),
        Optional.of(new ExecutorConfig(auroraExecutor, thermosConfigString)));

    expectedJob = new JobConfiguration()
        .setKey(jobKey.newBuilder())
        .setTaskConfig(expectedTask.getAssignedTask().getTask().newBuilder());

    assertEquals(IJobConfiguration.build(expectedJob), getJob(mid, jobKey));

    expectedTask = makeTask(
        newId,
        Optional.of(5),
        Optional.of(5),
        Optional.of(thermosConfigBytes),
        Optional.of(new ExecutorConfig(auroraExecutor, thermosConfigString)));

    expectedJob = new JobConfiguration()
        .setKey(jobKey.newBuilder())
        .setTaskConfig(expectedTask.getAssignedTask().getTask().newBuilder());

    assertEquals(IJobConfiguration.build(expectedJob), getJob(mid, jobKey));
  }

  private IJobConfiguration getJob(final String mid, final IJobKey jobKey) {
    return storage.consistentRead(new Storage.Work.Quiet<Optional<IJobConfiguration>>() {
        @Override public Optional<IJobConfiguration> apply(Storage.StoreProvider provider) {
          return provider.getJobStore().fetchJob(mid, jobKey);
        }
    }).get();
  }

  private IScheduledTask getTask(final String id) {
    return Iterables.getOnlyElement(
        Storage.Util.consistentFetchTasks(storage, Query.taskScoped(id)));
  }
}

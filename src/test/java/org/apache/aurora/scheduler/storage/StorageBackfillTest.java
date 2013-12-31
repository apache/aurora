package com.twitter.aurora.scheduler.storage;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.gen.AssignedTask;
import com.twitter.aurora.gen.Constraint;
import com.twitter.aurora.gen.Identity;
import com.twitter.aurora.gen.LimitConstraint;
import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.gen.TaskConstraint;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.aurora.scheduler.storage.Storage.MutateWork;
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

  private static IScheduledTask makeTask(String id, int instanceId) {

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
    task.getAssignedTask().setInstanceId(instanceId);
    return IScheduledTask.build(task);
  }

  @Test
  public void testRewriteThrottledState() {
    final IScheduledTask savedTask =
        IScheduledTask.build(makeTask("id", 0).newBuilder().setStatus(ScheduleStatus.THROTTLED));

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

  private IScheduledTask getTask(final String id) {
    return Iterables.getOnlyElement(
        Storage.Util.consistentFetchTasks(storage, Query.taskScoped(id)));
  }
}

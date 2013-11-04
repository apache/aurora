package com.twitter.aurora.scheduler.storage;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.gen.AssignedTask;
import com.twitter.aurora.gen.Constraint;
import com.twitter.aurora.gen.Identity;
import com.twitter.aurora.gen.LimitConstraint;
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

  private static IScheduledTask makeTask(
      String id,
      Optional<Integer> oldInstanceId,
      Optional<Integer> newInstanceId) {

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

  private IScheduledTask getTask(final String id) {
    return Iterables.getOnlyElement(
        Storage.Util.consistentFetchTasks(storage, Query.taskScoped(id)));
  }
}

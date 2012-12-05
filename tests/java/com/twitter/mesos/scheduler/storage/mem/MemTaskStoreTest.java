package com.twitter.mesos.scheduler.storage.mem;

import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Atomics;

import org.junit.Before;
import org.junit.Test;

import com.twitter.common.base.Closure;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.storage.TaskStore;

import static org.junit.Assert.assertEquals;

public class MemTaskStoreTest {

  private static final ScheduledTask TASK_A = makeTask("a");
  private static final ScheduledTask TASK_B = makeTask("b");
  private static final ScheduledTask TASK_C = makeTask("c");
  private static final ScheduledTask TASK_D = makeTask("d");

  private TaskStore.Mutable store;

  @Before
  public void setUp() {
    store = new MemTaskStore();
  }

  @Test
  public void testSave() {
    store.saveTasks(ImmutableSet.of(TASK_A, TASK_B));
    assertStoreContents(TASK_A, TASK_B);

    store.saveTasks(ImmutableSet.of(TASK_C, TASK_D));
    assertStoreContents(TASK_A, TASK_B, TASK_C, TASK_D);

    // Saving the same task should overwrite.
    ScheduledTask taskAModified = TASK_A.deepCopy().setStatus(ScheduleStatus.RUNNING);
    store.saveTasks(ImmutableSet.of(taskAModified));
    assertStoreContents(taskAModified, TASK_B, TASK_C, TASK_D);
  }

  @Test
  public void testQuery() {
    assertStoreContents();
    store.saveTasks(ImmutableSet.of(TASK_A, TASK_B, TASK_C, TASK_D));

    assertQueryResults(Query.byId("b"), TASK_B);
    assertQueryResults(Query.byId(ImmutableSet.of("a", "d")), TASK_A, TASK_D);
    assertQueryResults(Query.byRole("role-c"), TASK_C);
    assertQueryResults(Query.byStatus(ScheduleStatus.PENDING), TASK_A, TASK_B, TASK_C, TASK_D);
    assertQueryResults(Query.liveShard("role-a", "job-a", 0), TASK_A);
    assertQueryResults(Query.activeQuery(new Identity().setRole("role-b"), "job-b"), TASK_B);

    // Explicitly call out the current differing behaviors for types of empty query conditions.
    // Specifically - null task IDs and empty task IDs are different than other 'IN' conditions..
    assertQueryResults(new TaskQuery().setTaskIds(null), TASK_A, TASK_B, TASK_C, TASK_D);
    assertQueryResults(new TaskQuery().setTaskIds(ImmutableSet.<String>of()));
    assertQueryResults(
        new TaskQuery().setShardIds(ImmutableSet.<Integer>of()),
        TASK_A, TASK_B, TASK_C, TASK_D);
    assertQueryResults(
        new TaskQuery().setStatuses(ImmutableSet.<ScheduleStatus>of()),
        TASK_A, TASK_B, TASK_C, TASK_D);
  }

  @Test
  public void testMutate() {
    store.saveTasks(ImmutableSet.of(TASK_A, TASK_B, TASK_C, TASK_D));
    assertQueryResults(Query.byStatus(ScheduleStatus.RUNNING));

    store.mutateTasks(Query.byId("a"), new Closure<ScheduledTask>() {
      @Override public void execute(ScheduledTask task) {
        task.setStatus(ScheduleStatus.RUNNING);
      }
    });

    assertQueryResults(
        Query.byStatus(ScheduleStatus.RUNNING),
        TASK_A.deepCopy().setStatus(ScheduleStatus.RUNNING));

    store.mutateTasks(Query.GET_ALL, new Closure<ScheduledTask>() {
      @Override public void execute(ScheduledTask task) {
        task.setStatus(ScheduleStatus.ASSIGNED);
      }
    });

    assertStoreContents(
        TASK_A.deepCopy().setStatus(ScheduleStatus.ASSIGNED),
        TASK_B.deepCopy().setStatus(ScheduleStatus.ASSIGNED),
        TASK_C.deepCopy().setStatus(ScheduleStatus.ASSIGNED),
        TASK_D.deepCopy().setStatus(ScheduleStatus.ASSIGNED));
  }

  @Test
  public void testDelete() {
    store.saveTasks(ImmutableSet.of(TASK_A, TASK_B, TASK_C, TASK_D));
    store.deleteTasks(ImmutableSet.of("a"));
    assertStoreContents(TASK_B, TASK_C, TASK_D);
    store.deleteTasks(ImmutableSet.of("c"));
    assertStoreContents(TASK_B, TASK_D);
    store.deleteTasks(ImmutableSet.of("b", "d"));
    assertStoreContents();
  }

  @Test
  public void testDeleteAll() {
    store.saveTasks(ImmutableSet.of(TASK_A, TASK_B, TASK_C, TASK_D));
    store.deleteTasks();
    assertStoreContents();
  }

  @Test
  public void testImmutable() {
    ScheduledTask taskA = TASK_A.deepCopy();

    // Mutate after saving.
    store.saveTasks(ImmutableSet.of(taskA));
    taskA.setStatus(ScheduleStatus.RUNNING);
    assertStoreContents(TASK_A);

    // Mutate query result.
    Iterables.getOnlyElement(store.fetchTasks(Query.GET_ALL)).setStatus(ScheduleStatus.KILLED);
    assertStoreContents(TASK_A);

    // Capture reference during mutation and mutate later.
    final AtomicReference<ScheduledTask> capture = Atomics.newReference();
    store.mutateTasks(Query.GET_ALL, new Closure<ScheduledTask>() {
      @Override public void execute(ScheduledTask task) {
        task.setStatus(ScheduleStatus.ASSIGNED);
        capture.set(task);
      }
    });
    capture.get().setStatus(ScheduleStatus.LOST);
    assertStoreContents(TASK_A.deepCopy().setStatus(ScheduleStatus.ASSIGNED));
  }

  private void assertStoreContents(ScheduledTask... tasks) {
    assertQueryResults(Query.GET_ALL, tasks);
  }

  private void assertQueryResults(TaskQuery query, ScheduledTask... tasks) {
    assertEquals(
        ImmutableSet.<ScheduledTask>builder().add(tasks).build(),
        store.fetchTasks(query));
  }

  private static ScheduledTask makeTask(String id) {
    return new ScheduledTask()
        .setStatus(ScheduleStatus.PENDING)
        .setAssignedTask(new AssignedTask()
            .setTaskId(id)
            .setTask(new TwitterTaskInfo()
                .setShardId(0)
                .setJobName("job-" + id)
                .setOwner(new Identity("role-" + id, "user-" + id))));
  }
}

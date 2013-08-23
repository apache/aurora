package com.twitter.aurora.scheduler.storage.mem;

import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Atomics;

import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.gen.AssignedTask;
import com.twitter.aurora.gen.Identity;
import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.gen.TaskQuery;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.common.base.Closure;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MemTaskStoreTest {

  private static final ScheduledTask TASK_A = makeTask("a");
  private static final ScheduledTask TASK_B = makeTask("b");
  private static final ScheduledTask TASK_C = makeTask("c");
  private static final ScheduledTask TASK_D = makeTask("d");

  private MemTaskStore store;

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

    assertQueryResults(Query.taskScoped("b"), TASK_B);
    assertQueryResults(Query.taskScoped("a", "d"), TASK_A, TASK_D);
    assertQueryResults(Query.roleScoped("role-c"), TASK_C);
    assertQueryResults(Query.envScoped("role-c", "env-c"), TASK_C);
    assertQueryResults(Query.envScoped("role-c", "devel"));
    assertQueryResults(
        Query.unscoped().byStatus(ScheduleStatus.PENDING),
        TASK_A, TASK_B, TASK_C, TASK_D);
    assertQueryResults(
        Query.shardScoped(JobKeys.from("role-a", "env-a", "job-a"), 0).active(), TASK_A);
    assertQueryResults(Query.jobScoped(JobKeys.from("role-b", "env-b", "job-b")).active(), TASK_B);
    assertQueryResults(Query.jobScoped(JobKeys.from("role-b", "devel", "job-b")).active());

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
    assertQueryResults(Query.statusScoped(ScheduleStatus.RUNNING));

    store.mutateTasks(Query.taskScoped("a"), new Closure<ScheduledTask>() {
      @Override public void execute(ScheduledTask task) {
        task.setStatus(ScheduleStatus.RUNNING);
      }
    });

    assertQueryResults(
        Query.statusScoped(ScheduleStatus.RUNNING),
        TASK_A.deepCopy().setStatus(ScheduleStatus.RUNNING));

    store.mutateTasks(Query.unscoped(), new Closure<ScheduledTask>() {
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
  public void testUnsafeModifyInPlace() {
    TaskConfig updated =
        TASK_A.getAssignedTask().getTask().deepCopy().setThermosConfig("new_config".getBytes());

    String taskId = Tasks.id(TASK_A);
    assertFalse(store.unsafeModifyInPlace(taskId, updated.deepCopy()));

    store.saveTasks(ImmutableSet.of(TASK_A));
    assertTrue(store.unsafeModifyInPlace(taskId, updated.deepCopy()));
    Query.Builder query = Query.taskScoped(taskId);
    TaskConfig stored =
        Iterables.getOnlyElement(store.fetchTasks(query)).getAssignedTask().getTask();
    assertEquals(updated, stored);

    store.deleteTasks(ImmutableSet.of(taskId));
    assertFalse(store.unsafeModifyInPlace(taskId, updated.deepCopy()));
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
    store.deleteAllTasks();
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
    Iterables.getOnlyElement(store.fetchTasks(Query.unscoped())).setStatus(ScheduleStatus.KILLED);
    assertStoreContents(TASK_A);

    // Capture reference during mutation and mutate later.
    final AtomicReference<ScheduledTask> capture = Atomics.newReference();
    store.mutateTasks(Query.unscoped(), new Closure<ScheduledTask>() {
      @Override public void execute(ScheduledTask task) {
        task.setStatus(ScheduleStatus.ASSIGNED);
        capture.set(task);
      }
    });
    capture.get().setStatus(ScheduleStatus.LOST);
    assertStoreContents(TASK_A.deepCopy().setStatus(ScheduleStatus.ASSIGNED));
  }

  @Test
  public void testConsistentJobIndex() {
    final ScheduledTask a = makeTask("a", "jim", "test", "job");
    final ScheduledTask b = makeTask("b", "jim", "test", "job");
    final ScheduledTask c = makeTask("c", "jim", "test", "job2");
    final ScheduledTask d = makeTask("d", "joe", "test", "job");
    final ScheduledTask e = makeTask("e", "jim", "prod", "job");
    final Query.Builder jimsJob = Query.jobScoped(JobKeys.from("jim", "test", "job"));
    final Query.Builder jimsJob2 = Query.jobScoped(JobKeys.from("jim", "test", "job2"));
    final Query.Builder joesJob = Query.jobScoped(JobKeys.from("joe", "test", "job"));
    final Query.Builder jimsProdJob = Query.jobScoped(JobKeys.from("jim", "prod", "job"));

    store.saveTasks(ImmutableSet.of(a, b, c, d, e));
    assertQueryResults(jimsJob, a, b);
    assertQueryResults(jimsJob2, c);
    assertQueryResults(joesJob, d);

    store.deleteTasks(ImmutableSet.of(Tasks.id(b)));
    assertQueryResults(jimsJob, a);
    assertQueryResults(jimsJob2, c);
    assertQueryResults(joesJob, d);

    store.mutateTasks(jimsJob, new Closure<ScheduledTask>() {
      @Override public void execute(ScheduledTask task) {
        task.setStatus(ScheduleStatus.RUNNING);
      }
    });
    // Change 'a' locally to make subsequent equality checks pass.
    a.setStatus(ScheduleStatus.RUNNING);
    assertQueryResults(jimsJob, a);
    assertQueryResults(jimsJob2, c);
    assertQueryResults(joesJob, d);

    store.deleteTasks(ImmutableSet.of(Tasks.id(d)));
    assertQueryResults(joesJob);

    store.deleteTasks(ImmutableSet.of(Tasks.id(d)));
    assertQueryResults(jimsJob, a);
    assertQueryResults(jimsJob2, c);
    assertQueryResults(joesJob);

    store.saveTasks(ImmutableSet.of(b));
    assertQueryResults(jimsJob, a, b);
    assertQueryResults(jimsJob2, c);
    assertQueryResults(joesJob);
  }

  private void assertStoreContents(ScheduledTask... tasks) {
    assertQueryResults(Query.unscoped(), tasks);
  }

  private void assertQueryResults(TaskQuery query, ScheduledTask... tasks) {
    assertQueryResults(Query.arbitrary(query), tasks);
  }

  private void assertQueryResults(Query.Builder query, ScheduledTask... tasks) {
    assertEquals(
        ImmutableSet.<ScheduledTask>builder().add(tasks).build(),
        store.fetchTasks(query));
  }

  private static ScheduledTask makeTask(String id, String role, String env, String jobName) {
    return new ScheduledTask()
        .setStatus(ScheduleStatus.PENDING)
        .setAssignedTask(new AssignedTask()
            .setTaskId(id)
            .setTask(new TaskConfig()
                .setShardId(0)
                .setJobName(jobName)
                .setEnvironment(env)
                .setOwner(new Identity(role, role))));
  }

  private static ScheduledTask makeTask(String id) {
    return makeTask(id, "role-" + id, "env-" + id, "job-" + id);
  }
}

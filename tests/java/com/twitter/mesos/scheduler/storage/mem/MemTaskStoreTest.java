package com.twitter.mesos.scheduler.storage.mem;

import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Atomics;

import org.junit.Before;
import org.junit.Test;

import com.twitter.common.base.Closure;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.JobKeys;
import com.twitter.mesos.scheduler.Query;

import static org.junit.Assert.assertEquals;

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
    assertQueryResults(Query.envScoped("role-c", "test"), TASK_C);
    assertQueryResults(Query.envScoped("role-c", "devel"));
    assertQueryResults(
        Query.unscoped().byStatus(ScheduleStatus.PENDING),
        TASK_A, TASK_B, TASK_C, TASK_D);
    assertQueryResults(Query.shardScoped("role-a", "job-a", 0).active(), TASK_A);
    assertQueryResults(
        Query.shardScoped(JobKeys.from("role-a", "test", "job-a"), 0).active(),
        TASK_A);
    assertQueryResults(Query.jobScoped("role-b", "job-b").active(), TASK_B);
    assertQueryResults(Query.jobScoped(JobKeys.from("role-b", "test", "job-b")).active(), TASK_B);
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

  private void beforeAndAfterCommit(Runnable work) {
    work.run();
    store.commit();
    work.run();
  }

  @Test
  public void testConsistentJobIndex() {
    final ScheduledTask a = makeTask("a", "jim", "job");
    final ScheduledTask b = makeTask("b", "jim", "job");
    final ScheduledTask c = makeTask("c", "jim", "job2");
    final ScheduledTask d = makeTask("d", "joe", "job");
    final Query.Builder jimsJob = Query.jobScoped("jim", "job");
    final Query.Builder jimsJob2 = Query.jobScoped("jim", "job2");
    final Query.Builder joesJob = Query.jobScoped("joe", "job");

    store.saveTasks(ImmutableSet.of(a, b, c, d));
    beforeAndAfterCommit(new Runnable() {
      @Override public void run() {
        assertQueryResults(jimsJob, a, b);
        assertQueryResults(jimsJob2, c);
        assertQueryResults(joesJob, d);
      }
    });

    store.deleteTasks(ImmutableSet.of(Tasks.id(b)));
    beforeAndAfterCommit(new Runnable() {
      @Override public void run() {
        assertQueryResults(jimsJob, a);
        assertQueryResults(jimsJob2, c);
        assertQueryResults(joesJob, d);
      }
    });

    store.mutateTasks(jimsJob.get(), new Closure<ScheduledTask>() {
      @Override public void execute(ScheduledTask task) {
        task.setStatus(ScheduleStatus.RUNNING);
      }
    });
    // Change 'a' locally to make subsequent equality checks pass.
    a.setStatus(ScheduleStatus.RUNNING);
    beforeAndAfterCommit(new Runnable() {
      @Override public void run() {
        assertQueryResults(jimsJob, a);
        assertQueryResults(jimsJob2, c);
        assertQueryResults(joesJob, d);
      }
    });

    store.deleteTasks(ImmutableSet.of(Tasks.id(d)));
    assertQueryResults(joesJob);
    store.rollback();
    assertQueryResults(jimsJob, a);
    assertQueryResults(jimsJob2, c);
    assertQueryResults(joesJob, d);

    store.deleteTasks(ImmutableSet.of(Tasks.id(d)));
    beforeAndAfterCommit(new Runnable() {
      @Override public void run() {
        assertQueryResults(jimsJob, a);
        assertQueryResults(jimsJob2, c);
        assertQueryResults(joesJob);
      }
    });

    store.saveTasks(ImmutableSet.of(b));
    beforeAndAfterCommit(new Runnable() {
      @Override public void run() {
        assertQueryResults(jimsJob, a, b);
        assertQueryResults(jimsJob2, c);
        assertQueryResults(joesJob);
      }
    });
  }

  private void assertStoreContents(ScheduledTask... tasks) {
    assertQueryResults(Query.unscoped(), tasks);
  }

  private void assertQueryResults(TaskQuery query, ScheduledTask... tasks) {
    assertQueryResults(Suppliers.ofInstance(query), tasks);
  }

  private void assertQueryResults(Supplier<TaskQuery> query, ScheduledTask... tasks) {
    assertEquals(
        ImmutableSet.<ScheduledTask>builder().add(tasks).build(),
        store.fetchTasks(query));
  }

  private static ScheduledTask makeTask(String id, String role, String jobName) {
    return new ScheduledTask()
        .setStatus(ScheduleStatus.PENDING)
        .setAssignedTask(new AssignedTask()
            .setTaskId(id)
            .setTask(new TwitterTaskInfo()
                .setShardId(0)
                .setJobName(jobName)
                .setEnvironment("test")
                .setOwner(new Identity(role, role))));
  }

  private static ScheduledTask makeTask(String id) {
    return makeTask(id, "role-" + id, "job-" + id);
  }
}

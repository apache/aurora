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
package com.twitter.aurora.scheduler.storage.mem;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.gen.AssignedTask;
import com.twitter.aurora.gen.ExecutorConfig;
import com.twitter.aurora.gen.Identity;
import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.gen.TaskQuery;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.storage.TaskStore.Mutable.TaskMutation;
import com.twitter.aurora.scheduler.storage.entities.IScheduledTask;
import com.twitter.aurora.scheduler.storage.entities.ITaskConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static com.twitter.aurora.gen.ScheduleStatus.RUNNING;

public class MemTaskStoreTest {

  private static final IScheduledTask TASK_A = makeTask("a");
  private static final IScheduledTask TASK_B = makeTask("b");
  private static final IScheduledTask TASK_C = makeTask("c");
  private static final IScheduledTask TASK_D = makeTask("d");

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
    IScheduledTask taskAModified =
        IScheduledTask.build(TASK_A.newBuilder().setStatus(RUNNING));
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
        Query.instanceScoped(JobKeys.from("role-a", "env-a", "job-a"), 0).active(), TASK_A);
    assertQueryResults(Query.jobScoped(JobKeys.from("role-b", "env-b", "job-b")).active(), TASK_B);
    assertQueryResults(Query.jobScoped(JobKeys.from("role-b", "devel", "job-b")).active());

    // Explicitly call out the current differing behaviors for types of empty query conditions.
    // Specifically - null task IDs and empty task IDs are different than other 'IN' conditions..
    assertQueryResults(new TaskQuery().setTaskIds(null), TASK_A, TASK_B, TASK_C, TASK_D);
    assertQueryResults(new TaskQuery().setTaskIds(ImmutableSet.<String>of()));
    assertQueryResults(
        new TaskQuery().setInstanceIds(ImmutableSet.<Integer>of()),
        TASK_A, TASK_B, TASK_C, TASK_D);
    assertQueryResults(
        new TaskQuery().setStatuses(ImmutableSet.<ScheduleStatus>of()),
        TASK_A, TASK_B, TASK_C, TASK_D);
  }

  @Test
  public void testMutate() {
    store.saveTasks(ImmutableSet.of(TASK_A, TASK_B, TASK_C, TASK_D));
    assertQueryResults(Query.statusScoped(RUNNING));

    store.mutateTasks(Query.taskScoped("a"), new TaskMutation() {
      @Override
      public IScheduledTask apply(IScheduledTask task) {
        return IScheduledTask.build(task.newBuilder().setStatus(RUNNING));
      }
    });

    assertQueryResults(
        Query.statusScoped(RUNNING),
        IScheduledTask.build(TASK_A.newBuilder().setStatus(RUNNING)));

    store.mutateTasks(Query.unscoped(), new TaskMutation() {
      @Override
      public IScheduledTask apply(IScheduledTask task) {
        return IScheduledTask.build(task.newBuilder().setStatus(ScheduleStatus.ASSIGNED));
      }
    });

    assertStoreContents(
        IScheduledTask.build(TASK_A.newBuilder().setStatus(ScheduleStatus.ASSIGNED)),
        IScheduledTask.build(TASK_B.newBuilder().setStatus(ScheduleStatus.ASSIGNED)),
        IScheduledTask.build(TASK_C.newBuilder().setStatus(ScheduleStatus.ASSIGNED)),
        IScheduledTask.build(TASK_D.newBuilder().setStatus(ScheduleStatus.ASSIGNED)));
  }

  @Test
  public void testUnsafeModifyInPlace() {
    ITaskConfig updated = ITaskConfig.build(
        TASK_A.getAssignedTask()
            .getTask()
            .newBuilder()
            .setExecutorConfig(new ExecutorConfig("aurora", "new_config")));

    String taskId = Tasks.id(TASK_A);
    assertFalse(store.unsafeModifyInPlace(taskId, updated));

    store.saveTasks(ImmutableSet.of(TASK_A));
    assertTrue(store.unsafeModifyInPlace(taskId, updated));
    Query.Builder query = Query.taskScoped(taskId);
    ITaskConfig stored =
        Iterables.getOnlyElement(store.fetchTasks(query)).getAssignedTask().getTask();
    assertEquals(updated, stored);

    store.deleteTasks(ImmutableSet.of(taskId));
    assertFalse(store.unsafeModifyInPlace(taskId, updated));
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
  public void testConsistentJobIndex() {
    final IScheduledTask a = makeTask("a", "jim", "test", "job");
    final IScheduledTask b = makeTask("b", "jim", "test", "job");
    final IScheduledTask c = makeTask("c", "jim", "test", "job2");
    final IScheduledTask d = makeTask("d", "joe", "test", "job");
    final IScheduledTask e = makeTask("e", "jim", "prod", "job");
    final Query.Builder jimsJob = Query.jobScoped(JobKeys.from("jim", "test", "job"));
    final Query.Builder jimsJob2 = Query.jobScoped(JobKeys.from("jim", "test", "job2"));
    final Query.Builder joesJob = Query.jobScoped(JobKeys.from("joe", "test", "job"));

    store.saveTasks(ImmutableSet.of(a, b, c, d, e));
    assertQueryResults(jimsJob, a, b);
    assertQueryResults(jimsJob2, c);
    assertQueryResults(joesJob, d);

    store.deleteTasks(ImmutableSet.of(Tasks.id(b)));
    assertQueryResults(jimsJob, a);
    assertQueryResults(jimsJob2, c);
    assertQueryResults(joesJob, d);

    store.mutateTasks(jimsJob, new TaskMutation() {
      @Override public IScheduledTask apply(IScheduledTask task) {
        return IScheduledTask.build(task.newBuilder().setStatus(RUNNING));
      }
    });
    IScheduledTask aRunning = IScheduledTask.build(a.newBuilder().setStatus(RUNNING));
    assertQueryResults(jimsJob, aRunning);
    assertQueryResults(jimsJob2, c);
    assertQueryResults(joesJob, d);

    store.deleteTasks(ImmutableSet.of(Tasks.id(d)));
    assertQueryResults(joesJob);

    store.deleteTasks(ImmutableSet.of(Tasks.id(d)));
    assertQueryResults(jimsJob, aRunning);
    assertQueryResults(jimsJob2, c);
    assertQueryResults(joesJob);

    store.saveTasks(ImmutableSet.of(b));
    assertQueryResults(jimsJob, aRunning, b);
    assertQueryResults(jimsJob2, c);
    assertQueryResults(joesJob);
  }

  @Test
  public void testCanonicalTaskConfigs() {
    IScheduledTask a = makeTask("a", "role", "env", "job");
    IScheduledTask b = makeTask("a", "role", "env", "job");
    IScheduledTask c = makeTask("a", "role", "env", "job");
    Set<IScheduledTask> inserted = ImmutableSet.of(a, b, c);

    store.saveTasks(inserted);
    Set<ITaskConfig> storedConfigs = FluentIterable.from(store.fetchTasks(Query.unscoped()))
        .transform(Tasks.SCHEDULED_TO_INFO)
        .toSet();
    assertEquals(
        FluentIterable.from(inserted).transform(Tasks.SCHEDULED_TO_INFO).toSet(),
        storedConfigs);
    Map<ITaskConfig, ITaskConfig> identityMap = Maps.newIdentityHashMap();
    for (ITaskConfig stored : storedConfigs) {
      identityMap.put(stored, stored);
    }
    assertEquals(
        ImmutableMap.of(Tasks.SCHEDULED_TO_INFO.apply(a), Tasks.SCHEDULED_TO_INFO.apply(a)),
        identityMap);
  }

  private void assertStoreContents(IScheduledTask... tasks) {
    assertQueryResults(Query.unscoped(), tasks);
  }

  private void assertQueryResults(TaskQuery query, IScheduledTask... tasks) {
    assertQueryResults(Query.arbitrary(query), tasks);
  }

  private void assertQueryResults(Query.Builder query, IScheduledTask... tasks) {
    assertEquals(
        ImmutableSet.<IScheduledTask>builder().add(tasks).build(),
        store.fetchTasks(query));
  }

  private static IScheduledTask makeTask(String id, String role, String env, String jobName) {
    return IScheduledTask.build(new ScheduledTask()
        .setStatus(ScheduleStatus.PENDING)
        .setAssignedTask(new AssignedTask()
            .setInstanceId(0)
            .setTaskId(id)
            .setTask(new TaskConfig()
                .setJobName(jobName)
                .setEnvironment(env)
                .setOwner(new Identity(role, role)))));
  }

  private static IScheduledTask makeTask(String id) {
    return makeTask(id, "role-" + id, "env-" + id, "job-" + id);
  }
}

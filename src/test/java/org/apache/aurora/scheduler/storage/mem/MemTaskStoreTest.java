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
package org.apache.aurora.scheduler.storage.mem;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.concurrent.ExecutorServiceShutdown;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.TaskStore.Mutable.TaskMutation;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
  public void testQueryBySlaveHost() {
    String hostA = "slaveA";
    String hostB = "slaveB";
    final IScheduledTask a = setHost(makeTask("a", "role", "env", "job"), Optional.of(hostA));
    final IScheduledTask b = setHost(makeTask("b", "role", "env", "job"), Optional.of(hostB));
    store.saveTasks(ImmutableSet.of(a, b));

    assertQueryResults(Query.slaveScoped(hostA), a);
    assertQueryResults(Query.slaveScoped(hostA, hostB), a, b);
  }

  @Test
  public void testQueryByJobKeys() {
    assertStoreContents();
    store.saveTasks(ImmutableSet.of(TASK_A, TASK_B, TASK_C, TASK_D));

    assertQueryResults(
        Query.jobScoped(ImmutableSet.of(
            JobKeys.from("role-a", "env-a", "job-a"),
            JobKeys.from("role-b", "env-b", "job-b"),
            JobKeys.from("role-c", "env-c", "job-c"))),
        TASK_A, TASK_B, TASK_C);

    // Conflicting jobs will produce the result from the last added JobKey
    assertQueryResults(
          Query.jobScoped(JobKeys.from("role-a", "env-a", "job-a"))
              .byJobKeys(ImmutableSet.of(JobKeys.from("role-b", "env-b", "job-b"))),
        TASK_B);

    // The .byJobKeys will override the previous scoping and OR all of the keys.
    assertQueryResults(
        Query.jobScoped(JobKeys.from("role-a", "env-a", "job-a"))
            .byJobKeys(ImmutableSet.of(
                JobKeys.from("role-b", "env-b", "job-b"),
                JobKeys.from("role-a", "env-a", "job-a"))),
        TASK_A, TASK_B);

    // Combination of individual field and jobKeys is allowed.
    assertQueryResults(
        Query.roleScoped("role-b")
            .byJobKeys(ImmutableSet.of(
                JobKeys.from("role-b", "env-b", "job-b"),
                JobKeys.from("role-a", "env-a", "job-a"))),
        TASK_B);
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
      @Override
      public IScheduledTask apply(IScheduledTask task) {
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

  private static IScheduledTask setHost(IScheduledTask task, Optional<String> host) {
    ScheduledTask builder = task.newBuilder();
    builder.getAssignedTask().setSlaveHost(host.orNull());
    return IScheduledTask.build(builder);
  }

  private static IScheduledTask setConfigData(IScheduledTask task, String configData) {
    ScheduledTask builder = task.newBuilder();
    builder.getAssignedTask().getTask().getExecutorConfig().setData(configData);
    return IScheduledTask.build(builder);
  }

  @Test
  public void testAddSlaveHost() {
    final IScheduledTask a = makeTask("a", "role", "env", "job");
    store.saveTasks(ImmutableSet.of(a));
    String host = "slaveA";
    assertQueryResults(Query.slaveScoped(host));

    final IScheduledTask b = setHost(a, Optional.of(host));
    Set<IScheduledTask> result = store.mutateTasks(Query.taskScoped(Tasks.id(a)),
        new Function<IScheduledTask, IScheduledTask>() {
          @Override
          public IScheduledTask apply(IScheduledTask task) {
            assertEquals(a, task);
            return b;
          }
        });
    assertEquals(ImmutableSet.of(b), result);
    assertQueryResults(Query.slaveScoped(host), b);

    // Unrealistic behavior, but proving that the secondary index can handle key mutations.
    String host2 = "slaveA2";
    final IScheduledTask c = setHost(b, Optional.of(host2));
    Set<IScheduledTask> result2 = store.mutateTasks(Query.taskScoped(Tasks.id(a)),
        new Function<IScheduledTask, IScheduledTask>() {
          @Override
          public IScheduledTask apply(IScheduledTask task) {
            assertEquals(b, task);
            return c;
          }
        });
    assertEquals(ImmutableSet.of(c), result2);
    assertQueryResults(Query.slaveScoped(host2), c);

    store.deleteTasks(ImmutableSet.of(Tasks.id(a)));
    assertQueryResults(Query.slaveScoped(host));
  }

  @Test
  public void testUnsetSlaveHost() {
    // Unrealistic behavior, but proving that the secondary index does not become stale.

    String host = "slaveA";
    final IScheduledTask a = setHost(makeTask("a", "role", "env", "job"), Optional.of(host));
    store.saveTasks(ImmutableSet.of(a));
    assertQueryResults(Query.slaveScoped(host), a);

    final IScheduledTask b = setHost(a, Optional.<String>absent());
    Set<IScheduledTask> result = store.mutateTasks(Query.taskScoped(Tasks.id(a)),
        new Function<IScheduledTask, IScheduledTask>() {
          @Override
          public IScheduledTask apply(IScheduledTask task) {
            assertEquals(a, task);
            return b;
          }
        });
    assertEquals(ImmutableSet.of(b), result);
    assertQueryResults(Query.slaveScoped(host));
    assertQueryResults(Query.taskScoped(Tasks.id(b)), b);
  }

  @Test
  public void testTasksOnSameHost() {
    String host = "slaveA";
    final IScheduledTask a = setHost(makeTask("a", "role", "env", "job"), Optional.of(host));
    final IScheduledTask b = setHost(makeTask("b", "role", "env", "job"), Optional.of(host));
    store.saveTasks(ImmutableSet.of(a, b));
    assertQueryResults(Query.slaveScoped(host), a, b);
  }

  @Test
  public void testSaveOverwrites() {
    // Ensures that saving a task with an existing task ID is effectively the same as a mutate,
    // and does not result in a duplicate object in the primary or secondary index.

    String host = "slaveA";
    final IScheduledTask a = setHost(makeTask("a", "role", "env", "job"), Optional.of(host));
    store.saveTasks(ImmutableSet.of(a));

    final IScheduledTask updated = setConfigData(a, "new config data");
    store.saveTasks(ImmutableSet.of(updated));
    assertQueryResults(Query.taskScoped(Tasks.id(a)), updated);
    assertQueryResults(Query.slaveScoped(host), updated);
  }

  @Test
  public void testReadSecondaryIndexMultipleThreads() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(4,
        new ThreadFactoryBuilder().setNameFormat("SlowRead-%d").setDaemon(true).build());

    try {
      ImmutableSet.Builder<IScheduledTask> builder = ImmutableSet.builder();
      final int numTasks = 100;
      final int numJobs = 100;
      for (int j = 0; j < numJobs; j++) {
        for (int t = 0; t < numTasks; t++) {
          builder.add(makeTask("" + j + "-" + t, "role", "env", "name" + j));
        }
      }
      store.saveTasks(builder.build());

      final CountDownLatch read = new CountDownLatch(numJobs);
      for (int j = 0; j < numJobs; j++) {
        final int id = j;
        executor.submit(new Runnable() {
          @Override
          public void run() {
            assertNotNull(store.fetchTasks(Query.jobScoped(
                JobKeys.from("role", "env", "name" + id))));
            read.countDown();
          }
        });
        executor.submit(new Runnable() {
          @Override
          public void run() {
            store.saveTasks(ImmutableSet.of(makeTask("TaskNew1" + id)));
          }
        });
      }

      read.await();
    } finally {
      new ExecutorServiceShutdown(executor, Amount.of(1L, Time.SECONDS)).execute();
    }
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
                .setOwner(new Identity(role, role))
                .setExecutorConfig(new ExecutorConfig().setData("executor config")))));
  }

  private static IScheduledTask makeTask(String id) {
    return makeTask(id, "role-" + id, "env-" + id, "job-" + id);
  }
}

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
package org.apache.aurora.scheduler.storage;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

import org.apache.aurora.common.testing.TearDownTestCase;
import org.apache.aurora.gen.AppcImage;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.DockerImage;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.Image;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.MesosContainer;
import org.apache.aurora.gen.Metadata;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.TaskStore.Mutable.TaskMutation;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.testing.StorageEntityUtil;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.ASSIGNED;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.scheduler.base.TaskTestUtil.makeTask;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractTaskStoreTest extends TearDownTestCase {
  protected static final IHostAttributes HOST_A = IHostAttributes.build(
      new HostAttributes(
          "hostA",
          ImmutableSet.of(new Attribute("zone", ImmutableSet.of("1a"))))
          .setSlaveId("slaveIdA")
          .setMode(MaintenanceMode.NONE));
  protected static final IHostAttributes HOST_B = IHostAttributes.build(
      new HostAttributes(
          "hostB",
          ImmutableSet.of(new Attribute("zone", ImmutableSet.of("1a"))))
          .setSlaveId("slaveIdB")
          .setMode(MaintenanceMode.NONE));
  protected static final IScheduledTask TASK_A = createTask("a");
  protected static final IScheduledTask TASK_B =
      setContainer(createTask("b"), Container.mesos(new MesosContainer()));
  protected static final IScheduledTask TASK_C = createTask("c");
  protected static final IScheduledTask TASK_D = createTask("d");

  protected Injector injector;
  protected Storage storage;

  protected abstract Module getStorageModule();

  @Before
  public void baseSetUp() {
    injector = Guice.createInjector(getStorageModule());
    storage = injector.getInstance(Storage.class);
    storage.prepare();

    storage.write((NoResult.Quiet) storeProvider -> {
      AttributeStore.Mutable attributeStore = storeProvider.getAttributeStore();
      attributeStore.saveHostAttributes(HOST_A);
      attributeStore.saveHostAttributes(HOST_B);
    });
  }

  private Optional<IScheduledTask> fetchTask(String taskId) {
    return storage.read(storeProvider -> storeProvider.getTaskStore().fetchTask(taskId));
  }

  private Iterable<IScheduledTask> fetchTasks(Query.Builder query) {
    return storage.read(storeProvider -> storeProvider.getTaskStore().fetchTasks(query));
  }

  protected void saveTasks(IScheduledTask... tasks) {
    saveTasks(ImmutableSet.copyOf(tasks));
  }

  private void saveTasks(Set<IScheduledTask> tasks) {
    storage.write((NoResult.Quiet)
        storeProvider -> storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.copyOf(tasks)));
  }

  private Optional<IScheduledTask> mutateTask(String taskId, TaskMutation mutation) {
    return storage.write(
        storeProvider -> storeProvider.getUnsafeTaskStore().mutateTask(taskId, mutation));
  }

  private boolean unsafeModifyInPlace(String taskId, ITaskConfig taskConfiguration) {
    return storage.write(storeProvider ->
        storeProvider.getUnsafeTaskStore().unsafeModifyInPlace(taskId, taskConfiguration));
  }

  protected void deleteTasks(String... taskIds) {
    storage.write((NoResult.Quiet) storeProvider ->
        storeProvider.getUnsafeTaskStore().deleteTasks(ImmutableSet.copyOf(taskIds)));
  }

  protected void deleteAllTasks() {
    storage.write(
        (NoResult.Quiet) storeProvider -> storeProvider.getUnsafeTaskStore().deleteAllTasks());
  }

  @Test
  public void testSave() {
    IScheduledTask aWithHost = setHost(TASK_A, HOST_A);
    StorageEntityUtil.assertFullyPopulated(
        aWithHost.newBuilder(),
        StorageEntityUtil.getField(TaskConfig.class, "image"));

    saveTasks(aWithHost, TASK_B);
    assertStoreContents(aWithHost, TASK_B);

    saveTasks(TASK_C, TASK_D);
    assertStoreContents(aWithHost, TASK_B, TASK_C, TASK_D);

    // Saving the same task should overwrite.
    IScheduledTask taskAModified = IScheduledTask.build(aWithHost.newBuilder().setStatus(RUNNING));
    saveTasks(taskAModified);
    assertStoreContents(taskAModified, TASK_B, TASK_C, TASK_D);
  }

  @Test
  public void testSaveWithMetadata() {
    ScheduledTask builder = TASK_A.newBuilder();
    builder.getAssignedTask().getTask().setMetadata(
        ImmutableSet.of(
            new Metadata("package", "a"),
            new Metadata("package", "b")));
    IScheduledTask task = IScheduledTask.build(builder);
    saveTasks(task);
    assertStoreContents(task);
  }

  @Test
  public void testSaveWithDockerImage() {
    ScheduledTask builder = TASK_A.newBuilder();

    Image image = new Image();
    image.setDocker(new DockerImage().setName("some-name").setTag("some-tag"));
    builder.getAssignedTask().getTask().setImage(image);

    IScheduledTask task = IScheduledTask.build(builder);
    saveTasks(task);
    assertStoreContents(task);
  }

  @Test
  public void testSaveWithAppcImage() {
    ScheduledTask builder = TASK_A.newBuilder();

    Image image = new Image();
    image.setAppc(new AppcImage().setName("some-name").setImageId("some-tag"));
    builder.getAssignedTask().getTask().setImage(image);

    IScheduledTask task = IScheduledTask.build(builder);
    saveTasks(task);
    assertStoreContents(task);
  }

  @Test
  public void testQuery() {
    assertStoreContents();
    saveTasks(TASK_A, TASK_B, TASK_C, TASK_D);

    assertQueryResults(Query.taskScoped("b"), TASK_B);
    assertQueryResults(Query.taskScoped("a", "d"), TASK_A, TASK_D);
    assertQueryResults(Query.roleScoped("role-c"), TASK_C);
    assertQueryResults(Query.envScoped("role-c", "env-c"), TASK_C);
    assertQueryResults(Query.envScoped("role-c", "devel"));
    assertQueryResults(
        Query.unscoped().byStatus(ASSIGNED),
        TASK_A, TASK_B, TASK_C, TASK_D);
    assertQueryResults(
        Query.instanceScoped(JobKeys.from("role-a", "env-a", "job-a"), 2).active(), TASK_A);
    assertQueryResults(Query.jobScoped(JobKeys.from("role-b", "env-b", "job-b")).active(), TASK_B);
    assertQueryResults(Query.jobScoped(JobKeys.from("role-b", "devel", "job-b")).active());

    // Explicitly call out the matching behaviors for different means of expressing empty query
    // conditions. Specifically - neither null collections nor empty collections should trigger
    // query restrictions and thus they should yield the same results.
    assertQueryResults(new TaskQuery().setTaskIds(null), TASK_A, TASK_B, TASK_C, TASK_D);
    assertQueryResults(new TaskQuery().setTaskIds(ImmutableSet.of()),
        TASK_A, TASK_B, TASK_C, TASK_D);

    assertQueryResults(new TaskQuery().setInstanceIds(null),  TASK_A, TASK_B, TASK_C, TASK_D);
    assertQueryResults(
        new TaskQuery().setInstanceIds(ImmutableSet.of()),
        TASK_A, TASK_B, TASK_C, TASK_D);

    assertQueryResults(new TaskQuery().setStatuses(null), TASK_A, TASK_B, TASK_C, TASK_D);
    assertQueryResults(
        new TaskQuery().setStatuses(ImmutableSet.of()),
        TASK_A, TASK_B, TASK_C, TASK_D);

    assertQueryResults(new TaskQuery().setSlaveHosts(null), TASK_A, TASK_B, TASK_C, TASK_D);
    assertQueryResults(new TaskQuery().setSlaveHosts(ImmutableSet.of()),
        TASK_A, TASK_B, TASK_C, TASK_D);

    assertQueryResults(new TaskQuery().setJobKeys(null), TASK_A, TASK_B, TASK_C, TASK_D);
    assertQueryResults(new TaskQuery().setJobKeys(ImmutableSet.of()),
        TASK_A, TASK_B, TASK_C, TASK_D);
  }

  @Test
  public void testQueryMultipleInstances() {
    ImmutableSet.Builder<IScheduledTask> tasksBuilder = ImmutableSet.builder();
    for (int i = 0; i < 100; i++) {
      ScheduledTask builder = TASK_A.newBuilder();
      builder.getAssignedTask()
          .setTaskId("id" + i)
          .setInstanceId(i);
      tasksBuilder.add(IScheduledTask.build(builder));
    }
    Set<IScheduledTask> tasks = tasksBuilder.build();
    saveTasks(tasks);
    assertQueryResults(Query.unscoped(), tasks);
  }

  @Test
  public void testQueryBySlaveHost() {
    IScheduledTask a = setHost(makeTask("a", JobKeys.from("role", "env", "job")), HOST_A);
    IScheduledTask b = setHost(makeTask("b", JobKeys.from("role", "env", "job")), HOST_B);
    saveTasks(a, b);

    assertQueryResults(Query.slaveScoped(HOST_A.getHost()), a);
    assertQueryResults(Query.slaveScoped(HOST_A.getHost(), HOST_B.getHost()), a, b);
  }

  @Test
  public void testQueryByJobKeys() {
    assertStoreContents();
    saveTasks(TASK_A, TASK_B, TASK_C, TASK_D);

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
    saveTasks(TASK_A, TASK_B, TASK_C, TASK_D);
    assertQueryResults(Query.statusScoped(RUNNING));

    mutateTask("a", task -> IScheduledTask.build(task.newBuilder().setStatus(RUNNING)));

    assertQueryResults(
        Query.statusScoped(RUNNING),
        IScheduledTask.build(TASK_A.newBuilder().setStatus(RUNNING)));

    assertEquals(
        Optional.absent(),
        mutateTask(
            "nonexistent",
            task -> IScheduledTask.build(task.newBuilder().setStatus(RUNNING))));

    assertStoreContents(
        IScheduledTask.build(TASK_A.newBuilder().setStatus(RUNNING)),
        IScheduledTask.build(TASK_B.newBuilder().setStatus(ASSIGNED)),
        IScheduledTask.build(TASK_C.newBuilder().setStatus(ASSIGNED)),
        IScheduledTask.build(TASK_D.newBuilder().setStatus(ASSIGNED)));
  }

  @Test
  public void testUnsafeModifyInPlace() {
    ITaskConfig updated = ITaskConfig.build(
        TASK_A.getAssignedTask()
            .getTask()
            .newBuilder()
            .setExecutorConfig(new ExecutorConfig("aurora", "new_config")));

    String taskId = Tasks.id(TASK_A);
    assertFalse(unsafeModifyInPlace(taskId, updated));

    saveTasks(TASK_A);
    assertTrue(unsafeModifyInPlace(taskId, updated));
    assertEquals(updated, fetchTask(taskId).get().getAssignedTask().getTask());

    deleteTasks(taskId);
    assertFalse(unsafeModifyInPlace(taskId, updated));
  }

  @Test
  public void testDelete() {
    saveTasks(TASK_A, TASK_B, TASK_C, TASK_D);
    deleteTasks("a");
    assertStoreContents(TASK_B, TASK_C, TASK_D);
    deleteTasks("c");
    assertStoreContents(TASK_B, TASK_D);
    deleteTasks("b", "d");
    assertStoreContents();
  }

  @Test
  public void testDeleteAll() {
    saveTasks(TASK_A, TASK_B, TASK_C, TASK_D);
    deleteAllTasks();
    assertStoreContents();
  }

  @Test
  public void testConsistentJobIndex() {
    IScheduledTask a = makeTask("a", JobKeys.from("jim", "test", "job"));
    IScheduledTask b = makeTask("b", JobKeys.from("jim", "test", "job"));
    IScheduledTask c = makeTask("c", JobKeys.from("jim", "test", "job2"));
    IScheduledTask d = makeTask("d", JobKeys.from("joe", "test", "job"));
    IScheduledTask e = makeTask("e", JobKeys.from("jim", "prod", "job"));
    Query.Builder jimsJob = Query.jobScoped(JobKeys.from("jim", "test", "job"));
    Query.Builder jimsJob2 = Query.jobScoped(JobKeys.from("jim", "test", "job2"));
    Query.Builder joesJob = Query.jobScoped(JobKeys.from("joe", "test", "job"));

    saveTasks(a, b, c, d, e);
    assertQueryResults(jimsJob, a, b);
    assertQueryResults(jimsJob2, c);
    assertQueryResults(joesJob, d);

    deleteTasks(Tasks.id(b));
    assertQueryResults(jimsJob, a);
    assertQueryResults(jimsJob2, c);
    assertQueryResults(joesJob, d);

    mutateTask(Tasks.id(a), task -> IScheduledTask.build(task.newBuilder().setStatus(RUNNING)));
    IScheduledTask aRunning = IScheduledTask.build(a.newBuilder().setStatus(RUNNING));
    assertQueryResults(jimsJob, aRunning);
    assertQueryResults(jimsJob2, c);
    assertQueryResults(joesJob, d);

    deleteTasks(Tasks.id(d));
    assertQueryResults(joesJob);

    deleteTasks(Tasks.id(d));
    assertQueryResults(jimsJob, aRunning);
    assertQueryResults(jimsJob2, c);
    assertQueryResults(joesJob);

    saveTasks(b);
    assertQueryResults(jimsJob, aRunning, b);
    assertQueryResults(jimsJob2, c);
    assertQueryResults(joesJob);
  }

  @Test
  public void testCanonicalTaskConfigs() {
    IScheduledTask a = createTask("a");
    IScheduledTask b = createTask("a");
    IScheduledTask c = createTask("a");
    saveTasks(a, b, c);
    Set<IScheduledTask> inserted = ImmutableSet.of(a, b, c);

    Set<ITaskConfig> storedConfigs = FluentIterable.from(fetchTasks(Query.unscoped()))
        .transform(Tasks::getConfig)
        .toSet();
    assertEquals(
        FluentIterable.from(inserted).transform(Tasks::getConfig).toSet(),
        storedConfigs);
    Map<ITaskConfig, ITaskConfig> identityMap = Maps.newIdentityHashMap();
    for (ITaskConfig stored : storedConfigs) {
      identityMap.put(stored, stored);
    }
    assertEquals(
        ImmutableMap.of(Tasks.getConfig(a), Tasks.getConfig(a)),
        identityMap);
  }

  private static IScheduledTask setHost(IScheduledTask task, IHostAttributes host) {
    ScheduledTask builder = task.newBuilder();
    builder.getAssignedTask()
        .setSlaveHost(host.getHost())
        .setSlaveId(host.getSlaveId());
    return IScheduledTask.build(builder);
  }

  private static IScheduledTask unsetHost(IScheduledTask task) {
    ScheduledTask builder = task.newBuilder();
    builder.getAssignedTask()
        .setSlaveHost(null)
        .setSlaveId(null);
    return IScheduledTask.build(builder);
  }

  private static IScheduledTask setConfigData(IScheduledTask task, String configData) {
    ScheduledTask builder = task.newBuilder();
    builder.getAssignedTask().getTask().getExecutorConfig().setData(configData);
    return IScheduledTask.build(builder);
  }

  @Test
  public void testAddSlaveHost() {
    final IScheduledTask a = createTask("a");
    saveTasks(a);
    assertQueryResults(Query.slaveScoped(HOST_A.getHost()));

    final IScheduledTask b = setHost(a, HOST_A);
    Optional<IScheduledTask> result = mutateTask(Tasks.id(a),
        task -> {
          assertEquals(a, task);
          return b;
        });
    assertEquals(Optional.of(b), result);
    assertQueryResults(Query.slaveScoped(HOST_A.getHost()), b);

    // Unrealistic behavior, but proving that the secondary index can handle key mutations.
    final IScheduledTask c = setHost(b, HOST_B);
    Optional<IScheduledTask> result2 = mutateTask(Tasks.id(a),
        task -> {
          assertEquals(b, task);
          return c;
        });
    assertEquals(Optional.of(c), result2);
    assertQueryResults(Query.slaveScoped(HOST_B.getHost()), c);

    deleteTasks(Tasks.id(a));
    assertQueryResults(Query.slaveScoped(HOST_B.getHost()));
  }

  @Test
  public void testUnsetSlaveHost() {
    // Unrealistic behavior, but proving that the secondary index does not become stale.

    final IScheduledTask a = setHost(createTask("a"), HOST_A);
    saveTasks(a);
    assertQueryResults(Query.slaveScoped(HOST_A.getHost()), a);

    final IScheduledTask b = unsetHost(a);
    Optional<IScheduledTask> result = mutateTask(Tasks.id(a),
        task -> {
          assertEquals(a, task);
          return b;
        });
    assertEquals(Optional.of(b), result);
    assertQueryResults(Query.slaveScoped(HOST_A.getHost()));
    assertQueryResults(Query.taskScoped(Tasks.id(b)), b);
  }

  @Test
  public void testTasksOnSameHost() {
    final IScheduledTask a = setHost(createTask("a"), HOST_A);
    final IScheduledTask b = setHost(createTask("b"), HOST_A);
    saveTasks(a, b);
    assertQueryResults(Query.slaveScoped(HOST_A.getHost()), a, b);
  }

  @Test
  public void testSaveOverwrites() {
    // Ensures that saving a task with an existing task ID is effectively the same as a mutate,
    // and does not result in a duplicate object in the primary or secondary index.

    final IScheduledTask a = setHost(createTask("a"), HOST_A);
    saveTasks(a);

    final IScheduledTask updated = setConfigData(a, "new config data");
    saveTasks(updated);
    assertQueryResults(Query.taskScoped(Tasks.id(a)), updated);
    assertQueryResults(Query.slaveScoped(HOST_A.getHost()), updated);
  }

  private Set<IJobKey> getJobKeys() {
    return storage.read(storeProvider -> storeProvider.getTaskStore().getJobKeys());
  }

  private Set<IJobKey> toJobKeys(IScheduledTask... tasks) {
    return FluentIterable.from(ImmutableSet.copyOf(tasks))
        .transform(Tasks::getJob)
        .toSet();
  }

  @Test
  public void testGetsJobKeys() {
    assertEquals(ImmutableSet.of(), getJobKeys());
    saveTasks(TASK_A);
    assertEquals(toJobKeys(TASK_A), getJobKeys());
    saveTasks(TASK_B, TASK_C);
    assertEquals(toJobKeys(TASK_A, TASK_B, TASK_C), getJobKeys());
    deleteTasks(Tasks.id(TASK_B));
    assertEquals(toJobKeys(TASK_A, TASK_C), getJobKeys());
    IJobKey multiInstanceJob = JobKeys.from("role", "env", "instances");
    saveTasks(
        makeTask("instance1", multiInstanceJob),
        makeTask("instance2", multiInstanceJob),
        makeTask("instance3", multiInstanceJob));
    assertEquals(
        ImmutableSet.builder().addAll(toJobKeys(TASK_A, TASK_C)).add(multiInstanceJob).build(),
        getJobKeys());
  }

  @Ignore
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
          builder.add(makeTask("" + j + "-" + t, JobKeys.from("role", "env", "name" + j)));
        }
      }
      saveTasks(builder.build());

      final CountDownLatch read = new CountDownLatch(numJobs);
      for (int j = 0; j < numJobs; j++) {
        final int id = j;
        executor.submit(() -> {
          assertNotNull(fetchTasks(Query.jobScoped(JobKeys.from("role", "env", "name" + id))));
          read.countDown();
        });
        executor.submit(() -> saveTasks(createTask("TaskNew1" + id)));
      }

      read.await();
    } finally {
      MoreExecutors.shutdownAndAwaitTermination(executor, 1, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testNullVsEmptyRelations() throws Exception {
    // Test for regression of AURORA-1476.

    ITaskConfig nullMetadata =
        ITaskConfig.build(TaskTestUtil.makeConfig(TaskTestUtil.JOB).newBuilder().setMetadata(null));

    IScheduledTask a = makeTask("a", nullMetadata);
    IScheduledTask b = makeTask("a", nullMetadata);
    IScheduledTask c = makeTask("a", nullMetadata);
    saveTasks(a);
    saveTasks(b);
    saveTasks(c);
  }

  @Test
  public void testConcurrentFetchTasks() throws Exception {
    // Test for regression of AURORA-1625
    ListeningExecutorService executor =
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(5));
    assertStoreContents();
    saveTasks(TASK_A, TASK_B, TASK_C, TASK_D);

    List<ListenableFuture<Integer>> futures = Lists.newArrayList();

    for (int i = 0; i < 100; i++) {
      futures.add(executor.submit(() -> Iterables.size(fetchTasks(Query.unscoped()))));
    }

    Future<List<Integer>> f = Futures.allAsList(futures);

    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.MINUTES);

    assertEquals(Iterables.getOnlyElement(ImmutableSet.copyOf(f.get())), (Integer) 4);
  }

  private void assertStoreContents(IScheduledTask... tasks) {
    assertQueryResults(Query.unscoped(), tasks);
  }

  private void assertQueryResults(TaskQuery query, IScheduledTask... tasks) {
    assertQueryResults(Query.arbitrary(query), tasks);
  }

  private void assertQueryResults(Query.Builder query, IScheduledTask... tasks) {
    assertQueryResults(query, ImmutableSet.copyOf(tasks));
  }

  private void assertQueryResults(Query.Builder query, Set<IScheduledTask> tasks) {
    Iterable<IScheduledTask> result = fetchTasks(query);
    assertQueryHasNoDupes(result);
    assertEquals(tasks, ImmutableSet.copyOf(fetchTasks(query)));
  }

  private void assertQueryHasNoDupes(Iterable<IScheduledTask> result) {
    assertEquals(ImmutableList.copyOf(result).size(), ImmutableSet.copyOf(result).size());
  }

  private static IScheduledTask createTask(String id) {
    return makeTask(id, JobKeys.from("role-" + id, "env-" + id, "job-" + id));
  }

  private static IScheduledTask setContainer(IScheduledTask task, Container container) {
    ScheduledTask builder = task.newBuilder();
    builder.getAssignedTask().getTask().setContainer(container);
    return IScheduledTask.build(builder);
  }
}

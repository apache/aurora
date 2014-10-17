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

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.twitter.common.util.Clock;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.configuration.SanitizedConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.entities.ITaskEvent;
import org.apache.aurora.scheduler.storage.mem.MemStorage;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.FINISHED;
import static org.apache.aurora.gen.ScheduleStatus.KILLED;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.gen.ScheduleStatus.SANDBOX_DELETED;
import static org.junit.Assert.assertEquals;

public class StorageBackfillTest {
  private static final String ROLE = "Test_Role_A";
  private static final String USER = "Test_User_A";
  private static final Identity OWNER = new Identity(ROLE, USER);
  private static final String ENV = "Test_Env";
  private static final String JOB_NAME = "Test_Job";
  private static final IJobKey JOB_KEY = JobKeys.from(ROLE, ENV, JOB_NAME);
  private static final int ONE_GB = 1024;
  private static final String TASK_ID = "task_id";
  private static final ExecutorConfig EXECUTOR_CONFIG =
      new ExecutorConfig("AuroraExecutor", "executorConfig");

  private Storage storage;
  private Clock clock;

  @Before
  public void setUp() {
    storage = MemStorage.newEmptyStorage();
    clock = new FakeClock();
  }

  @Test
  public void testRewriteSandboxDeleted() throws Exception {
    final TaskConfig storedTask = defaultTask();
    final TaskEvent expectedEvent = new TaskEvent(100, FINISHED);

    storage.write(new Storage.MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(Storage.MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(
            IScheduledTask.build(new ScheduledTask()
                .setStatus(SANDBOX_DELETED)
                .setTaskEvents(ImmutableList.of(expectedEvent, new TaskEvent(200, SANDBOX_DELETED)))
                .setAssignedTask(new AssignedTask()
                    .setTaskId(TASK_ID)
                    .setInstanceId(0)
                    .setTask(storedTask)))));
      }
    });

    backfill();

    assertEquals(FINISHED, getTask(TASK_ID).getStatus());
    assertEquals(
        ImmutableList.of(ITaskEvent.build(expectedEvent)),
        getTask(TASK_ID).getTaskEvents());
  }

  @Test
  public void testLoadTasksFromStorage() throws Exception {
    final TaskConfig storedTask = defaultTask();

    storage.write(new Storage.MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(Storage.MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(
            IScheduledTask.build(new ScheduledTask()
                .setStatus(PENDING)
                .setTaskEvents(ImmutableList.of(new TaskEvent(100, PENDING)))
                .setAssignedTask(new AssignedTask()
                    .setTaskId(TASK_ID)
                    .setInstanceId(0)
                    .setTask(storedTask)))));
      }
    });

    backfill();

    // Since task fields are backfilled with defaults, additional flags should be filled.
    ITaskConfig expected = ITaskConfig.build(new TaskConfig(storedTask)
        .setProduction(false)
        .setMaxTaskFailures(1)
        .setExecutorConfig(EXECUTOR_CONFIG)
        .setConstraints(ImmutableSet.of(ConfigurationManager.hostLimitConstraint(1))));

    assertEquals(expected, getTask(TASK_ID).getAssignedTask().getTask());
  }

  @Test
  public void testShardUniquenessCorrection() throws Exception {

    final AtomicInteger taskId = new AtomicInteger();

    final TaskConfig task = defaultTask();
    SanitizedConfiguration job = makeJob(JOB_KEY, task, 10);
    final Set<IScheduledTask> badTasks = ImmutableSet.copyOf(Iterables.transform(
        job.getInstanceIds(),
        new Function<Integer, IScheduledTask>() {
          @Override
          public IScheduledTask apply(Integer instanceId) {
            return IScheduledTask.build(new ScheduledTask()
                .setStatus(RUNNING)
                .setAssignedTask(new AssignedTask()
                    .setInstanceId(0)
                    .setTaskId("task-" + taskId.incrementAndGet())
                    .setTask(task)));
          }
        }));

    storage.write(new Storage.MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(Storage.MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(badTasks);
      }
    });

    backfill();

    assertEquals(1, getTasksByStatus(RUNNING).size());
    assertEquals(9, getTasksByStatus(KILLED).size());
  }

  private void backfill() {
    storage.write(new Storage.MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(Storage.MutableStoreProvider storeProvider) {
        StorageBackfill.backfill(storeProvider, clock);
      }
    });
  }

  private static SanitizedConfiguration makeJob(
      IJobKey jobKey,
      TaskConfig task,
      int numTasks) throws Exception {

    JobConfiguration job = new JobConfiguration()
        .setOwner(OWNER)
        .setKey(jobKey.newBuilder())
        .setInstanceCount(numTasks)
        .setTaskConfig(new TaskConfig(task)
            .setOwner(OWNER)
            .setEnvironment(jobKey.getEnvironment())
            .setJobName(jobKey.getName()));
    return SanitizedConfiguration.fromUnsanitized(IJobConfiguration.build(job));
  }

  private static TaskConfig defaultTask() {
    return new TaskConfig()
        .setOwner(OWNER)
        .setJobName(JOB_NAME)
        .setEnvironment(ENV)
        .setNumCpus(1.0)
        .setRamMb(ONE_GB)
        .setDiskMb(500)
        .setExecutorConfig(EXECUTOR_CONFIG)
        .setRequestedPorts(ImmutableSet.<String>of())
        .setConstraints(ImmutableSet.<Constraint>of())
        .setTaskLinks(ImmutableMap.<String, String>of());
  }

  private IScheduledTask getTask(String taskId) {
    return Iterables.getOnlyElement(Storage.Util.consistentFetchTasks(
        storage,
        Query.taskScoped(taskId)));
  }

  private Set<IScheduledTask> getTasksByStatus(ScheduleStatus status) {
    return Storage.Util.consistentFetchTasks(storage, Query.unscoped().byStatus(status));
  }
}

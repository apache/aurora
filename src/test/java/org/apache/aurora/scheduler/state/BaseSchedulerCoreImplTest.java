/**
 * Copyright 2013 Apache Software Foundation
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
package org.apache.aurora.scheduler.state;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.base.Closure;
import com.twitter.common.collections.Pair;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.CronCollisionPolicy;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.gen.ValueConstraint;
import org.apache.aurora.scheduler.Driver;
import org.apache.aurora.scheduler.TaskIdGenerator;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.ScheduleException;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import org.apache.aurora.scheduler.configuration.SanitizedConfiguration;
import org.apache.aurora.scheduler.cron.CronScheduler;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.state.JobFilter.JobFilterResult;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.StorageBackfill;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IIdentity;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.entities.ITaskEvent;

import org.apache.mesos.Protos.SlaveID;

import org.easymock.EasyMock;

import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.ASSIGNED;
import static org.apache.aurora.gen.ScheduleStatus.FAILED;
import static org.apache.aurora.gen.ScheduleStatus.FINISHED;
import static org.apache.aurora.gen.ScheduleStatus.KILLED;
import static org.apache.aurora.gen.ScheduleStatus.KILLING;
import static org.apache.aurora.gen.ScheduleStatus.LOST;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.RESTARTING;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.gen.ScheduleStatus.STARTING;
import static org.apache.aurora.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;
import static org.apache.aurora.scheduler.configuration.ConfigurationManager.hostLimitConstraint;
import static org.apache.aurora.scheduler.configuration.ConfigurationManager.validateAndPopulate;

import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Base integration test for the SchedulerCoreImpl, subclasses should supply a concrete Storage
 * system.
 */
public abstract class BaseSchedulerCoreImplTest extends EasyMockTest {

  private static final String ROLE_A = "Test_Role_A";
  private static final String USER_A = "Test_User_A";
  private static final Identity OWNER_A = new Identity(ROLE_A, USER_A);
  private static final String ENV_A = "Test_Env_A";
  private static final String JOB_A = "Test_Job_A";
  private static final IJobKey KEY_A = JobKeys.from(ROLE_A, ENV_A, JOB_A);
  private static final int ONE_GB = 1024;

  private static final String ROLE_B = "Test_Role_B";
  private static final IJobKey KEY_B = JobKeys.from(ROLE_B, ENV_A, JOB_A);

  private static final SlaveID SLAVE_ID = SlaveID.newBuilder().setValue("SlaveId").build();
  private static final String SLAVE_HOST_1 = "SlaveHost1";

  private Driver driver;
  private StateManagerImpl stateManager;
  private Storage storage;
  private SchedulerCoreImpl scheduler;
  private CronScheduler cronScheduler;
  private CronJobManager cron;
  private FakeClock clock;
  private Closure<PubsubEvent> eventSink;
  private ShutdownRegistry shutdownRegistry;
  private JobFilter jobFilter;

  // TODO(William Farner): Set up explicit expectations for calls to generate task IDs.
  private final AtomicLong idCounter = new AtomicLong();
  private TaskIdGenerator taskIdGenerator = new TaskIdGenerator() {
    @Override public String generate(ITaskConfig input, int instanceId) {
      return "task-" + idCounter.incrementAndGet();
    }
  };

  @Before
  public void setUp() throws Exception {
    driver = createMock(Driver.class);
    clock = new FakeClock();
    eventSink = createMock(new Clazz<Closure<PubsubEvent>>() { });
    eventSink.execute(EasyMock.<PubsubEvent>anyObject());
    cronScheduler = createMock(CronScheduler.class);
    shutdownRegistry = createMock(ShutdownRegistry.class);
    jobFilter = createMock(JobFilter.class);
    expectLastCall().anyTimes();

    expect(cronScheduler.schedule(anyObject(String.class), anyObject(Runnable.class)))
        .andStubReturn("key");
    expect(cronScheduler.isValidSchedule(anyObject(String.class))).andStubReturn(true);

    expect(jobFilter.filter(anyObject(ITaskConfig.class), anyInt())).andStubReturn(
        JobFilterResult.pass());
  }

  /**
   * Subclasses should create the {@code Storage} implementation to be used by the
   * {@link SchedulerCoreImpl} under test.
   *
   * @return the {@code Storage} for the SchedulerCoreImpl to use under tests
   * @throws Exception if there is a problem creating the storage implementation
   */
  protected abstract Storage createStorage() throws Exception;

  private void buildScheduler() throws Exception {
    buildScheduler(createStorage());
  }

  // TODO(ksweeney): Use Guice to instantiate everything here.
  private void buildScheduler(Storage newStorage) throws Exception {
    this.storage = newStorage;
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        StorageBackfill.backfill(storeProvider, clock);
      }
    });

    stateManager = new StateManagerImpl(storage, clock, driver, taskIdGenerator, eventSink);
    ImmediateJobManager immediateManager = new ImmediateJobManager(stateManager, storage);
    cron = new CronJobManager(stateManager, storage, cronScheduler, shutdownRegistry);
    scheduler = new SchedulerCoreImpl(
        storage,
        cron,
        immediateManager,
        stateManager,
        taskIdGenerator,
        jobFilter);
    cron.schedulerCore = scheduler;
    immediateManager.schedulerCore = scheduler;
  }

  @Test
  public void testCreateJob() throws Exception {
    int numTasks = 10;

    control.replay();
    buildScheduler();

    SanitizedConfiguration job = makeJob(KEY_A, numTasks);
    scheduler.createJob(job);
    assertTaskCount(numTasks);

    Set<IScheduledTask> tasks = Storage.Util.consistentFetchTasks(storage, Query.jobScoped(KEY_A));
    assertEquals(numTasks, tasks.size());
    for (IScheduledTask state : tasks) {
      assertEquals(PENDING, state.getStatus());
      assertTrue(state.getAssignedTask().isSetTaskId());
      assertFalse(state.getAssignedTask().isSetSlaveId());
      assertEquals(
          validateAndPopulate(job.getJobConfig()).getTaskConfig(),
          state.getAssignedTask().getTask());
    }
    Set<Integer> expectedInstanceIds =
        ContiguousSet.create(Range.closedOpen(0, numTasks), DiscreteDomain.integers());
    assertEquals(
        expectedInstanceIds,
        FluentIterable.from(tasks).transform(Tasks.SCHEDULED_TO_INSTANCE_ID).toSet());
  }

  private static Constraint dedicatedConstraint(Set<String> values) {
    return new Constraint(DEDICATED_ATTRIBUTE,
        TaskConstraint.value(new ValueConstraint(false, values)));
  }

  @Test
  public void testDedicatedJob() throws Exception {
    control.replay();
    buildScheduler();

    TaskConfig newTask = nonProductionTask();
    newTask.addToConstraints(dedicatedConstraint(ImmutableSet.of(ROLE_A)));
    scheduler.createJob(makeJob(KEY_A, newTask));
    assertEquals(PENDING, getOnlyTask(Query.jobScoped(KEY_A)).getStatus());
  }

  @Test
  public void testDedicatedJobKey() throws Exception {
    control.replay();
    buildScheduler();

    TaskConfig newTask = nonProductionTask();
    newTask.addToConstraints(dedicatedConstraint(ImmutableSet.of(JobKeys.toPath(KEY_A))));
    scheduler.createJob(makeJob(KEY_A, newTask));
    assertEquals(PENDING, getOnlyTask(Query.jobScoped(KEY_A)).getStatus());
  }

  @Test
  public void testDedicatedArbitrarySuffix() throws Exception {
    control.replay();
    buildScheduler();

    TaskConfig newTask = nonProductionTask();
    newTask.addToConstraints(dedicatedConstraint(ImmutableSet.of(ROLE_A + "/arbitrary")));
    scheduler.createJob(makeJob(KEY_A, newTask, 1));
    assertEquals(PENDING, getOnlyTask(Query.jobScoped(KEY_A)).getStatus());
  }

  @Test
  public void testLoadTasksFromStorage() throws Exception {
    final String storedTaskId = "task_on_disk";

    control.replay();

    storage = createStorage();
    String thermosConfig = "thermosConfig";

    final TaskConfig storedTask = new TaskConfig()
        .setOwner(OWNER_A)
        .setJobName(JOB_A)
        .setEnvironment(ENV_A)
        .setNumCpus(1.0)
        .setRamMb(ONE_GB)
        .setDiskMb(500)
        .setExecutorConfig(new ExecutorConfig("AuroraExecutor", thermosConfig))
        .setRequestedPorts(ImmutableSet.<String>of())
        .setConstraints(ImmutableSet.<Constraint>of())
        .setTaskLinks(ImmutableMap.<String, String>of());

    storage.write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(
            IScheduledTask.build(
              new ScheduledTask()
                  .setStatus(PENDING)
                  .setTaskEvents(ImmutableList.of(new TaskEvent(100, ScheduleStatus.PENDING)))
                  .setAssignedTask(
                      new AssignedTask()
                          .setTaskId(storedTaskId)
                          .setInstanceId(0)
                          .setTask(storedTask)))));
      }
    });

    buildScheduler(storage);

    assignTask(storedTaskId, SLAVE_ID, SLAVE_HOST_1);

    // Since task fields are backfilled with defaults, additional flags should be filled.
    ITaskConfig expected = ITaskConfig.build(new TaskConfig(storedTask)
        .setProduction(false)
        .setMaxTaskFailures(1)
        .setExecutorConfig(new ExecutorConfig("AuroraExecutor", thermosConfig))
        .setConstraints(ImmutableSet.of(ConfigurationManager.hostLimitConstraint(1))));

    assertEquals(expected, getTask(storedTaskId).getAssignedTask().getTask());
    assertEquals(ASSIGNED, getTask(storedTaskId).getStatus());
  }

  private void assignTask(String taskId, SlaveID slaveId, String slaveHost, Set<Integer> ports) {
    stateManager.assignTask(taskId, slaveHost, slaveId, ports);
  }

  private void assignTask(String taskId, SlaveID slaveId, String slaveHost) {
    assignTask(taskId, slaveId, slaveHost, ImmutableSet.<Integer>of());
  }

  @Test
  public void testShardUniquenessCorrection() throws Exception {
    control.replay();

    storage = createStorage();

    final AtomicInteger taskId = new AtomicInteger();

    SanitizedConfiguration job = makeJob(KEY_A, 10);
    final Set<IScheduledTask> badTasks = ImmutableSet.copyOf(Iterables
        .transform(job.getTaskConfigs().values(),
            new Function<ITaskConfig, IScheduledTask>() {
              @Override public IScheduledTask apply(ITaskConfig task) {
                return IScheduledTask.build(new ScheduledTask()
                    .setStatus(RUNNING)
                    .setAssignedTask(
                        new AssignedTask()
                            .setInstanceId(0)
                            .setTaskId("task-" + taskId.incrementAndGet())
                            .setTask(task.newBuilder())));
              }
            }));

    storage.write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(badTasks);
      }
    });

    buildScheduler(storage);
    assertEquals(1, getTasksByStatus(RUNNING).size());
    assertEquals(9, getTasksByStatus(KILLED).size());
  }

  @Test
  public void testRejectsBadIdentifiers() throws Exception {
    control.replay();
    buildScheduler();

    Identity validIdentity = new Identity("foo", "bar");
    Identity[] invalidIdentities = {
      new Identity().setRole("foo"),
      new Identity("foo/", "bar"),
      new Identity("foo", "&bar"),
      new Identity().setUser("bar")
    };

    String validJob = "baz";
    String[] invalidIdentifiers = {"&baz", "/baz", "baz&", ""};

    for (Identity ident : invalidIdentities) {
      for (String env : invalidIdentifiers) {
        for (String job : invalidIdentifiers) {
          // Subvert JobKeys.from to avoid IllegalArgumentExceptions.
          expectRejected(ident, IJobKey.build(new JobKey()
              .setRole(ident.getRole())
              .setEnvironment(env)
              .setName(job)));
        }
      }
    }

    for (String jobName : invalidIdentifiers) {
      expectRejected(validIdentity, IJobKey.build(new JobKey()
          .setRole(validIdentity.getRole())
          .setEnvironment(validJob)
          .setName(jobName)));
    }

    for (Identity ident : invalidIdentities) {
      expectRejected(ident, KEY_A);
    }
  }

  private void expectRejected(Identity identity, IJobKey jobKey) throws ScheduleException {
    try {
      scheduler.createJob(SanitizedConfiguration.fromUnsanitized(IJobConfiguration.build(
          makeJob(jobKey, 1).getJobConfig().newBuilder().setOwner(identity))));
      fail("Job owner/name should have been rejected.");
    } catch (TaskDescriptionException e) {
      // Expected.
    }
  }

  @Test
  public void testSortableTaskIds() throws Exception {
    control.replay();
    buildScheduler();

    for (IScheduledTask task : getTasks(Query.unscoped())) {
      assertEquals(IIdentity.build(OWNER_A), task.getAssignedTask().getTask().getOwner());
    }
  }

  @Test(expected = ScheduleException.class)
  public void testCreateDuplicateJob() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(KEY_A, 1));
    assertTaskCount(1);

    scheduler.createJob(makeJob(KEY_A, 1));
  }

  @Test(expected = ScheduleException.class)
  public void testCreateDuplicateCronJob() throws Exception {
    SanitizedConfiguration sanitizedConfiguration = makeCronJob(KEY_A, 1, "1 1 1 1 1");

    control.replay();
    buildScheduler();

    // Cron jobs are scheduled on a delay, so this job's tasks will not be scheduled immediately,
    // but duplicate jobs should still be rejected.
    scheduler.createJob(sanitizedConfiguration);
    assertTaskCount(0);

    scheduler.createJob(makeJob(KEY_A, 1));
  }

  @Test
  public void testStartCronJob() throws Exception {
    // Create a cron job, ask the scheduler to start it, and ensure that the tasks exist
    // in the PENDING state.

    SanitizedConfiguration sanitizedConfiguration = makeCronJob(KEY_A, 1, "1 1 1 1 1");
    IJobKey jobKey = sanitizedConfiguration.getJobConfig().getKey();

    control.replay();
    buildScheduler();

    scheduler.createJob(sanitizedConfiguration);
    assertTaskCount(0);

    scheduler.startCronJob(jobKey);
    assertEquals(PENDING, getOnlyTask(Query.jobScoped(jobKey)).getStatus());
  }

  @Test(expected = ScheduleException.class)
  public void testStartNonexistentCronJob() throws Exception {
    // Try to start a cron job that doesn't exist.
    control.replay();
    buildScheduler();

    scheduler.startCronJob(KEY_A);
  }

  @Test
  public void testStartNonCronJob() throws Exception {
    // Create a NON cron job and try to start it as though it were a cron job, and ensure that
    // no cron tasks are created.
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(KEY_A, 1));
    String taskId = Tasks.id(getOnlyTask(Query.jobScoped(KEY_A)));

    try {
      scheduler.startCronJob(KEY_A);
      fail("Start should have failed.");
    } catch (ScheduleException e) {
      // Expected.
    }

    assertEquals(PENDING, getTask(taskId).getStatus());
    assertFalse(cron.hasJob(KEY_A));
  }

  @Test(expected = ScheduleException.class)
  public void testStartNonOwnedCronJob() throws Exception {
    // Try to start a cron job that is not owned by us.
    // Should throw an exception.

    SanitizedConfiguration sanitizedConfiguration = makeCronJob(KEY_A, 1, "1 1 1 1 1");
    IJobConfiguration job = sanitizedConfiguration.getJobConfig();
    expect(cronScheduler.isValidSchedule(job.getCronSchedule())).andReturn(true);
    expect(cronScheduler.schedule(eq(job.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");

    control.replay();
    buildScheduler();

    scheduler.createJob(sanitizedConfiguration);
    assertTaskCount(0);

    scheduler.startCronJob(KEY_B);
  }

  @Test
  public void testStartRunningCronJob() throws Exception {
    // Start a cron job that is already started by an earlier
    // call and is PENDING. Make sure it follows the cron collision policy.
    SanitizedConfiguration sanitizedConfiguration =
        makeCronJob(KEY_A, 1, "1 1 1 1 1", CronCollisionPolicy.KILL_EXISTING);
    expect(cronScheduler.schedule(eq(sanitizedConfiguration.getJobConfig().getCronSchedule()),
        EasyMock.<Runnable>anyObject()))
        .andReturn("key");

    control.replay();
    buildScheduler();

    scheduler.createJob(sanitizedConfiguration);
    assertTaskCount(0);
    assertTrue(cron.hasJob(KEY_A));

    scheduler.startCronJob(KEY_A);
    assertTaskCount(1);

    String taskId = Tasks.id(getOnlyTask(Query.jobScoped(KEY_A)));

    // Now start the same cron job immediately.
    scheduler.startCronJob(KEY_A);
    assertTaskCount(1);
    assertEquals(PENDING, getOnlyTask(Query.jobScoped(KEY_A)).getStatus());

    // Make sure the pending job is the new one.
    String newTaskId = Tasks.id(getOnlyTask(Query.jobScoped(KEY_A)));
    assertFalse(taskId.equals(newTaskId));
  }

  @Test
  public void testStartRunningOverlapCronJob() throws Exception {
    // Start a cron job that is already started by an earlier
    // call and is PENDING. Make sure it follows the cron collision policy.
    SanitizedConfiguration sanitizedConfiguration =
        makeCronJob(KEY_A, 1, "1 1 1 1 1", CronCollisionPolicy.RUN_OVERLAP);
    expect(cronScheduler.schedule(eq(sanitizedConfiguration.getJobConfig().getCronSchedule()),
        EasyMock.<Runnable>anyObject()))
        .andReturn("key");

    control.replay();
    buildScheduler();

    scheduler.createJob(sanitizedConfiguration);
    assertTaskCount(0);
    assertTrue(cron.hasJob(KEY_A));

    scheduler.startCronJob(KEY_A);
    assertTaskCount(1);

    String taskId = Tasks.id(getOnlyTask(Query.jobScoped(KEY_A)));

    // Now start the same cron job immediately.
    scheduler.startCronJob(KEY_A);

    // Since the task never left PENDING, the second run should have been suppressed.
    assertTaskCount(1);
    assertEquals(PENDING, getTask(taskId).getStatus());

    changeStatus(Query.taskScoped(taskId), ASSIGNED);

    scheduler.startCronJob(KEY_A);
    assertTaskCount(2);
    assertEquals(ASSIGNED, getTask(taskId).getStatus());

    getOnlyTask(Query.unscoped().byStatus(ScheduleStatus.PENDING));
  }

  @Test
  public void testKillCreateCronJob() throws Exception {
    SanitizedConfiguration sanitizedConfiguration = makeCronJob(KEY_A, 1, "1 1 1 1 1");
    IJobConfiguration job = sanitizedConfiguration.getJobConfig();
    expect(cronScheduler.schedule(eq(job.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");
    cronScheduler.deschedule("key");

    SanitizedConfiguration updated = makeCronJob(KEY_A, 1, "1 2 3 4 5");
    IJobConfiguration updatedJob = updated.getJobConfig();
    expect(cronScheduler.schedule(eq(updatedJob.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key2");

    control.replay();
    buildScheduler();

    scheduler.createJob(sanitizedConfiguration);
    assertTrue(cron.hasJob(KEY_A));

    scheduler.killTasks(Query.jobScoped(KEY_A), OWNER_A.getUser());
    scheduler.createJob(updated);

    IJobConfiguration stored = Iterables.getOnlyElement(cron.getJobs());
    assertEquals(updatedJob.getCronSchedule(), stored.getCronSchedule());
  }

  @Test
  public void testKillTask() throws Exception {
    driver.killTask(EasyMock.<String>anyObject());
    // We only expect three kills because the first test does not move out of PENDING.
    expectLastCall().times(3);

    control.replay();
    buildScheduler();

    for (Set<ScheduleStatus> statuses : ImmutableSet.of(
        ImmutableSet.<ScheduleStatus>of(),
        EnumSet.of(ASSIGNED),
        EnumSet.of(ASSIGNED, STARTING),
        EnumSet.of(ASSIGNED, STARTING, RUNNING))) {

      scheduler.createJob(makeJob(KEY_A, 1));
      String taskId = Tasks.id(getOnlyTask(
          Query.jobScoped(KEY_A).active()));

      for (ScheduleStatus status : statuses) {
        changeStatus(taskId, status);
      }

      scheduler.killTasks(Query.roleScoped(ROLE_A), OWNER_A.getUser());

      if (!statuses.isEmpty()) {
        // If there was no move out of the PENDING state, the task is deleted outright.
        assertEquals(KILLING, getTask(taskId).getStatus());
      }

      // SImulate a KILLED ack from the executor.
      changeStatus(Query.roleScoped(ROLE_A), KILLED);
      assertTrue(
          getTasks(Query.jobScoped(KEY_A).active()).isEmpty());
    }
  }

  @Test
  public void testServiceTasksRescheduled() throws Exception {
    control.replay();
    buildScheduler();

    // Schedule 5 service and 5 non-service tasks.
    scheduler.createJob(makeJob(KEY_A, 5));
    TaskConfig task = productionTask().setIsService(true);
    scheduler.createJob(
        makeJob(IJobKey.build(KEY_A.newBuilder().setName(KEY_A.getName() + "service")), task, 5));

    assertEquals(10, getTasksByStatus(PENDING).size());
    changeStatus(Query.roleScoped(ROLE_A), ASSIGNED);
    changeStatus(Query.roleScoped(ROLE_A), STARTING);
    assertEquals(10, getTasksByStatus(STARTING).size());

    changeStatus(Query.roleScoped(ROLE_A), RUNNING);
    assertEquals(10, getTasksByStatus(RUNNING).size());

    // Service tasks will move back into PENDING state after finishing.
    changeStatus(Query.roleScoped(ROLE_A), FINISHED);
    Set<IScheduledTask> newTasks = getTasksByStatus(PENDING);
    assertEquals(5, newTasks.size());
    for (IScheduledTask state : newTasks) {
      assertEquals(
          getTask(state.getAncestorId()).getAssignedTask().getInstanceId(),
          state.getAssignedTask().getInstanceId());
    }

    assertEquals(10, getTasksByStatus(FINISHED).size());
  }

  @Test
  public void testServiceTaskIgnoresMaxFailures() throws Exception {
    control.replay();
    buildScheduler();

    int maxFailures = 5;
    int totalFailures = 10;

    // Schedule a service task.
    TaskConfig task = productionTask()
        .setIsService(true)
        .setMaxTaskFailures(maxFailures);
    scheduler.createJob(makeJob(KEY_A, task, 1));
    assertTaskCount(1);

    // Fail the task more than maxFailures.
    for (int i = 1; i <= totalFailures; i++) {
      String taskId = Tasks.id(
          getOnlyTask(Query.jobScoped(KEY_A).active()));

      changeStatus(taskId, ASSIGNED);
      changeStatus(taskId, STARTING);
      changeStatus(taskId, RUNNING);
      assertEquals(i - 1, getTask(taskId).getFailureCount());
      changeStatus(taskId, FAILED);

      assertTaskCount(i + 1);
      IScheduledTask rescheduled = getOnlyTask(Query.unscoped().byStatus(PENDING));
      assertEquals(i, rescheduled.getFailureCount());
    }

    assertEquals(totalFailures, getTasksByStatus(FAILED).size());
    assertEquals(1, getTasksByStatus(PENDING).size());
  }

  @Test
  public void testTaskRescheduleOnKill() throws Exception {
    control.replay();
    buildScheduler();

    // Create 5 non-service and 5 service tasks.
    scheduler.createJob(makeJob(KEY_A, 5));
    TaskConfig task = productionTask().setIsService(true);
    scheduler.createJob(
        makeJob(IJobKey.build(KEY_A.newBuilder().setName(KEY_A.getName() + "service")), task, 5));

    assertEquals(10, getTasksByStatus(PENDING).size());
    changeStatus(Query.roleScoped(ROLE_A), ASSIGNED);
    changeStatus(Query.roleScoped(ROLE_A), STARTING);
    assertEquals(10, getTasksByStatus(STARTING).size());
    changeStatus(Query.roleScoped(ROLE_A), RUNNING);
    assertEquals(10, getTasksByStatus(RUNNING).size());

    // All tasks will move back into PENDING state after getting KILLED.
    changeStatus(Query.roleScoped(ROLE_A), KILLED);
    Set<IScheduledTask> newTasks = getTasksByStatus(PENDING);
    assertEquals(10, newTasks.size());
    assertEquals(10, getTasksByStatus(KILLED).size());
  }

  @Test
  public void testNoTransitionFromTerminalState() throws Exception {
    expectKillTask(1);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(KEY_A, 1));
    changeStatus(Query.roleScoped(ROLE_A), ASSIGNED);
    changeStatus(Query.roleScoped(ROLE_A), STARTING);
    changeStatus(Query.roleScoped(ROLE_A), RUNNING);
    scheduler.killTasks(Query.roleScoped(ROLE_A), OWNER_A.getUser());
    changeStatus(Query.roleScoped(ROLE_A), KILLED);

    String taskId = Tasks.id(getOnlyTask(Query.roleScoped(ROLE_A)));

    // This transition should be rejected.
    changeStatus(Query.roleScoped(ROLE_A), LOST);
    assertEquals(KILLED, getTask(taskId).getStatus());
  }

  @Test
  public void testFailedTaskIncrementsFailureCount() throws Exception {
    int maxFailures = 5;
    control.replay();
    buildScheduler();

    TaskConfig task = productionTask().setMaxTaskFailures(maxFailures);
    scheduler.createJob(makeJob(KEY_A, task, 1));
    assertTaskCount(1);

    assertEquals(1, getTasks(Query.jobScoped(KEY_A)).size());

    for (int i = 1; i <= maxFailures; i++) {
      String taskId = Tasks.id(getOnlyTask(
          Query.jobScoped(KEY_A).active()));

      changeStatus(taskId, ASSIGNED);
      changeStatus(taskId, STARTING);
      changeStatus(taskId, RUNNING);
      assertEquals(i - 1, getTask(taskId).getFailureCount());
      changeStatus(taskId, FAILED);

      if (i != maxFailures) {
        assertTaskCount(i + 1);
        IScheduledTask rescheduled = getOnlyTask(Query.unscoped().byStatus(PENDING));
        assertEquals(i, rescheduled.getFailureCount());
      } else {
        assertTaskCount(maxFailures);
      }
    }

    assertEquals(maxFailures, getTasksByStatus(FAILED).size());
    assertTrue(getTasksByStatus(PENDING).isEmpty());
  }

  @Test
  public void testCronJobLifeCycle() throws Exception {
    SanitizedConfiguration sanitizedConfiguration = makeCronJob(KEY_A, 10, "1 1 1 1 1");
    IJobConfiguration job = sanitizedConfiguration.getJobConfig();
    expect(cronScheduler.schedule(eq(job.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");

    control.replay();
    buildScheduler();

    scheduler.createJob(sanitizedConfiguration);
    assertTaskCount(0);
    assertTrue(cron.hasJob(KEY_A));

    // Simulate a triggering of the cron job.
    scheduler.startCronJob(KEY_A);
    assertTaskCount(10);
    assertEquals(10,
        getTasks(Query.jobScoped(KEY_A).byStatus(PENDING)).size());

    assertTaskCount(10);

    changeStatus(Query.roleScoped(ROLE_A), ASSIGNED);
    changeStatus(Query.roleScoped(ROLE_A), STARTING);
    assertTaskCount(10);
    changeStatus(Query.roleScoped(ROLE_A), RUNNING);
    assertTaskCount(10);
    changeStatus(Query.roleScoped(ROLE_A), FINISHED);
  }

  @Test
  public void testCronNoSuicide() throws Exception {
    SanitizedConfiguration sanitizedConfiguration =
        makeCronJob(KEY_A, 10, "1 1 1 1 1", CronCollisionPolicy.KILL_EXISTING);
    expect(cronScheduler.schedule(eq(sanitizedConfiguration.getJobConfig().getCronSchedule()),
        EasyMock.<Runnable>anyObject()))
        .andReturn("key");

    control.replay();
    buildScheduler();

    scheduler.createJob(sanitizedConfiguration);
    assertTaskCount(0);

    try {
      scheduler.createJob(sanitizedConfiguration);
      fail();
    } catch (ScheduleException e) {
      // Expected.
    }
    assertTrue(cron.hasJob(KEY_A));

    // Simulate a triggering of the cron job.
    scheduler.startCronJob(KEY_A);
    assertTaskCount(10);

    Set<String> taskIds = Tasks.ids(getTasksOwnedBy(OWNER_A));

    // Simulate a triggering of the cron job.
    scheduler.startCronJob(KEY_A);
    assertTaskCount(10);
    assertTrue(Sets.intersection(taskIds, Tasks.ids(getTasksOwnedBy(OWNER_A))).isEmpty());

    try {
      scheduler.createJob(sanitizedConfiguration);
      fail();
    } catch (ScheduleException e) {
      // Expected.
    }
    assertTrue(cron.hasJob(KEY_A));
  }

  @Test
  public void testKillPendingTask() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(KEY_A, 1));
    assertTaskCount(1);

    Set<IScheduledTask> tasks = Storage.Util.consistentFetchTasks(storage, Query.jobScoped(KEY_A));
    assertEquals(1, tasks.size());

    String taskId = Tasks.id(Iterables.get(tasks, 0));

    scheduler.killTasks(Query.taskScoped(taskId), OWNER_A.getUser());
    assertTaskCount(0);
  }

  @Test
  public void testKillRunningTask() throws Exception {
    expectKillTask(1);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(KEY_A, 1));
    String taskId = Tasks.id(getOnlyTask(Query.roleScoped(ROLE_A)));
    changeStatus(taskId, ASSIGNED);
    changeStatus(taskId, STARTING);
    changeStatus(taskId, RUNNING);
    scheduler.killTasks(Query.taskScoped(taskId), OWNER_A.getUser());
    assertEquals(KILLING, getTask(taskId).getStatus());
    assertEquals(1, getTasks(Query.roleScoped(ROLE_A)).size());
    changeStatus(taskId, KILLED);
    assertEquals(KILLED, getTask(taskId).getStatus());
  }

  @Test
  public void testKillCronTask() throws Exception {
    SanitizedConfiguration sanitizedConfiguration =
        makeCronJob(KEY_A, 1, "1 1 1 1 1", CronCollisionPolicy.KILL_EXISTING);
    expect(cronScheduler.schedule(eq(sanitizedConfiguration.getJobConfig().getCronSchedule()),
        EasyMock.<Runnable>anyObject()))
        .andReturn("key");
    cronScheduler.deschedule("key");

    control.replay();
    buildScheduler();
    scheduler.createJob(makeCronJob(KEY_A, 1, "1 1 1 1 1"));

    // This will fail if the cron task could not be found.
    scheduler.killTasks(Query.jobScoped(KEY_A), OWNER_A.getUser());
  }

  @Test
  public void testLostTaskRescheduled() throws Exception {
    expectKillTask(2);

    control.replay();
    buildScheduler();

    int maxFailures = 5;
    TaskConfig task = productionTask().setMaxTaskFailures(maxFailures);
    scheduler.createJob(makeJob(KEY_A, task, 1));
    assertTaskCount(1);

    Set<IScheduledTask> tasks = Storage.Util.consistentFetchTasks(storage, Query.jobScoped(KEY_A));
    assertEquals(1, tasks.size());

    changeStatus(Query.unscoped().byStatus(PENDING), ASSIGNED);

    Query.Builder pendingQuery = Query.unscoped().byStatus(PENDING);
    changeStatus(Query.unscoped().byStatus(ASSIGNED), LOST);
    assertEquals(PENDING, getOnlyTask(pendingQuery).getStatus());
    assertTaskCount(2);

    changeStatus(Query.unscoped().byStatus(PENDING), ASSIGNED);
    changeStatus(Query.unscoped().byStatus(ASSIGNED), LOST);
    assertEquals(PENDING, getOnlyTask(pendingQuery).getStatus());
    assertTaskCount(3);
  }

  @Test
  public void testIsStrictlyJobScoped() throws Exception {
    // TODO(Sathya): Remove this after adding a unit test for Query utility class.
    control.replay();
    assertTrue(Query.isOnlyJobScoped(Query.jobScoped(KEY_A)));
    assertFalse(Query.isOnlyJobScoped(Query.jobScoped(KEY_A).byId("xyz")));
  }

  @Test
  public void testKillNotStrictlyJobScoped() throws Exception {
    // Makes sure that queries that are not strictly job scoped will not remove the job entirely.
    SanitizedConfiguration config = makeCronJob(KEY_A, 10, "1 1 1 1 1");
    IJobConfiguration job = config.getJobConfig();
    expect(cronScheduler.schedule(eq(job.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");
    cronScheduler.deschedule("key");

    control.replay();
    buildScheduler();

    scheduler.createJob(config);
    assertTrue(cron.hasJob(KEY_A));
    scheduler.startCronJob(KEY_A);
    assertTaskCount(10);

    scheduler.killTasks(Query.instanceScoped(KEY_A, 0), USER_A);
    assertTaskCount(9);
    assertTrue(cron.hasJob(KEY_A));

    scheduler.killTasks(Query.jobScoped(KEY_A), USER_A);
    assertFalse(cron.hasJob(KEY_A));
  }

  @Test
  public void testKillJob() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(KEY_A, 10));
    assertTaskCount(10);

    scheduler.killTasks(Query.jobScoped(KEY_A), OWNER_A.getUser());
    assertTaskCount(0);
  }

  @Test
  public void testKillJob2() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(KEY_A, 5));
    assertTaskCount(5);

    scheduler.createJob(
        makeJob(IJobKey.build(KEY_A.newBuilder().setName(KEY_A.getName() + "2")), 5));
    assertTaskCount(10);

    scheduler.killTasks(queryJob(OWNER_A, JOB_A + "2"), OWNER_A.getUser());
    assertTaskCount(5);

    for (IScheduledTask state : Storage.Util.consistentFetchTasks(storage, Query.unscoped())) {
      assertEquals(JOB_A, Tasks.getJob(state));
    }
  }

  @Test
  public void testSlaveDeletesTasks() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(KEY_A, 2));

    Query.Builder builder = Query.unscoped().active();
    String taskId1 = Tasks.id(getOnlyTask(builder.byInstances(KEY_A, 0)));
    String taskId2 = Tasks.id(getOnlyTask(builder.byInstances(KEY_A, 1)));

    assignTask(taskId1, SLAVE_ID, SLAVE_HOST_1);
    assignTask(taskId2, SLAVE_ID, SLAVE_HOST_1);

    changeStatus(taskId1, STARTING);
    changeStatus(taskId1, RUNNING);
    changeStatus(taskId2, STARTING);
    changeStatus(taskId2, FINISHED);

    scheduler.tasksDeleted(ImmutableSet.of(taskId1, taskId2));

    // The expected outcome is that one task is moved into the LOST state and rescheduled.
    // The FINISHED task's state is deleted.
    assertTaskCount(2);
    assertEquals(LOST, getOnlyTask(Query.taskScoped(taskId1)).getStatus());
    assertTrue(getTasks(Query.taskScoped(taskId2)).isEmpty());

    IScheduledTask rescheduled = Iterables.getOnlyElement(getTasksByStatus(PENDING));
    assertEquals(taskId1, rescheduled.getAncestorId());
  }

  @Test
  public void testRestartShards() throws Exception {
    expectKillTask(2);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(KEY_A, productionTask().setIsService(true), 6));
    changeStatus(Query.jobScoped(KEY_A), ASSIGNED);
    changeStatus(Query.jobScoped(KEY_A), RUNNING);
    scheduler.restartShards(KEY_A, ImmutableSet.of(1, 5), OWNER_A.user);
    assertEquals(4, getTasks(Query.unscoped().byStatus(RUNNING)).size());
    assertEquals(2, getTasks(Query.unscoped().byStatus(RESTARTING)).size());
    changeStatus(Query.unscoped().byStatus(RESTARTING), FINISHED);
    assertEquals(2, getTasks(Query.unscoped().byStatus(PENDING)).size());
  }

  @Test(expected = ScheduleException.class)
  public void testRestartNonexistentShard() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(KEY_A, productionTask().setIsService(true), 1));
    changeStatus(Query.jobScoped(KEY_A), ASSIGNED);
    changeStatus(Query.jobScoped(KEY_A), FINISHED);
    scheduler.restartShards(KEY_A, ImmutableSet.of(5), OWNER_A.user);
  }

  @Test
  public void testRestartPendingShard() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(KEY_A, productionTask().setIsService(true), 1));
    scheduler.restartShards(KEY_A, ImmutableSet.of(0), OWNER_A.user);
  }

  @Test
  public void testPortResource() throws Exception {
    control.replay();
    buildScheduler();

    TaskConfig config = productionTask()
        .setRequestedPorts(ImmutableSet.of("one", "two", "three"));

    scheduler.createJob(makeJob(KEY_A, config, 1));

    String taskId = Tasks.id(getOnlyTask(
        Query.instanceScoped(KEY_A, 0).active()));

    assignTask(taskId, SLAVE_ID, SLAVE_HOST_1, ImmutableSet.of(80, 81, 82));

    IAssignedTask task = getTask(taskId).getAssignedTask();
    assertEquals(
        ImmutableSet.of("one", "two", "three"),
        task.getTask().getRequestedPorts());
  }

  @Test
  public void testPortResourceResetAfterReschedule() throws Exception {
    expectKillTask(1);

    control.replay();
    buildScheduler();

    TaskConfig config = productionTask().setRequestedPorts(ImmutableSet.of("one"));

    scheduler.createJob(makeJob(KEY_A, config, 1));

    String taskId = Tasks.id(getOnlyTask(
        Query.instanceScoped(KEY_A, 0).active()));

    assignTask(taskId, SLAVE_ID, SLAVE_HOST_1, ImmutableSet.of(80));

    // The task should be rescheduled.
    changeStatus(taskId, LOST);

    String newTaskId = Tasks.id(getOnlyTask(
        Query.instanceScoped(KEY_A, 0).active()));
    assignTask(newTaskId, SLAVE_ID, SLAVE_HOST_1, ImmutableSet.of(86));

    IAssignedTask task = getTask(newTaskId).getAssignedTask();
    assertEquals(ImmutableMap.of("one", 86), task.getAssignedPorts());
  }

  @Test
  public void testAuditMessage() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(KEY_A, 1));

    String taskId = Tasks.id(getOnlyTask(Query.roleScoped(ROLE_A)));
    changeStatus(taskId, ASSIGNED);
    changeStatus(taskId, STARTING);
    changeStatus(taskId, FAILED, Optional.of("bad stuff happened"));

    String hostname = getLocalHost();

    Iterator<Pair<ScheduleStatus, String>> expectedEvents =
        ImmutableList.<Pair<ScheduleStatus, String>>builder()
            .add(Pair.<ScheduleStatus, String>of(PENDING, null))
            .add(Pair.<ScheduleStatus, String>of(ASSIGNED, null))
            .add(Pair.<ScheduleStatus, String>of(STARTING, null))
            .add(Pair.<ScheduleStatus, String>of(FAILED, "bad stuff happened"))
            .build()
        .iterator();
    for (ITaskEvent event : getTask(taskId).getTaskEvents()) {
      Pair<ScheduleStatus, String> expected = expectedEvents.next();
      assertEquals(expected.getFirst(), event.getStatus());
      assertEquals(expected.getSecond(), event.getMessage());
      assertEquals(hostname, event.getScheduler());
    }
  }

  @Test
  public void testEnsureCanAddInstances() throws Exception {
    SanitizedConfiguration job = makeJob(KEY_A, 1);
    expect(jobFilter.filter(job.getJobConfig().getTaskConfig(), 1))
        .andReturn(JobFilterResult.pass());

    control.replay();
    buildScheduler();

    scheduler.validateJobResources(job);
  }

  @Test(expected = ScheduleException.class)
  public void testEnsureCanAddInstancesFails() throws Exception {
    SanitizedConfiguration job = makeJob(KEY_A, 1);
    expect(jobFilter.filter(job.getJobConfig().getTaskConfig(), 1))
        .andReturn(JobFilterResult.fail("fail"));

    control.replay();
    buildScheduler();

    scheduler.validateJobResources(job);
  }

  @Test
  public void testTaskIdLimit() throws Exception {
    taskIdGenerator = new TaskIdGenerator() {
      @Override public String generate(ITaskConfig input, int instanceCount) {
        return Strings.repeat("a", SchedulerCoreImpl.MAX_TASK_ID_LENGTH);
      }
    };

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(KEY_A, 1));
  }

  @Test(expected = ScheduleException.class)
  public void testRejectLongTaskId() throws Exception {
    taskIdGenerator = new TaskIdGenerator() {
      @Override public String generate(ITaskConfig input, int instanceCount) {
        return Strings.repeat("a", SchedulerCoreImpl.MAX_TASK_ID_LENGTH + 1);
      }
    };

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(KEY_A, 1));
  }

  @Test(expected = ScheduleException.class)
  public void testFilterFailRejectsCreate() throws Exception {
    SanitizedConfiguration job = makeJob(KEY_A, 1);
    expect(jobFilter.filter(job.getJobConfig().getTaskConfig(), 1))
        .andReturn(JobFilterResult.fail("failed"));

    control.replay();

    buildScheduler();
    scheduler.createJob(job);
  }

  @Test(expected = ScheduleException.class)
  public void testFilterFailRejectsAddInstances() throws Exception {
    IJobConfiguration job = makeJob(KEY_A, 1).getJobConfig();
    expect(jobFilter.filter(job.getTaskConfig(), 1)).andReturn(JobFilterResult.fail("failed"));

    control.replay();

    buildScheduler();
    scheduler.addInstances(job.getKey(), ImmutableSet.of(1), job.getTaskConfig());
  }

  @Test(expected = ScheduleException.class)
  public void testMaxJobCheckFailsForAddInstances() throws Exception {
    IJobConfiguration job = makeJob(KEY_A, 1).getJobConfig();

    control.replay();
    buildScheduler();

    scheduler.addInstances(
        job.getKey(),
        ImmutableSet.copyOf(
            ContiguousSet.create(Range.closed(0, SchedulerCoreImpl.MAX_TASKS_PER_JOB.get()),
                DiscreteDomain.integers())),
        job.getTaskConfig());
  }

  @Test
  public void testAddInstances() throws Exception {
    TaskConfig existingTask = productionTask();
    TaskConfig newTask = productionTask()
        .setEnvironment(ENV_A)
        .setJobName(KEY_A.getName())
        .setOwner(OWNER_A);
    expect(jobFilter.filter(ITaskConfig.build(newTask), 2)).andReturn(JobFilterResult.pass());
    ImmutableSet<Integer> instances = ImmutableSet.of(1);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(KEY_A, existingTask, 1));

    assertTaskCount(1);
    scheduler.addInstances(KEY_A, instances, ITaskConfig.build(newTask));
    assertTaskCount(2);
  }

  @Test
  public void testAddInstancesNoExistingTasks() throws Exception {
    TaskConfig newTask = productionTask()
        .setEnvironment(ENV_A)
        .setJobName(KEY_A.getName())
        .setOwner(OWNER_A);

    ImmutableSet<Integer> instances = ImmutableSet.of(1);

    control.replay();
    buildScheduler();

    assertTaskCount(0);
    scheduler.addInstances(KEY_A, instances, ITaskConfig.build(newTask));
    assertTaskCount(1);
  }

  @Test(expected = ScheduleException.class)
  public void testAddInstancesIdCollision() throws Exception {
    TaskConfig existingTask = productionTask();
    TaskConfig newTask = productionTask()
        .setEnvironment(ENV_A)
        .setJobName(KEY_A.getName())
        .setOwner(OWNER_A);

    ImmutableSet<Integer> instances = ImmutableSet.of(0);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(KEY_A, existingTask, 1));

    assertTaskCount(1);
    scheduler.addInstances(KEY_A, instances, ITaskConfig.build(newTask));
  }

  private static String getLocalHost() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw Throwables.propagate(e);
    }
  }

  // TODO(William Farner): Inject a task ID generation function into StateManager so that we can
  //     expect specific task IDs to be killed here.
  private void expectKillTask(int numTasks) {
    driver.killTask(EasyMock.<String>anyObject());
    expectLastCall().times(numTasks);
  }

  private void assertTaskCount(int numTasks) {
    assertEquals(numTasks, Storage.Util.consistentFetchTasks(storage, Query.unscoped()).size());
  }

  private static Identity makeIdentity(String role) {
    return new Identity().setRole(role).setUser(USER_A);
  }

  private static Identity makeIdentity(JobKey jobKey) {
    return makeIdentity(jobKey.getRole());
  }

  private static SanitizedConfiguration makeCronJob(
      IJobKey jobKey,
      int numDefaultTasks,
      String cronSchedule,
      CronCollisionPolicy policy) throws TaskDescriptionException {

    return new SanitizedConfiguration(IJobConfiguration.build(
        makeCronJob(jobKey, numDefaultTasks, cronSchedule)
            .getJobConfig()
            .newBuilder()
            .setCronCollisionPolicy(policy)));
  }

  private static SanitizedConfiguration makeCronJob(
      IJobKey jobKey,
      int numDefaultTasks,
      String cronSchedule) throws TaskDescriptionException {

    SanitizedConfiguration job = makeJob(jobKey, numDefaultTasks);
    return new SanitizedConfiguration(
        IJobConfiguration.build(job.getJobConfig().newBuilder().setCronSchedule(cronSchedule)));
  }

  private static SanitizedConfiguration makeJob(IJobKey jobKey, int numDefaultTasks)
      throws TaskDescriptionException  {

    return makeJob(jobKey, productionTask(), numDefaultTasks);
  }

  private static SanitizedConfiguration makeJob(IJobKey jobKey, TaskConfig task)
      throws TaskDescriptionException {

    return makeJob(jobKey, task, 1);
  }

  private static SanitizedConfiguration makeJob(
      IJobKey jobKey,
      TaskConfig task,
      int numTasks) throws TaskDescriptionException  {

    JobConfiguration job = new JobConfiguration()
        .setOwner(makeIdentity(jobKey.newBuilder()))
        .setKey(jobKey.newBuilder())
        .setInstanceCount(numTasks)
        .setTaskConfig(new TaskConfig(task)
          .setOwner(makeIdentity(jobKey.newBuilder()))
          .setEnvironment(jobKey.getEnvironment())
          .setJobName(jobKey.getName()));
    return SanitizedConfiguration.fromUnsanitized(IJobConfiguration.build(job));
  }

  private static TaskConfig defaultTask(boolean production) {
    return new TaskConfig()
        .setNumCpus(1)
        .setRamMb(1024)
        .setDiskMb(1024)
        .setProduction(production)
        .setExecutorConfig(new ExecutorConfig("aurora", "thermos"))
        // Avoid per-host scheduling constraints.
        .setConstraints(Sets.newHashSet(hostLimitConstraint(100)))
        .setContactEmail("testing@twitter.com");
  }

  private static TaskConfig productionTask() {
    return defaultTask(true);
  }

  private static TaskConfig nonProductionTask() {
    return defaultTask(false);
  }

  private IScheduledTask getTask(String taskId) {
    return getOnlyTask(Query.taskScoped(taskId));
  }

  private IScheduledTask getOnlyTask(Query.Builder query) {
    return Iterables.getOnlyElement(Storage.Util.consistentFetchTasks(storage, query));
  }

  private Set<IScheduledTask> getTasks(Query.Builder query) {
    return Storage.Util.consistentFetchTasks(storage, query);
  }

  private Set<IScheduledTask> getTasksByStatus(ScheduleStatus status) {
    return Storage.Util.consistentFetchTasks(storage, Query.unscoped().byStatus(status));
  }

  private Set<IScheduledTask> getTasksOwnedBy(Identity owner) {
    return Storage.Util.consistentFetchTasks(storage, query(owner, null, null));
  }

  private Query.Builder queryJob(Identity owner, String jobName) {
    return query(owner, jobName, null);
  }

  private Query.Builder query(
      @Nullable Identity owner,
      @Nullable String jobName,
      @Nullable Iterable<String> taskIds) {

    TaskQuery query = new TaskQuery();
    if (owner != null) {
      query.setOwner(owner);
    }
    if (jobName != null) {
      query.setJobName(jobName);
    }
    if (taskIds != null) {
      query.setTaskIds(Sets.newHashSet(taskIds));
    }

    return Query.arbitrary(query);
  }

  public void changeStatus(
      Query.Builder query,
      ScheduleStatus status,
      Optional<String> message) {

    scheduler.setTaskStatus(query, status, message);
  }

  public void changeStatus(Query.Builder query, ScheduleStatus status) {
    changeStatus(query, status, Optional.<String>absent());
  }

  public void changeStatus(String taskId, ScheduleStatus status) {
    changeStatus(taskId, status, Optional.<String>absent());
  }

  public void changeStatus(String taskId, ScheduleStatus status, Optional<String> message) {
    changeStatus(Query.taskScoped(taskId), status, message);
  }
}

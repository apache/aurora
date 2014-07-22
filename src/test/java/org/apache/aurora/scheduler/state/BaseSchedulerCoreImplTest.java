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
package org.apache.aurora.scheduler.state;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.twitter.common.collections.Pair;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.ValueConstraint;
import org.apache.aurora.scheduler.Driver;
import org.apache.aurora.scheduler.TaskIdGenerator;
import org.apache.aurora.scheduler.async.RescheduleCalculator;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.ScheduleException;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import org.apache.aurora.scheduler.configuration.SanitizedConfiguration;
import org.apache.aurora.scheduler.cron.CronException;
import org.apache.aurora.scheduler.cron.CronJobManager;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.quota.QuotaCheckResult;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.StorageBackfill;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.entities.ITaskEvent;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.ASSIGNED;
import static org.apache.aurora.gen.ScheduleStatus.FAILED;
import static org.apache.aurora.gen.ScheduleStatus.FINISHED;
import static org.apache.aurora.gen.ScheduleStatus.KILLED;
import static org.apache.aurora.gen.ScheduleStatus.LOST;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.RESTARTING;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.gen.ScheduleStatus.STARTING;
import static org.apache.aurora.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;
import static org.apache.aurora.scheduler.configuration.ConfigurationManager.hostLimitConstraint;
import static org.apache.aurora.scheduler.configuration.ConfigurationManager.validateAndPopulate;
import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.INSUFFICIENT_QUOTA;
import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.SUFFICIENT_QUOTA;
import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyObject;
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

  private static final QuotaCheckResult ENOUGH_QUOTA = new QuotaCheckResult(SUFFICIENT_QUOTA);
  private static final QuotaCheckResult NOT_ENOUGH_QUOTA = new QuotaCheckResult(INSUFFICIENT_QUOTA);

  private Driver driver;
  private StateManagerImpl stateManager;
  private Storage storage;
  private SchedulerCoreImpl scheduler;
  private CronJobManager cronJobManager;
  private FakeClock clock;
  private EventSink eventSink;
  private RescheduleCalculator rescheduleCalculator;
  private QuotaManager quotaManager;

  // TODO(William Farner): Set up explicit expectations for calls to generate task IDs.
  private final AtomicLong idCounter = new AtomicLong();
  private TaskIdGenerator taskIdGenerator = new TaskIdGenerator() {
    @Override
    public String generate(ITaskConfig input, int instanceId) {
      return "task-" + idCounter.incrementAndGet();
    }
  };

  @Before
  public void setUp() throws Exception {
    driver = createMock(Driver.class);
    clock = new FakeClock();
    eventSink = createMock(EventSink.class);
    rescheduleCalculator = createMock(RescheduleCalculator.class);
    cronJobManager = createMock(CronJobManager.class);
    quotaManager = createMock(QuotaManager.class);

    eventSink.post(EasyMock.<PubsubEvent>anyObject());
    expectLastCall().anyTimes();

    expect(quotaManager.checkQuota(anyObject(ITaskConfig.class), anyInt()))
        .andStubReturn(ENOUGH_QUOTA);
    expect(cronJobManager.hasJob(anyObject(IJobKey.class))).andStubReturn(false);
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
      @Override
      protected void execute(MutableStoreProvider storeProvider) {
        StorageBackfill.backfill(storeProvider, clock);
      }
    });

    stateManager = new StateManagerImpl(
        storage,
        clock,
        driver,
        taskIdGenerator,
        eventSink,
        rescheduleCalculator);
    scheduler = new SchedulerCoreImpl(
        storage,
        cronJobManager,
        stateManager,
        taskIdGenerator,
        quotaManager);
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

  @Test
  public void testCreateJobEmptyString() throws Exception {
    // TODO(ksweeney): Deprecate this as part of AURORA-423.

    control.replay();
    buildScheduler();

    SanitizedConfiguration job = SanitizedConfiguration.fromUnsanitized(
        IJobConfiguration.build(makeJob(KEY_A, 1).getJobConfig().newBuilder().setCronSchedule("")));
    scheduler.createJob(job);
    assertTaskCount(1);
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
    newTask.addToConstraints(dedicatedConstraint(ImmutableSet.of(JobKeys.canonicalString(KEY_A))));
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

  @Test(expected = ScheduleException.class)
  public void testCreateDuplicateJob() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(KEY_A, 1));
    assertTaskCount(1);

    scheduler.createJob(makeJob(KEY_A, 1));
  }

  private IExpectationSetters<Long> expectTaskNotThrottled() {
    return expect(rescheduleCalculator.getFlappingPenaltyMs(EasyMock.<IScheduledTask>anyObject()))
        .andReturn(0L);
  }

  @Test
  public void testServiceTasksRescheduled() throws Exception {
    int numServiceTasks = 5;
    IJobKey adhocKey = KEY_A;
    IJobKey serviceKey = IJobKey.build(
        adhocKey.newBuilder().setName(adhocKey.getName() + "service"));

    expectTaskNotThrottled().times(numServiceTasks);
    expectNoCronJob(adhocKey);
    expectNoCronJob(serviceKey);

    control.replay();
    buildScheduler();

    // Schedule 5 service and 5 non-service tasks.
    scheduler.createJob(makeJob(adhocKey, numServiceTasks));
    TaskConfig task = productionTask().setIsService(true);
    scheduler.createJob(makeJob(serviceKey, task, 5));

    assertEquals(10, getTasksByStatus(PENDING).size());
    changeStatus(Query.roleScoped(ROLE_A), ASSIGNED, STARTING);
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
    int totalFailures = 10;

    expectTaskNotThrottled().times(totalFailures);
    expectNoCronJob(KEY_A);

    control.replay();
    buildScheduler();

    int maxFailures = 5;

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

      changeStatus(taskId, ASSIGNED, STARTING, RUNNING);
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
    int numServiceTasks = 5;

    expectTaskNotThrottled().times(numServiceTasks);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(KEY_A, numServiceTasks));

    assertEquals(5, getTasksByStatus(PENDING).size());
    changeStatus(Query.roleScoped(ROLE_A), ASSIGNED, STARTING);
    assertEquals(5, getTasksByStatus(STARTING).size());
    changeStatus(Query.roleScoped(ROLE_A), RUNNING);
    assertEquals(5, getTasksByStatus(RUNNING).size());

    // All tasks will move back into PENDING state after getting KILLED.
    changeStatus(Query.roleScoped(ROLE_A), KILLED);
    Set<IScheduledTask> newTasks = getTasksByStatus(PENDING);
    assertEquals(5, newTasks.size());
    assertEquals(5, getTasksByStatus(KILLED).size());
  }

  @Test
  public void testFailedTaskIncrementsFailureCount() throws Exception {
    int maxFailures = 5;
    expectTaskNotThrottled().times(maxFailures - 1);
    expect(cronJobManager.hasJob(KEY_A)).andReturn(false);

    control.replay();
    buildScheduler();

    TaskConfig task = productionTask().setMaxTaskFailures(maxFailures);
    scheduler.createJob(makeJob(KEY_A, task, 1));
    assertTaskCount(1);

    assertEquals(1, getTasks(Query.jobScoped(KEY_A)).size());

    for (int i = 1; i <= maxFailures; i++) {
      String taskId = Tasks.id(getOnlyTask(
          Query.jobScoped(KEY_A).active()));

      changeStatus(taskId, ASSIGNED, STARTING, RUNNING);
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
  public void testLostTaskRescheduled() throws Exception {
    expectKillTask(2);
    expectTaskNotThrottled().times(2);
    expect(cronJobManager.hasJob(KEY_A)).andReturn(false);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(KEY_A, 1));
    assertTaskCount(1);

    Set<IScheduledTask> tasks = Storage.Util.consistentFetchTasks(storage, Query.jobScoped(KEY_A));
    String taskId = Tasks.id(getOnlyTask(Query.roleScoped(ROLE_A)));
    assertEquals(1, tasks.size());

    changeStatus(taskId, ASSIGNED, LOST);

    String newTaskId = Tasks.id(getOnlyTask(Query.unscoped().byStatus(PENDING)));
    assertFalse(newTaskId.equals(taskId));

    changeStatus(newTaskId, ASSIGNED, LOST);
    assertFalse(newTaskId.equals(Tasks.id(getOnlyTask(Query.unscoped().byStatus(PENDING)))));
  }

  @Test
  public void testIsStrictlyJobScoped() throws Exception {
    // TODO(Sathya): Remove this after adding a unit test for Query utility class.
    control.replay();
    assertTrue(Query.isSingleJobScoped(Query.jobScoped(KEY_A)));
    assertFalse(Query.isSingleJobScoped(Query.jobScoped(KEY_A).byId("xyz")));
  }

  @Test
  public void testRestartShards() throws Exception {
    expectKillTask(2);
    expectTaskNotThrottled().times(2);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(KEY_A, productionTask().setIsService(true), 6));
    changeStatus(Query.jobScoped(KEY_A), ASSIGNED, RUNNING);
    scheduler.restartShards(KEY_A, ImmutableSet.of(1, 5), OWNER_A.user);
    assertEquals(4, getTasks(Query.unscoped().byStatus(RUNNING)).size());
    assertEquals(2, getTasks(Query.unscoped().byStatus(RESTARTING)).size());
    changeStatus(Query.unscoped().byStatus(RESTARTING), FINISHED);
    assertEquals(2, getTasks(Query.unscoped().byStatus(PENDING)).size());
  }

  @Test(expected = ScheduleException.class)
  public void testRestartNonexistentShard() throws Exception {
    expectTaskNotThrottled();
    expectNoCronJob(KEY_A);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(KEY_A, productionTask().setIsService(true), 1));
    changeStatus(Query.jobScoped(KEY_A), ASSIGNED, FINISHED);
    scheduler.restartShards(KEY_A, ImmutableSet.of(5), OWNER_A.user);
  }

  @Test
  public void testRestartPendingShard() throws Exception {
    expect(cronJobManager.hasJob(KEY_A)).andReturn(false);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(KEY_A, productionTask().setIsService(true), 1));
    scheduler.restartShards(KEY_A, ImmutableSet.of(0), OWNER_A.user);
  }

  @Test
  public void testAuditMessage() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(KEY_A, 1));

    String taskId = Tasks.id(getOnlyTask(Query.roleScoped(ROLE_A)));
    changeStatus(taskId, ASSIGNED, STARTING);
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
  public void testTaskIdLimit() throws Exception {
    taskIdGenerator = new TaskIdGenerator() {
      @Override
      public String generate(ITaskConfig input, int instanceCount) {
        return Strings.repeat("a", SchedulerCoreImpl.MAX_TASK_ID_LENGTH);
      }
    };

    expectNoCronJob(KEY_A);
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(KEY_A, 1));
  }

  @Test(expected = ScheduleException.class)
  public void testRejectLongTaskId() throws Exception {
    taskIdGenerator = new TaskIdGenerator() {
      @Override
      public String generate(ITaskConfig input, int instanceCount) {
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
    expect(quotaManager.checkQuota(anyObject(ITaskConfig.class), anyInt()))
        .andReturn(NOT_ENOUGH_QUOTA);

    control.replay();

    buildScheduler();
    scheduler.createJob(job);
  }

  @Test(expected = ScheduleException.class)
  public void testFilterFailRejectsAddInstances() throws Exception {
    IJobConfiguration job = makeJob(KEY_A, 1).getJobConfig();
    expect(quotaManager.checkQuota(anyObject(ITaskConfig.class), anyInt()))
        .andReturn(NOT_ENOUGH_QUOTA);

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
        ContiguousSet.create(Range.closed(0, SchedulerCoreImpl.MAX_TASKS_PER_JOB.get()),
            DiscreteDomain.integers()),
        job.getTaskConfig());
  }

  @Test
  public void testAddInstances() throws Exception {
    TaskConfig existingTask = productionTask();
    TaskConfig newTask = productionTask()
        .setEnvironment(ENV_A)
        .setJobName(KEY_A.getName())
        .setOwner(OWNER_A);
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
    expectNoCronJob(KEY_A);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(KEY_A, existingTask, 1));

    assertTaskCount(1);
    scheduler.addInstances(KEY_A, instances, ITaskConfig.build(newTask));
  }

  private void expectNoCronJob(IJobKey jobKey) throws CronException {
    expect(cronJobManager.hasJob(jobKey)).andReturn(false);
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

  public void changeStatus(
      Query.Builder query,
      ScheduleStatus status,
      Optional<String> message) {

    for (String taskId : Tasks.ids(Storage.Util.consistentFetchTasks(storage, query))) {
      scheduler.setTaskStatus(taskId, status, message);
    }
  }

  public void changeStatus(Query.Builder query, ScheduleStatus status, ScheduleStatus... statuses) {
    for (ScheduleStatus nextStatus
        : ImmutableList.<ScheduleStatus>builder().add(status).add(statuses).build()) {

      changeStatus(query, nextStatus, Optional.<String>absent());
    }
  }

  public void changeStatus(String taskId, ScheduleStatus status, ScheduleStatus... statuses) {
    changeStatus(Query.taskScoped(taskId), status, statuses);
  }

  public void changeStatus(String taskId, ScheduleStatus status, Optional<String> message) {
    changeStatus(Query.taskScoped(taskId), status, message);
  }
}

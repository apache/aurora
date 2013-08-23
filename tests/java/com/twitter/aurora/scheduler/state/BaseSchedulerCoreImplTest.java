package com.twitter.aurora.scheduler.state;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import org.apache.mesos.Protos.SlaveID;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.gen.AssignedTask;
import com.twitter.aurora.gen.Constraint;
import com.twitter.aurora.gen.CronCollisionPolicy;
import com.twitter.aurora.gen.Identity;
import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.JobKey;
import com.twitter.aurora.gen.Quota;
import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.ShardUpdateResult;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.gen.TaskConstraint;
import com.twitter.aurora.gen.TaskEvent;
import com.twitter.aurora.gen.TaskQuery;
import com.twitter.aurora.gen.UpdateResult;
import com.twitter.aurora.gen.ValueConstraint;
import com.twitter.aurora.scheduler.Driver;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.ScheduleException;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.configuration.ConfigurationManager;
import com.twitter.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import com.twitter.aurora.scheduler.configuration.ParsedConfiguration;
import com.twitter.aurora.scheduler.events.PubsubEvent;
import com.twitter.aurora.scheduler.quota.QuotaManager;
import com.twitter.aurora.scheduler.quota.QuotaManager.QuotaManagerImpl;
import com.twitter.aurora.scheduler.quota.Quotas;
import com.twitter.aurora.scheduler.state.CronJobManager.CronScheduler;
import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.aurora.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.aurora.scheduler.storage.Storage.MutateWork;
import com.twitter.aurora.scheduler.storage.StorageBackfill;
import com.twitter.common.base.Closure;
import com.twitter.common.collections.Pair;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static com.twitter.aurora.gen.ScheduleStatus.ASSIGNED;
import static com.twitter.aurora.gen.ScheduleStatus.FAILED;
import static com.twitter.aurora.gen.ScheduleStatus.FINISHED;
import static com.twitter.aurora.gen.ScheduleStatus.KILLED;
import static com.twitter.aurora.gen.ScheduleStatus.KILLING;
import static com.twitter.aurora.gen.ScheduleStatus.LOST;
import static com.twitter.aurora.gen.ScheduleStatus.PENDING;
import static com.twitter.aurora.gen.ScheduleStatus.RESTARTING;
import static com.twitter.aurora.gen.ScheduleStatus.RUNNING;
import static com.twitter.aurora.gen.ScheduleStatus.STARTING;
import static com.twitter.aurora.gen.ScheduleStatus.UPDATING;
import static com.twitter.aurora.gen.UpdateResult.SUCCESS;
import static com.twitter.aurora.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;
import static com.twitter.aurora.scheduler.configuration.ConfigurationManager.hostLimitConstraint;
import static com.twitter.aurora.scheduler.configuration.ConfigurationManager.populateFields;

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
  private static final JobKey KEY_A = JobKeys.from(ROLE_A, ENV_A, JOB_A);
  private static final int ONE_GB = 1024;
  private static final Quota DEFAULT_TASK_QUOTA = new Quota(1.0, ONE_GB, ONE_GB);
  private static final int DEFAULT_TASKS_IN_QUOTA = 10;

  private static final String ROLE_B = "Test_Role_B";
  private static final String USER_B = "Test_User_B";
  private static final Identity OWNER_B = new Identity(ROLE_B, USER_B);
  private static final JobKey KEY_B = JobKeys.from(ROLE_B, ENV_A, JOB_A);

  private static final SlaveID SLAVE_ID = SlaveID.newBuilder().setValue("SlaveId").build();
  private static final String SLAVE_HOST_1 = "SlaveHost1";

  private Driver driver;
  private Function<TaskConfig, String> taskIdGenerator;
  private StateManagerImpl stateManager;
  private Storage storage;
  private SchedulerCoreImpl scheduler;
  private CronScheduler cronScheduler;
  private CronJobManager cron;
  private QuotaManager quotaManager;
  private FakeClock clock;
  private Closure<PubsubEvent> eventSink;

  @Before
  public void setUp() throws Exception {
    driver = createMock(Driver.class);

    // TODO(William Farner): Set up explicit expectations for calls to generate task IDs.
    taskIdGenerator = createMock(new Clazz<Function<TaskConfig, String>>() { });
    final AtomicLong idCounter = new AtomicLong();
    expect(taskIdGenerator.apply(EasyMock.<TaskConfig>anyObject())).andAnswer(
        new IAnswer<String>() {
          @Override public String answer() throws Throwable {
            return "task-" + idCounter.incrementAndGet();
          }
        }).anyTimes();

    clock = new FakeClock();
    eventSink = createMock(new Clazz<Closure<PubsubEvent>>() { });
    eventSink.execute(EasyMock.<PubsubEvent>anyObject());
    cronScheduler = createMock(CronScheduler.class);
    expectLastCall().anyTimes();
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

  private static Quota scale(Quota quota, int factor) {
    return new Quota()
        .setNumCpus(quota.getNumCpus() * factor)
        .setRamMb(quota.getRamMb() * factor)
        .setDiskMb(quota.getDiskMb() * factor);
  }

  private void buildScheduler(Storage newStorage) throws Exception {
    this.storage = newStorage;
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        StorageBackfill.backfill(storeProvider, clock);
      }
    });

    stateManager = new StateManagerImpl(storage, clock, driver, taskIdGenerator, eventSink);
    ImmediateJobManager immediateManager = new ImmediateJobManager(stateManager, storage);
    quotaManager = new QuotaManagerImpl(storage);
    cron = new CronJobManager(stateManager, storage, cronScheduler);
    scheduler = new SchedulerCoreImpl(
        storage,
        cron,
        immediateManager,
        stateManager,
        quotaManager);
    cron.schedulerCore = scheduler;
    immediateManager.schedulerCore = scheduler;

    // Apply a default quota for users so we don't have to give quota for every test.
    quotaManager.setQuota(ROLE_A, scale(DEFAULT_TASK_QUOTA, DEFAULT_TASKS_IN_QUOTA));
    quotaManager.setQuota(OWNER_B.getRole(), scale(DEFAULT_TASK_QUOTA, DEFAULT_TASKS_IN_QUOTA));
  }

  @Test(expected = ScheduleException.class)
  public void testCreateJobNoQuota() throws Exception {
    control.replay();
    buildScheduler();

    quotaManager.setQuota(ROLE_A, Quotas.NO_QUOTA);
    scheduler.createJob(makeJob(KEY_A, 1));
  }

  @Test
  public void testCreateNonproductionJobNoQuota() throws Exception {
    control.replay();
    buildScheduler();

    TaskConfig task = nonProductionTask();
    scheduler.createJob(makeJob(KEY_A, task, 100));
    assertEquals(100, getTasks(Query.roleScoped(ROLE_A)).size());
  }

  @Test(expected = ScheduleException.class)
  public void testCreateJobExceedsQuota() throws Exception {
    control.replay();
    buildScheduler();
    scheduler.createJob(makeJob(KEY_A, DEFAULT_TASKS_IN_QUOTA + 1));
  }

  @Test
  public void testCreateJob() throws Exception {
    int numTasks = 10;

    control.replay();
    buildScheduler();

    ParsedConfiguration job = makeJob(KEY_A, numTasks);
    scheduler.createJob(job);
    assertTaskCount(numTasks);

    Set<ScheduledTask> tasks = Storage.Util.consistentFetchTasks(storage, Query.jobScoped(KEY_A));
    assertEquals(numTasks, tasks.size());
    for (ScheduledTask state : tasks) {
      assertEquals(PENDING, state.getStatus());
      assertTrue(state.getAssignedTask().isSetTaskId());
      assertFalse(state.getAssignedTask().isSetSlaveId());
      // Need to clear shard ID since that was assigned when the job is scheduled.
      state.getAssignedTask().getTask().setShardId(0);
      assertEquals(populateFields(job.getJobConfig()), state.getAssignedTask().getTask());
    }
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

    final TaskConfig storedTask = new TaskConfig()
        .setOwner(OWNER_A)
        .setJobName(JOB_A)
        .setEnvironment(ENV_A)
        .setNumCpus(1.0)
        .setRamMb(ONE_GB)
        .setDiskMb(500)
        .setShardId(0)
        .setThermosConfig(new byte[] {})
        .setRequestedPorts(ImmutableSet.<String>of())
        .setConstraints(ImmutableSet.<Constraint>of())
        .setTaskLinks(ImmutableMap.<String, String>of());

    storage.write(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(
            new ScheduledTask()
                .setStatus(PENDING)
                .setAssignedTask(
                    new AssignedTask()
                        .setTaskId(storedTaskId)
                        .setTask(storedTask))));
      }
    });

    buildScheduler(storage);

    // Check that the missing event was synthesized.
    assertEquals(PENDING, Iterables.getLast(getTask(storedTaskId).getTaskEvents()).getStatus());

    assignTask(storedTaskId, SLAVE_ID, SLAVE_HOST_1);

    // Since task fields are backfilled with defaults, additional flags should be filled.
    TaskConfig expected = new TaskConfig(storedTask)
        .setProduction(false)
        .setHealthCheckIntervalSecs(30)
        .setMaxTaskFailures(1)
        .setThermosConfig(new byte[] {})
        .setConstraints(ImmutableSet.of(ConfigurationManager.hostLimitConstraint(1)));

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

    ParsedConfiguration job = makeJob(KEY_A, 10);
    final Set<ScheduledTask> badTasks = ImmutableSet.copyOf(Iterables
        .transform(job.getTaskConfigs(),
            new Function<TaskConfig, ScheduledTask>() {
              @Override public ScheduledTask apply(TaskConfig task) {
                return new ScheduledTask()
                    .setStatus(RUNNING)
                    .setAssignedTask(
                        new AssignedTask()
                            .setTaskId("task-" + taskId.incrementAndGet())
                            .setTask(task.setShardId(0)));
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
          expectRejected(ident,
              new JobKey().setRole(ident.getRole()).setEnvironment(env).setName(job));
        }
      }
    }

    for (String jobName : invalidIdentifiers) {
      expectRejected(validIdentity,
          new JobKey().setRole(validIdentity.getRole()).setEnvironment(validJob).setName(jobName));
    }

    for (Identity ident : invalidIdentities) {
      expectRejected(ident, KEY_A);
    }
  }

  private void expectRejected(Identity identity, JobKey jobKey) throws ScheduleException {
    try {
      scheduler.createJob(
          ParsedConfiguration.fromUnparsed(makeJob(jobKey, 1).getJobConfig().setOwner(identity)));
      fail("Job owner/name should have been rejected.");
    } catch (TaskDescriptionException e) {
      // Expected.
    }
  }

  @Test
  public void testSortableTaskIds() throws Exception {
    control.replay();
    buildScheduler();

    for (ScheduledTask task : getTasks(Query.unscoped())) {
      assertEquals(OWNER_A, task.getAssignedTask().getTask().getOwner());
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
    ParsedConfiguration parsedConfiguration = makeCronJob(KEY_A, 1, "1 1 1 1 1");
    JobConfiguration job = parsedConfiguration.getJobConfig();
    expect(cronScheduler.schedule(eq(job.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");

    control.replay();
    buildScheduler();

    // Cron jobs are scheduled on a delay, so this job's tasks will not be scheduled immediately,
    // but duplicate jobs should still be rejected.
    scheduler.createJob(parsedConfiguration);
    assertTaskCount(0);

    scheduler.createJob(makeJob(KEY_A, 1));
  }

  @Test
  public void testStartCronJob() throws Exception {
    // Create a cron job, ask the scheduler to start it, and ensure that the tasks exist
    // in the PENDING state.

    ParsedConfiguration parsedConfiguration = makeCronJob(KEY_A, 1, "1 1 1 1 1");
    JobConfiguration job = parsedConfiguration.getJobConfig();
    expect(cronScheduler.schedule(eq(job.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");

    control.replay();
    buildScheduler();

    scheduler.createJob(parsedConfiguration);
    assertTaskCount(0);

    scheduler.startCronJob(job.getKey());
    assertEquals(PENDING, getOnlyTask(Query.jobScoped(job.getKey())).getStatus());
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

    ParsedConfiguration parsedConfiguration = makeCronJob(KEY_A, 1, "1 1 1 1 1");
    JobConfiguration job = parsedConfiguration.getJobConfig();
    expect(cronScheduler.schedule(eq(job.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");

    control.replay();
    buildScheduler();

    scheduler.createJob(parsedConfiguration);
    assertTaskCount(0);

    scheduler.startCronJob(KEY_B);
  }

  @Test
  public void testStartRunningCronJob() throws Exception {
    // Start a cron job that is already started by an earlier
    // call and is PENDING. Make sure it follows the cron collision policy.
    ParsedConfiguration parsedConfiguration = makeCronJob(KEY_A, 1, "1 1 1 1 1");
    JobConfiguration job = parsedConfiguration.getJobConfig();
    job.setCronCollisionPolicy(CronCollisionPolicy.KILL_EXISTING);
    expect(cronScheduler.schedule(eq(job.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");

    control.replay();
    buildScheduler();

    scheduler.createJob(parsedConfiguration);
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
    ParsedConfiguration parsedConfiguration = makeCronJob(KEY_A, 1, "1 1 1 1 1");
    JobConfiguration job = parsedConfiguration.getJobConfig();
    parsedConfiguration.getJobConfig().setCronCollisionPolicy(CronCollisionPolicy.RUN_OVERLAP);
    expect(cronScheduler.schedule(eq(job.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");

    control.replay();
    buildScheduler();

    scheduler.createJob(parsedConfiguration);
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
    ParsedConfiguration parsedConfiguration = makeCronJob(KEY_A, 1, "1 1 1 1 1");
    JobConfiguration job = parsedConfiguration.getJobConfig();
    expect(cronScheduler.schedule(eq(job.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");
    cronScheduler.deschedule("key");

    ParsedConfiguration updated = makeCronJob(KEY_A, 1, "1 2 3 4 5");
    JobConfiguration updatedJob = updated.getJobConfig();
    expect(cronScheduler.schedule(eq(updatedJob.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key2");

    control.replay();
    buildScheduler();

    scheduler.createJob(parsedConfiguration);
    assertTrue(cron.hasJob(KEY_A));

    scheduler.killTasks(Query.jobScoped(KEY_A), OWNER_A.getUser());
    scheduler.createJob(updated);

    JobConfiguration stored = Iterables.getOnlyElement(cron.getJobs());
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
    scheduler.createJob(makeJob(KEY_A.deepCopy().setName(KEY_A.getName() + "service"), task, 5));

    assertEquals(10, getTasksByStatus(PENDING).size());
    changeStatus(Query.roleScoped(ROLE_A), ASSIGNED);
    changeStatus(Query.roleScoped(ROLE_A), STARTING);
    assertEquals(10, getTasksByStatus(STARTING).size());

    changeStatus(Query.roleScoped(ROLE_A), RUNNING);
    assertEquals(10, getTasksByStatus(RUNNING).size());

    // Service tasks will move back into PENDING state after finishing.
    changeStatus(Query.roleScoped(ROLE_A), FINISHED);
    Set<ScheduledTask> newTasks = getTasksByStatus(PENDING);
    assertEquals(5, newTasks.size());
    for (ScheduledTask state : newTasks) {
      assertEquals(
          getTask(state.getAncestorId()).getAssignedTask().getTask().getShardId(),
          state.getAssignedTask().getTask().getShardId());
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
      ScheduledTask rescheduled = getOnlyTask(Query.unscoped().byStatus(PENDING));
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
    scheduler.createJob(makeJob(KEY_A.deepCopy().setName(KEY_A.getName() + "service"), task, 5));

    assertEquals(10, getTasksByStatus(PENDING).size());
    changeStatus(Query.roleScoped(ROLE_A), ASSIGNED);
    changeStatus(Query.roleScoped(ROLE_A), STARTING);
    assertEquals(10, getTasksByStatus(STARTING).size());
    changeStatus(Query.roleScoped(ROLE_A), RUNNING);
    assertEquals(10, getTasksByStatus(RUNNING).size());

    // All tasks will move back into PENDING state after getting KILLED.
    changeStatus(Query.roleScoped(ROLE_A), KILLED);
    Set<ScheduledTask> newTasks = getTasksByStatus(PENDING);
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
        ScheduledTask rescheduled = getOnlyTask(Query.unscoped().byStatus(PENDING));
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
    ParsedConfiguration parsedConfiguration = makeCronJob(KEY_A, 10, "1 1 1 1 1");
    JobConfiguration job = parsedConfiguration.getJobConfig();
    expect(cronScheduler.schedule(eq(job.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");

    control.replay();
    buildScheduler();

    scheduler.createJob(parsedConfiguration);
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
    ParsedConfiguration parsedConfiguration = makeCronJob(KEY_A, 10, "1 1 1 1 1");
    JobConfiguration job = parsedConfiguration.getJobConfig();
    job.setCronCollisionPolicy(CronCollisionPolicy.KILL_EXISTING);
    expect(cronScheduler.schedule(eq(job.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");

    control.replay();
    buildScheduler();

    scheduler.createJob(parsedConfiguration);
    assertTaskCount(0);

    try {
      scheduler.createJob(parsedConfiguration);
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
      scheduler.createJob(parsedConfiguration);
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

    Set<ScheduledTask> tasks = Storage.Util.consistentFetchTasks(storage, Query.jobScoped(KEY_A));
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
    ParsedConfiguration parsedConfiguration = makeCronJob(KEY_A, 1, "1 1 1 1 1");
    JobConfiguration job = parsedConfiguration.getJobConfig();
    job.setCronCollisionPolicy(CronCollisionPolicy.KILL_EXISTING);
    expect(cronScheduler.schedule(eq(job.getCronSchedule()), EasyMock.<Runnable>anyObject()))
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

    Set<ScheduledTask> tasks = Storage.Util.consistentFetchTasks(storage, Query.jobScoped(KEY_A));
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
    ParsedConfiguration config = makeCronJob(KEY_A, 10, "1 1 1 1 1");
    JobConfiguration job = config.getJobConfig();
    expect(cronScheduler.schedule(eq(job.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");
    cronScheduler.deschedule("key");

    control.replay();
    buildScheduler();

    scheduler.createJob(config);
    assertTrue(cron.hasJob(KEY_A));
    scheduler.startCronJob(KEY_A);
    assertTaskCount(10);

    scheduler.killTasks(Query.shardScoped(KEY_A, 0), USER_A);
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

    scheduler.createJob(makeJob(KEY_A.deepCopy().setName(KEY_A.getName() + "2"), 5));
    assertTaskCount(10);

    scheduler.killTasks(queryJob(OWNER_A, JOB_A + "2"), OWNER_A.getUser());
    assertTaskCount(5);

    for (ScheduledTask state : Storage.Util.consistentFetchTasks(storage, Query.unscoped())) {
      assertEquals(JOB_A, Tasks.getJob(state));
    }
  }

  @Test
  public void testSlaveDeletesTasks() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(KEY_A, 2));

    Query.Builder builder = Query.unscoped().active();
    String taskId1 = Tasks.id(getOnlyTask(builder.byShards(KEY_A, 0)));
    String taskId2 = Tasks.id(getOnlyTask(builder.byShards(KEY_A, 1)));

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

    ScheduledTask rescheduled = Iterables.getOnlyElement(getTasksByStatus(PENDING));
    assertEquals(taskId1, rescheduled.getAncestorId());
  }

  @Test
  public void testStartAndFinishUpdate() throws Exception {
    control.replay();
    buildScheduler();

    ParsedConfiguration job = makeJob(KEY_A, 1);
    scheduler.createJob(job);
    Optional<String> updateToken = scheduler.initiateJobUpdate(job);
    scheduler.finishUpdate(KEY_A, USER_A, updateToken, SUCCESS);

    // If the finish update succeeded internally, we should be able to start a new update.
    assertTrue(scheduler.initiateJobUpdate(job).isPresent());
  }

  @Test
  public void testUpdateCronJob() throws Exception {
    ParsedConfiguration parsedConfiguration = makeCronJob(KEY_A, 1, "1 1 1 1 1");
    JobConfiguration job = parsedConfiguration.getJobConfig();
    expect(cronScheduler.schedule(eq(job.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");
    cronScheduler.deschedule("key");

    ParsedConfiguration updated = makeCronJob(KEY_A, 5, "1 2 3 4 5");
    JobConfiguration updatedJob = updated.getJobConfig();
    expect(cronScheduler.schedule(eq(updatedJob.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key2");

    control.replay();
    buildScheduler();

    scheduler.createJob(parsedConfiguration);
    assertFalse(scheduler.initiateJobUpdate(updated).isPresent());
    scheduler.startCronJob(KEY_A);
    assertTaskCount(5);
  }

  @Test(expected = ScheduleException.class)
  public void testInvalidStartUpdate() throws Exception {
    expectKillTask(1);
    control.replay();
    buildScheduler();

    ParsedConfiguration job = makeJob(KEY_A, 1);
    scheduler.createJob(job);

    changeStatus(Query.roleScoped(ROLE_A), ASSIGNED);
    changeStatus(Query.roleScoped(ROLE_A), STARTING);
    changeStatus(Query.roleScoped(ROLE_A), RUNNING);
    scheduler.initiateJobUpdate(job);
    changeStatus(Query.roleScoped(ROLE_A), UPDATING);

    scheduler.initiateJobUpdate(job);
  }

  @Test
  public void testFinishUpdateNotFound() throws Exception {
    control.replay();
    buildScheduler();

    try {
      scheduler.finishUpdate(KEY_A.deepCopy().setName("t"), "foo", Optional.of("foo"), SUCCESS);
      fail("Call should have failed.");
    } catch (ScheduleException e) {
      // Expected.
    }

    try {
      scheduler.finishUpdate(
          KEY_A.deepCopy().setName("t"), "f", Optional.<String>absent(), SUCCESS);
      fail("Call should have failed.");
    } catch (ScheduleException e) {
      // Expected.
    }
  }

  @Test
  public void testFinishUpdateInvalidToken() throws Exception {
    control.replay();
    buildScheduler();

    ParsedConfiguration job = makeJob(KEY_A, 1);
    scheduler.createJob(job);
    Optional<String> token = scheduler.initiateJobUpdate(job);

    try {
      scheduler.finishUpdate(KEY_A, USER_B, Optional.of("foo"), SUCCESS);
      fail("Finish update should have failed.");
    } catch (ScheduleException e) {
      // expected.
    }

    scheduler.finishUpdate(KEY_A, USER_A, token, SUCCESS);
  }

  @Test
  public void testRejectsSimultaneousUpdates() throws Exception {
    control.replay();
    buildScheduler();

    ParsedConfiguration job = makeJob(KEY_A, 1);
    scheduler.createJob(job);
    Optional<String> token = scheduler.initiateJobUpdate(job);

    try {
      scheduler.initiateJobUpdate(job);
      fail("Second update should have failed.");
    } catch (ScheduleException e) {
      // expected.
    }

    scheduler.finishUpdate(KEY_A, USER_A, token, SUCCESS);
  }

  private void verifyUpdate(
      Set<ScheduledTask> tasks,
      ParsedConfiguration job,
      Closure<ScheduledTask> updatedTaskChecker) {

    Map<Integer, ScheduledTask> fetchedShards =
        Maps.uniqueIndex(tasks, Tasks.SCHEDULED_TO_SHARD_ID);
    Map<Integer, TaskConfig> originalConfigsByShard =
        Maps.uniqueIndex(job.getTaskConfigs(), Tasks.INFO_TO_SHARD_ID);
    assertEquals(originalConfigsByShard.keySet(), fetchedShards.keySet());
    for (ScheduledTask task : tasks) {
      updatedTaskChecker.execute(task);
    }
  }

  private static final Set<String> OLD_PORTS = ImmutableSet.of("old");
  private static final Set<String> NEW_PORTS = ImmutableSet.of("new");

  // TODO(William Farner): Rework this - it's a nightmare to follow.
  private abstract class UpdaterTest {
    UpdaterTest(int numTasks, int additionalTasks) throws Exception {
      control.replay();
      buildScheduler();

      ParsedConfiguration job = makeJob(KEY_A, productionTask().deepCopy(), numTasks);
      for (TaskConfig config : job.getTaskConfigs()) {
        config.setRequestedPorts(OLD_PORTS);
      }
      scheduler.createJob(job);

      ParsedConfiguration updatedJob =
          makeJob(KEY_A, productionTask().deepCopy(), numTasks + additionalTasks);
      for (TaskConfig config : updatedJob.getTaskConfigs()) {
        config.setRequestedPorts(NEW_PORTS);
      }
      Optional<String> updateToken = scheduler.initiateJobUpdate(updatedJob);

      Set<Integer> jobShards = FluentIterable.from(updatedJob.getTaskConfigs())
          .transform(Tasks.INFO_TO_SHARD_ID).toSet();

      UpdateResult result = performRegisteredUpdate(
          updatedJob.getJobConfig(),
          updateToken.get(),
          jobShards,
          numTasks,
          additionalTasks);

      scheduler.finishUpdate(KEY_A, USER_A, updateToken, result);
      postUpdate();
      Set<ScheduledTask> tasks =
          getTasks(Query.jobScoped(KEY_A).active());
      verify(tasks, job, updatedJob);
      scheduler.initiateJobUpdate(job);
    }

    abstract UpdateResult performRegisteredUpdate(
        JobConfiguration job,
        String updateToken,
        Set<Integer> jobShards,
        int numTasks,
        int additionalTasks) throws Exception;

    void postUpdate() {
      // Default no-op.
    }

    abstract void verify(
        Set<ScheduledTask> tasks,
        ParsedConfiguration oldJob,
        ParsedConfiguration updatedJob);
  }

  @Test
  public void testUpdateShards() throws Exception {
    int numTasks = 10;
    int additionalTasks = 0;
    // Kill Tasks called at RUNNING->UPDATING
    expectKillTask(numTasks);

    new UpdaterTest(numTasks, additionalTasks) {
      @Override UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
          Set<Integer> jobShards, int numTasks, int additionalTasks) throws Exception {
        changeStatus(Query.roleScoped(ROLE_A), ASSIGNED);
        changeStatus(Query.roleScoped(ROLE_A), RUNNING);

        ImmutableMap.Builder<Integer, ShardUpdateResult> expected = ImmutableMap.builder();
        StateManagerImpl.putResults(expected, ShardUpdateResult.RESTARTING, jobShards);
        assertEquals(
            expected.build(),
            scheduler.updateShards(KEY_A, USER_A, jobShards, updateToken));
        assertEquals(numTasks, getTasksByStatus(UPDATING).size());

        changeStatus(Query.roleScoped(ROLE_A), FINISHED);
        changeStatus(Query.unscoped().byStatus(PENDING), ASSIGNED);
        changeStatus(Query.unscoped().byStatus(ASSIGNED), RUNNING);

        return SUCCESS;
      }

      @Override void verify(
          Set<ScheduledTask> tasks,
          ParsedConfiguration oldJob,
          ParsedConfiguration updatedJob) {

        verifyUpdate(tasks, oldJob, VERIFY_NEW_TASK);
      }
    };
  }

  @Test
  public void testAddingShards() throws Exception {
    control.replay();
    buildScheduler();

    // Use command line wildcards to detect bugs where command lines with populated wildcards
    // make tasks appear different.
    Set<String> ports = ImmutableSet.of("foo");
    TaskConfig task = productionTask().deepCopy().setRequestedPorts(ports);
    scheduler.createJob(makeJob(KEY_A, task, 3));
    List<String> taskIds = Ordering.natural().sortedCopy(Tasks.ids(getTasksOwnedBy(OWNER_A)));

    Set<Integer> port = ImmutableSet.of(80);
    assignTask(taskIds.get(0), SLAVE_ID, SLAVE_HOST_1, port);
    assignTask(taskIds.get(1), SLAVE_ID, SLAVE_HOST_1, port);
    assignTask(taskIds.get(2), SLAVE_ID, SLAVE_HOST_1, port);
    changeStatus(Query.roleScoped(ROLE_A), ASSIGNED);
    changeStatus(Query.roleScoped(ROLE_A), RUNNING);

    ParsedConfiguration updatedJob = makeJob(KEY_A, task, 10);
    Set<String> differentPorts = ImmutableSet.of("different");
    // Change the requested ports on shard 1 to ensure that it (and only it) gets restarted as a
    // part of the update.
    Iterables.getOnlyElement(Iterables.filter(updatedJob.getTaskConfigs(),
        Predicates.compose(Predicates.equalTo(1), Tasks.INFO_TO_SHARD_ID)))
        .setRequestedPorts(differentPorts);

    Optional<String> updateToken = scheduler.initiateJobUpdate(updatedJob);

    ImmutableMap.Builder<Integer, ShardUpdateResult> expected = ImmutableMap.builder();
    StateManagerImpl.putResults(expected, ShardUpdateResult.ADDED, ImmutableSet.of(3, 4, 5));
    assertEquals(
        expected.build(),
        scheduler.updateShards(KEY_A, JOB_A, ImmutableSet.of(3, 4, 5), updateToken.get()));

    expected = ImmutableMap.builder();
    StateManagerImpl.putResults(expected, ShardUpdateResult.ADDED, ImmutableSet.of(6, 7, 8));
    assertEquals(
        expected.build(),
        scheduler.updateShards(KEY_A, USER_A, ImmutableSet.of(6, 7, 8), updateToken.get()));

    expected = ImmutableMap.builder();
    StateManagerImpl.putResults(expected, ShardUpdateResult.ADDED, ImmutableSet.of(9));
    assertEquals(
        expected.build(),
        scheduler.updateShards(KEY_A, USER_A, ImmutableSet.of(9), updateToken.get()));
    scheduler.finishUpdate(KEY_A, USER_A, updateToken, UpdateResult.SUCCESS);
  }

  @Test
  public void testRollback() throws Exception {
    int numTasks = 4;
    // Kill Tasks called at RUNNING->UPDATING.
    expectKillTask(numTasks);

    new UpdaterTest(numTasks, 0) {
      @Override UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
          Set<Integer> jobShards, int numTasks, int additionalTask) throws Exception {
        changeStatus(Query.roleScoped(ROLE_A), ASSIGNED);
        changeStatus(Query.roleScoped(ROLE_A), RUNNING);

        ImmutableMap.Builder<Integer, ShardUpdateResult> expected = ImmutableMap.builder();
        StateManagerImpl.putResults(
            expected,
            ShardUpdateResult.RESTARTING,
            ImmutableSet.of(0, 1, 2, 3));
        assertEquals(
            expected.build(),
            scheduler.updateShards(KEY_A, USER_A, jobShards, updateToken));
        assertEquals(numTasks, getTasksByStatus(UPDATING).size());

        changeStatus(Query.roleScoped(ROLE_A), KILLED);

        expected = ImmutableMap.builder();
        StateManagerImpl.putResults(
            expected,
            ShardUpdateResult.RESTARTING,
            ImmutableSet.of(0, 1, 2, 3));
        assertEquals(
            expected.build(),
            scheduler.rollbackShards(KEY_A, USER_A, jobShards, updateToken));

        changeStatus(Query.unscoped().byStatus(PENDING), ASSIGNED);
        changeStatus(Query.unscoped().byStatus(ASSIGNED), RUNNING);

        return UpdateResult.FAILED;
      }

      @Override void verify(
          Set<ScheduledTask> tasks,
          ParsedConfiguration oldJob,
          ParsedConfiguration updatedJob) {

        verifyUpdate(tasks, oldJob, new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask state) {
            assertEquals(OLD_PORTS, Tasks.SCHEDULED_TO_INFO.apply(state).getRequestedPorts());
          }
        });
      }
    };
  }

  private static Map<Integer, ShardUpdateResult> shardResults(
      int lower,
      int upper,
      ShardUpdateResult result) {

    ImmutableMap.Builder<Integer, ShardUpdateResult> results = ImmutableMap.builder();
    for (int i = lower; i <= upper; i++) {
      results.put(i, result);
    }
    return results.build();
  }

  private static Map<Integer, ShardUpdateResult> shardResults(
      int numShards,
      ShardUpdateResult result) {

    return shardResults(0, numShards - 1, result);
  }

  @Test
  public void testNoopUpdateRollback() throws Exception {
    control.replay();
    buildScheduler();

    int numTasks = 2;

    // Use command line wildcards to detect bugs where command lines with populated wildcards
    // make tasks appear different.
    ParsedConfiguration job = makeJob(KEY_A, productionTask(), numTasks);
    scheduler.createJob(job);
    List<String> taskIds = Ordering.natural().sortedCopy(Tasks.ids(getTasksOwnedBy(OWNER_A)));

    assignTask(taskIds.get(0), SLAVE_ID, SLAVE_HOST_1);
    assignTask(taskIds.get(1), SLAVE_ID, SLAVE_HOST_1);
    changeStatus(Query.roleScoped(ROLE_A), ASSIGNED);
    changeStatus(Query.roleScoped(ROLE_A), RUNNING);

    Optional<String> updateToken = scheduler.initiateJobUpdate(job);

    assertEquals(
        shardResults(numTasks, ShardUpdateResult.UNCHANGED),
        scheduler.updateShards(KEY_A, USER_A, ImmutableSet.of(0, 1), updateToken.get()));

    assertEquals(
        shardResults(numTasks, ShardUpdateResult.UNCHANGED),
        scheduler.rollbackShards(KEY_A, USER_A, ImmutableSet.of(0, 1), updateToken.get()));

    scheduler.finishUpdate(KEY_A, USER_A, updateToken, UpdateResult.FAILED);
  }

  private static Closure<ScheduledTask> verifyPorts(final Set<String> requestedPorts) {
    return new Closure<ScheduledTask>() {
      @Override public void execute(ScheduledTask task) {
        assertEquals(requestedPorts, Tasks.SCHEDULED_TO_INFO.apply(task).getRequestedPorts());
      }
    };
  }
  private static final Closure<ScheduledTask> VERIFY_OLD_TASK = verifyPorts(OLD_PORTS);
  private static final Closure<ScheduledTask> VERIFY_NEW_TASK = verifyPorts(NEW_PORTS);

  @Test
  public void testInvalidTransition() throws Exception {
    // Kill Tasks called at RUNNING->UPDATING and UPDATING->RUNNING (Invalid).
    final int numTasks = 4;
    int expectedKillTasks = 8;
    expectKillTask(expectedKillTasks);

    new UpdaterTest(numTasks, 0) {
      @Override UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
          Set<Integer> jobShards, int numTasks, int additionalTasks) throws Exception {
        changeStatus(Query.roleScoped(ROLE_A), ASSIGNED);
        changeStatus(Query.roleScoped(ROLE_A), RUNNING);

        assertEquals(
            shardResults(numTasks, ShardUpdateResult.RESTARTING),
            scheduler.updateShards(KEY_A, USER_A, jobShards, updateToken));
        assertEquals(numTasks, getTasksByStatus(UPDATING).size());

        changeStatus(Query.roleScoped(ROLE_A), RUNNING);
        changeStatus(Query.roleScoped(ROLE_A), KILLED);
        changeStatus(Query.unscoped().byStatus(PENDING), ASSIGNED);
        changeStatus(Query.unscoped().byStatus(ASSIGNED), RUNNING);

        return SUCCESS;
      }

      @Override void verify(
          Set<ScheduledTask> tasks,
          ParsedConfiguration oldJob,
          ParsedConfiguration updatedJob) {

        verifyUpdate(tasks, oldJob, VERIFY_NEW_TASK);
      }
    };
  }

  @Test
  public void testPendingToUpdating() throws Exception {
    int numTasks = 4;
    new UpdaterTest(numTasks, 0) {
      @Override UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
          Set<Integer> jobShards, int numTasks, int additionalTasks) throws Exception {

        assertEquals(
            shardResults(numTasks, ShardUpdateResult.RESTARTING),
            scheduler.updateShards(KEY_A, USER_A, jobShards, updateToken));
        assertEquals(numTasks, getTasksByStatus(PENDING).size());

        return SUCCESS;
      }

      @Override void verify(
          Set<ScheduledTask> tasks,
          ParsedConfiguration oldJob,
          ParsedConfiguration updatedJob) {

        verifyUpdate(tasks, oldJob, VERIFY_NEW_TASK);
      }
    };
  }

  @Test
  public void testIncreaseShardsUpdate() throws Exception {
    int numTasks = 2;
    // Kill Tasks called at RUNNING->UPDATING.
    expectKillTask(numTasks);

    new UpdaterTest(numTasks, 2) {
      @Override UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
          Set<Integer> jobShards, int numTasks, int additionalTasks) throws Exception {
        changeStatus(Query.roleScoped(ROLE_A), ASSIGNED);
        changeStatus(Query.roleScoped(ROLE_A), RUNNING);

        Map<Integer, ShardUpdateResult> expected =
            ImmutableMap.<Integer, ShardUpdateResult>builder()
                .putAll(shardResults(numTasks, ShardUpdateResult.RESTARTING))
                .putAll(shardResults(2, 3, ShardUpdateResult.ADDED))
                .build();
        assertEquals(
            expected,
            scheduler.updateShards(KEY_A, USER_A, jobShards, updateToken));
        changeStatus(Query.unscoped().byStatus(UPDATING), KILLED);

        assertEquals(numTasks + additionalTasks, getTasksByStatus(PENDING).size());

        return SUCCESS;
      }

      @Override void verify(
          Set<ScheduledTask> tasks,
          ParsedConfiguration oldJob,
          ParsedConfiguration updatedJob) {

        verifyUpdate(tasks, updatedJob, VERIFY_NEW_TASK);
      }
    };
  }

  @Test
  public void testDecreaseShardsUpdate() throws Exception {
    int numTasks = 4;
    expectKillTask(numTasks);

    new UpdaterTest(numTasks, -2) {
      @Override UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
          Set<Integer> jobShards, int numTasks, int additionalTasks) throws Exception {
        changeStatus(Query.roleScoped(ROLE_A), ASSIGNED);
        changeStatus(Query.roleScoped(ROLE_A), RUNNING);

        assertEquals(
            shardResults(2, ShardUpdateResult.RESTARTING),
            scheduler.updateShards(KEY_A, USER_A, jobShards, updateToken));
        changeStatus(Query.unscoped().byStatus(UPDATING), FINISHED);

        assertEquals(numTasks + additionalTasks, getTasksByStatus(PENDING).size());
        return SUCCESS;
      }

      @Override void postUpdate() {
        changeStatus(Query.unscoped().byStatus(KILLING), FINISHED);
      }

      @Override void verify(
          Set<ScheduledTask> tasks,
          ParsedConfiguration oldJob,
          ParsedConfiguration updatedJob) {

        verifyUpdate(tasks, updatedJob, VERIFY_NEW_TASK);
      }
    };
  }

  @Test
  public void testIncreaseShardsRollback() throws Exception {
    final int numTasks = 2;
    // Kill Tasks called at RUNNING->UPDATING.
    expectKillTask(numTasks);

    new UpdaterTest(numTasks, 2) {
      @Override UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
          Set<Integer> jobShards, int numTasks, int additionalTasks) throws Exception {
        changeStatus(Query.roleScoped(ROLE_A), ASSIGNED);
        changeStatus(Query.roleScoped(ROLE_A), RUNNING);

        Map<Integer, ShardUpdateResult> expected =
            ImmutableMap.<Integer, ShardUpdateResult>builder()
                .putAll(shardResults(numTasks, ShardUpdateResult.RESTARTING))
                .putAll(shardResults(2, 3, ShardUpdateResult.ADDED))
                .build();
        assertEquals(
            expected,
            scheduler.updateShards(KEY_A, USER_A, jobShards, updateToken));
        changeStatus(Query.unscoped().byStatus(UPDATING), KILLED);

        assertEquals(numTasks + additionalTasks, getTasksByStatus(PENDING).size());

        assertEquals(
            shardResults(numTasks, ShardUpdateResult.RESTARTING),
            scheduler.rollbackShards(KEY_A, USER_A, ImmutableSet.of(0, 1), updateToken));

        return UpdateResult.FAILED;
      }

      @Override void postUpdate() {
        changeStatus(Query.unscoped().byStatus(KILLING), FINISHED);
        assertEquals(numTasks,
            getTasks(Query.jobScoped(KEY_A).active()).size());
      }

      @Override void verify(
          Set<ScheduledTask> tasks,
          ParsedConfiguration oldJob,
          ParsedConfiguration updatedJob) {

        verifyUpdate(tasks, oldJob, VERIFY_OLD_TASK);
      }
    };
  }

  @Test(expected = ScheduleException.class)
  public void testIncreaseShardsExceedsQuota() throws Exception {
    int numTasks = DEFAULT_TASKS_IN_QUOTA;
    int additionalTasks = 1;

    control.replay();
    buildScheduler();

    ParsedConfiguration job = makeJob(KEY_A, productionTask().deepCopy(), numTasks);
    for (TaskConfig config : job.getTaskConfigs()) {
      config.setRequestedPorts(OLD_PORTS);
    }
    scheduler.createJob(job);

    ParsedConfiguration updatedJob =
        makeJob(KEY_A, productionTask().deepCopy(), numTasks + additionalTasks);
    for (TaskConfig config : updatedJob.getTaskConfigs()) {
      config.setRequestedPorts(NEW_PORTS);
    }
    scheduler.initiateJobUpdate(updatedJob);
  }

  @Test
  public void testDecreaseShardsRollback() throws Exception {
    final int numTasks = 4;
    int additionalTasks = -2;
    // Kill Tasks called at RUNNING->UPDATING and PENDING->ROLLBACK
    int expectedKillTasks = 2;
    expectKillTask(expectedKillTasks);

    new UpdaterTest(numTasks, additionalTasks) {
      @Override UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
          Set<Integer> jobShards, int numTasks, int additionalTasks) throws Exception {
        changeStatus(Query.roleScoped(ROLE_A), ASSIGNED);
        changeStatus(Query.roleScoped(ROLE_A), RUNNING);

        assertEquals(
            shardResults(2, ShardUpdateResult.RESTARTING),
            scheduler.updateShards(KEY_A, USER_A, jobShards, updateToken));
        changeStatus(Query.unscoped().byStatus(UPDATING), KILLED);

        assertEquals(numTasks + additionalTasks, getTasksByStatus(PENDING).size());

        assertEquals(
            shardResults(2, ShardUpdateResult.RESTARTING),
            scheduler.rollbackShards(KEY_A, USER_A, jobShards, updateToken));

        return UpdateResult.FAILED;
      }

      @Override void postUpdate() {
        assertEquals(numTasks,
            getTasks(Query.jobScoped(KEY_A).active()).size());
      }

      @Override void verify(
          Set<ScheduledTask> tasks,
          ParsedConfiguration oldJob,
          ParsedConfiguration updatedJob) {

        verifyUpdate(tasks, oldJob, VERIFY_OLD_TASK);
      }
    };
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
        Query.shardScoped(KEY_A, 0).active()));

    assignTask(taskId, SLAVE_ID, SLAVE_HOST_1, ImmutableSet.of(80, 81, 82));

    AssignedTask task = getTask(taskId).getAssignedTask();
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
        Query.shardScoped(KEY_A, 0).active()));

    assignTask(taskId, SLAVE_ID, SLAVE_HOST_1, ImmutableSet.of(80));

    // The task should be rescheduled.
    changeStatus(taskId, LOST);

    String newTaskId = Tasks.id(getOnlyTask(
        Query.shardScoped(KEY_A, 0).active()));
    assignTask(newTaskId, SLAVE_ID, SLAVE_HOST_1, ImmutableSet.of(86));

    AssignedTask task = getTask(newTaskId).getAssignedTask();
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
    for (TaskEvent event : getTask(taskId).getTaskEvents()) {
      Pair<ScheduleStatus, String> expected = expectedEvents.next();
      assertEquals(expected.getFirst(), event.getStatus());
      assertEquals(expected.getSecond(), event.getMessage());
      assertEquals(hostname, event.getScheduler());
    }
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

  private static ParsedConfiguration makeCronJob(
      JobKey jobKey,
      int numDefaultTasks,
      String cronSchedule) throws TaskDescriptionException {

    ParsedConfiguration job = makeJob(jobKey, numDefaultTasks);
    job.getJobConfig().setCronSchedule(cronSchedule);
    return job;
  }

  private static ParsedConfiguration makeJob(JobKey jobKey, int numDefaultTasks)
      throws TaskDescriptionException  {

    return makeJob(jobKey, productionTask(), numDefaultTasks);
  }

  private static ParsedConfiguration makeJob(JobKey jobKey, TaskConfig task)
      throws TaskDescriptionException {

    return makeJob(jobKey, task, 1);
  }

  private static ParsedConfiguration makeJob(
      JobKey jobKey,
      TaskConfig task,
      int numTasks) throws TaskDescriptionException  {

    JobConfiguration job = new JobConfiguration()
        .setName(jobKey.getName())
        .setOwner(makeIdentity(jobKey))
        .setKey(jobKey)
        .setShardCount(numTasks)
        .setTaskConfig(new TaskConfig(task)
          .setOwner(makeIdentity(jobKey))
          .setEnvironment(jobKey.getEnvironment())
          .setJobName(jobKey.getName()));
    return ParsedConfiguration.fromUnparsed(job);
  }

  private static TaskConfig defaultTask(boolean production) {
    return new TaskConfig()
        .setNumCpus(1)
        .setRamMb(1024)
        .setDiskMb(1024)
        .setProduction(production)
        .setThermosConfig("thermos".getBytes())
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

  private ScheduledTask getTask(String taskId) {
    return getOnlyTask(Query.taskScoped(taskId));
  }

  private ScheduledTask getOnlyTask(Query.Builder query) {
    return Iterables.getOnlyElement(Storage.Util.consistentFetchTasks(storage, query));
  }

  private Set<ScheduledTask> getTasks(Query.Builder query) {
    return Storage.Util.consistentFetchTasks(storage, query);
  }

  private Set<ScheduledTask> getTasksByStatus(ScheduleStatus status) {
    return Storage.Util.consistentFetchTasks(storage, Query.unscoped().byStatus(status));
  }

  private Set<ScheduledTask> getTasksOwnedBy(Identity owner) {
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

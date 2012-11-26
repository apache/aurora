package com.twitter.mesos.scheduler;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import org.apache.mesos.Protos.SlaveID;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.base.Closure;
import com.twitter.common.collections.Pair;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Constraint;
import com.twitter.mesos.gen.CronCollisionPolicy;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.ShardUpdateResult;
import com.twitter.mesos.gen.TaskConstraint;
import com.twitter.mesos.gen.TaskEvent;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.UpdateResult;
import com.twitter.mesos.gen.ValueConstraint;
import com.twitter.mesos.scheduler.CronJobManager.CronScheduler;
import com.twitter.mesos.scheduler.StateManagerVars.MutableState;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import com.twitter.mesos.scheduler.events.PubsubEvent;
import com.twitter.mesos.scheduler.quota.QuotaManager;
import com.twitter.mesos.scheduler.quota.QuotaManager.QuotaManagerImpl;
import com.twitter.mesos.scheduler.quota.Quotas;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;
bra
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static com.twitter.mesos.gen.ScheduleStatus.ASSIGNED;
import static com.twitter.mesos.gen.ScheduleStatus.FAILED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLING;
import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static com.twitter.mesos.gen.ScheduleStatus.STARTING;
import static com.twitter.mesos.gen.ScheduleStatus.UPDATING;
import static com.twitter.mesos.gen.UpdateResult.SUCCESS;
import static com.twitter.mesos.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;
import static com.twitter.mesos.scheduler.configuration.ConfigurationManager.LEGACY_EXECUTOR;
import static com.twitter.mesos.scheduler.configuration.ConfigurationManager.hostLimitConstraint;
import static com.twitter.mesos.scheduler.configuration.ConfigurationManager.populateFields;

/**
 * Base integration test for the SchedulerCoreImpl, subclasses should supply a concrete Storage
 * system.
 */
public abstract class BaseSchedulerCoreImplTest extends EasyMockTest {

  private static final String FRAMEWORK_ID = "FrameworkId";

  private static final Identity OWNER_A = new Identity("Test_Role_A", "Test_User_A");
  private static final String JOB_A = "Test_Job_A";
  private static final String JOB_A_KEY = Tasks.jobKey(OWNER_A, JOB_A);
  private static final int ONE_GB = 1024;
  private static final Quota DEFAULT_TASK_QUOTA = new Quota(1.0, ONE_GB, ONE_GB);
  private static final int DEFAULT_TASKS_IN_QUOTA = 10;

  private static final Identity OWNER_B = new Identity("Test_Role_B", "Test_User_B");
  private static final String JOB_B = "Test_Job_B";

  private static final SlaveID SLAVE_ID = SlaveID.newBuilder().setValue("SlaveId").build();
  private static final String SLAVE_HOST_1 = "SlaveHost1";

  private static final Function<Integer, String> NEW_COMMAND = new Function<Integer, String>() {
    @Override public String apply(Integer shardId) {
      return "updated start command for shard " + shardId;
    }
  };

  private static final Function<Integer, String> OLD_COMMAND = new Function<Integer, String>() {
    @Override public String apply(Integer shardId) {
      return "start command for shard " + shardId;
    }
  };

  private Driver driver;
  private StateManagerImpl stateManager;
  private SchedulerCoreImpl scheduler;
  private CronScheduler cronScheduler;
  private CronJobManager cron;
  private QuotaManager quotaManager;
  private FakeClock clock;
  private Closure<PubsubEvent> eventSink;

  @Before
  public void setUp() throws Exception {
    driver = createMock(Driver.class);
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
   * @return the {@code Storage} to for the SchedulerCoreImpl to use under tests
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

  private void buildScheduler(Storage storage) throws Exception {
    ImmediateJobManager immediateManager = new ImmediateJobManager();
    cron = new CronJobManager(storage, cronScheduler);
    stateManager = new StateManagerImpl(storage, clock, new MutableState(), driver, eventSink);
    quotaManager = new QuotaManagerImpl(storage);
    scheduler = new SchedulerCoreImpl(cron,
        immediateManager,
        stateManager,
        quotaManager);

    cron.schedulerCore = scheduler;
    immediateManager.schedulerCore = scheduler;
    scheduler.prepare();
    scheduler.initialize();
    scheduler.start();

    // Apply a default quota for users so we don't have to give quota for every test.
    quotaManager.setQuota(OWNER_A.getRole(), scale(DEFAULT_TASK_QUOTA, DEFAULT_TASKS_IN_QUOTA));
    quotaManager.setQuota(OWNER_B.getRole(), scale(DEFAULT_TASK_QUOTA, DEFAULT_TASKS_IN_QUOTA));
  }

  @Test(expected = ScheduleException.class)
  public void testCreateJobNoQuota() throws Exception {
    control.replay();
    buildScheduler();

    quotaManager.setQuota(OWNER_A.getRole(), Quotas.NO_QUOTA);
    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
  }

  @Test
  public void testCreateNonproductionJobNoQuota() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo task = nonProductionTask();
    scheduler.createJob(makeJob(OWNER_A, JOB_A, task, 100));
    assertEquals(100, getTasks(Query.byRole(OWNER_A.getRole())).size());
  }

  @Test(expected = ScheduleException.class)
  public void testCreateJobExceedsQuota() throws Exception {
    control.replay();
    buildScheduler();
    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASKS_IN_QUOTA + 1));
  }

  @Test
  public void testCreateJob() throws Exception {
    int numTasks = 10;

    control.replay();
    buildScheduler();

    ParsedConfiguration job = makeJob(OWNER_A, JOB_A, numTasks);
    scheduler.createJob(job);
    assertTaskCount(numTasks);

    Set<ScheduledTask> tasks = scheduler.getTasks(queryJob(OWNER_A, JOB_A));
    assertEquals(numTasks, tasks.size());
    for (ScheduledTask state : tasks) {
      assertEquals(PENDING, state.getStatus());
      assertTrue(state.getAssignedTask().isSetTaskId());
      assertFalse(state.getAssignedTask().isSetSlaveId());
      // Need to clear shard ID since that was assigned in our makeJob function.
      assertEquals(
          populateFields(job.get(), productionTask().setShardId(0)),
          state.getAssignedTask().getTask().setShardId(0));
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

    TwitterTaskInfo newTask = nonProductionTask();
    newTask.addToConstraints(dedicatedConstraint(ImmutableSet.of(OWNER_A.getRole())));
    scheduler.createJob(makeJob(OWNER_A, JOB_A, ImmutableSet.of(newTask)));
    assertEquals(PENDING, getOnlyTask(queryJob(OWNER_A, JOB_A)).getStatus());
  }

  @Test
  public void testDedicatedJobKey() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo newTask = nonProductionTask();
    newTask.addToConstraints(dedicatedConstraint(ImmutableSet.of(Tasks.jobKey(OWNER_A, JOB_A))));
    scheduler.createJob(makeJob(OWNER_A, JOB_A, ImmutableSet.of(newTask)));
    assertEquals(PENDING, getOnlyTask(queryJob(OWNER_A, JOB_A)).getStatus());
  }

  @Test
  public void testLoadTasksFromStorage() throws Exception {
    final String storedTaskId = "task_on_disk";

    control.replay();

    Storage storage = createStorage();

    storage.start(MutateWork.NOOP);

    final TwitterTaskInfo storedTask = new TwitterTaskInfo()
        .setOwner(OWNER_A)
        .setJobName(JOB_A)
        .setNumCpus(1.0)
        .setRamMb(ONE_GB)
        .setDiskMb(500)
        .setShardId(0)
        .setStartCommand("ls")
        .setRequestedPorts(ImmutableSet.<String>of())
        .setConstraints(ImmutableSet.<Constraint>of())
        .setTaskLinks(ImmutableMap.<String, String>of());

    storage.doInWriteTransaction(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getTaskStore().saveTasks(ImmutableSet.of(new ScheduledTask()
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
    TwitterTaskInfo expected = new TwitterTaskInfo(storedTask)
        .setProduction(false)
        .setThermosConfig(new byte[] {})
        .setConstraints(ImmutableSet.of(
            ConfigurationManager.hostLimitConstraint(1),
            LEGACY_EXECUTOR));
    assertEquals(
        expected,
        getTask(storedTaskId).getAssignedTask().getTask());
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

    Storage storage = createStorage();

    storage.start(MutateWork.NOOP);

    final AtomicInteger taskId = new AtomicInteger();

    ParsedConfiguration job = makeJob(OWNER_A, JOB_A, 10);
    final Set<ScheduledTask> badTasks = ImmutableSet.copyOf(Iterables
        .transform(job.get().getTaskConfigs(),
            new Function<TwitterTaskInfo, ScheduledTask>() {
              @Override public ScheduledTask apply(TwitterTaskInfo task) {
                return new ScheduledTask()
                    .setStatus(RUNNING)
                    .setAssignedTask(
                        new AssignedTask()
                            .setTaskId("task-" + taskId.incrementAndGet())
                            .setTask(task.setShardId(0)));
              }
            }));

    storage.doInWriteTransaction(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getTaskStore().saveTasks(badTasks);
      }
    });

    buildScheduler(storage);
    assertEquals(1, getTasksByStatus(RUNNING).size());
    assertEquals(9, getTasksByStatus(KILLED).size());
  }

  @Test
  public void testBackfillRequestedPorts() throws Exception {
    final String storedTaskId = "task_on_disk";

    control.replay();

    Storage storage = createStorage();

    storage.start(MutateWork.NOOP);

    final TwitterTaskInfo storedTask = new TwitterTaskInfo()
        .setOwner(OWNER_A)
        .setJobName(JOB_A)
        .setNumCpus(1.0)
        .setRamMb(ONE_GB)
        .setDiskMb(500)
        .setShardId(0)
        .setStartCommand("ls %port:foo%");

    storage.doInWriteTransaction(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getTaskStore().saveTasks(ImmutableSet.of(new ScheduledTask()
            .setStatus(PENDING)
            .setAssignedTask(
                new AssignedTask()
                    .setTaskId(storedTaskId)
                    .setTask(storedTask))));
      }
    });

    buildScheduler(storage);

    assignTask(storedTaskId, SLAVE_ID, SLAVE_HOST_1, ImmutableSet.of(80, 81));

    assertEquals(
        ImmutableSet.of("foo"),
        getTask(storedTaskId).getAssignedTask().getTask().getRequestedPorts());
  }

  @Test
  public void testBackfillRequestedPortsForCronJob() throws Exception {
    TwitterTaskInfo storedTask = productionTask("start_command", "ls %port:foo%");
    final ParsedConfiguration job = makeJob(OWNER_A, JOB_A, storedTask, 1);
    job.get().setCronSchedule("1 1 1 1 1");
    Iterables.getOnlyElement(job.get().getTaskConfigs()).unsetRequestedPorts();
    expect(cronScheduler.schedule(eq(job.get().getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");

    control.replay();

    Storage storage = createStorage();

    storage.start(MutateWork.NOOP);

    storage.doInWriteTransaction(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getJobStore().saveAcceptedJob(CronJobManager.MANAGER_KEY, job.get());
      }
    });

    buildScheduler(storage);

    assertTaskCount(0);

    scheduler.startCronJob(OWNER_A.getRole(), JOB_A);

    assertEquals(ImmutableSet.of("foo"),
        getOnlyTask(queryJob(OWNER_A, JOB_A)).getAssignedTask().getTask().getRequestedPorts());
  }

  @Test
  public void testCreateJobNoHdfs() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo task = new TwitterTaskInfo().setConfiguration(
        ImmutableMap.<String, String>builder()
            .put("start_command", "date")
            .put("num_cpus", "1.0")
            .put("ram_mb", "1024")
            .put("disk_mb", "1024")
            .build());

    scheduler.createJob(makeJob(OWNER_A, JOB_A, task, 1));
    assertTaskCount(1);

    TwitterTaskInfo task2 = new TwitterTaskInfo().setConfiguration(
        ImmutableMap.<String, String>builder()
            .put("start_command", "date")
            .put("hdfs_path", "")
            .put("num_cpus", "1.0")
            .put("ram_mb", "1024")
            .put("disk_mb", "1024")
            .build());
    scheduler.createJob(makeJob(OWNER_A, JOB_B, task2, 1));
    assertTaskCount(2);
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
    String[] invalidJobs = {"&baz", "/baz", "baz&", ""};

    for (Identity ident : invalidIdentities) {
      for (String job : invalidJobs) {
        expectRejected(ident, job);
      }
    }

    for (String job : invalidJobs) {
      expectRejected(validIdentity, job);
    }

    for (Identity ident : invalidIdentities) {
      expectRejected(ident, validJob);
    }
  }

  private void expectRejected(Identity owner, String jobName) throws ScheduleException {
    try {
      scheduler.createJob(makeJob(owner, jobName, 1));
      fail("Job owner/name should have been rejected.");
    } catch (TaskDescriptionException e) {
      // Expected.
    }
  }

  @Test
  public void testSortableTaskIds() throws Exception {
    control.replay();
    buildScheduler();

    for (ScheduledTask task : getTasks(Query.GET_ALL)) {
      assertEquals(OWNER_A, task.getAssignedTask().getTask().getOwner());
    }
  }

  @Test(expected = ScheduleException.class)
  public void testCreateDuplicateJob() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
    assertTaskCount(1);

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
  }

  @Test(expected = ScheduleException.class)
  public void testCreateDuplicateCronJob() throws Exception {
    ParsedConfiguration job = makeCronJob(OWNER_A, JOB_A, 1, "1 1 1 1 1");
    expect(cronScheduler.schedule(eq(job.get().getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");

    control.replay();
    buildScheduler();

    // Cron jobs are scheduled on a delay, so this job's tasks will not be scheduled immediately,
    // but duplicate jobs should still be rejected.
    scheduler.createJob(job);
    assertTaskCount(0);

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
  }

  @Test
  public void testStartCronJob() throws Exception {
    // Create a cron job, ask the scheduler to start it, and ensure that the tasks exist
    // in the PENDING state.

    ParsedConfiguration job = makeCronJob(OWNER_A, JOB_A, 1, "1 1 1 1 1");
    expect(cronScheduler.schedule(eq(job.get().getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");

    control.replay();
    buildScheduler();

    scheduler.createJob(job);
    assertTaskCount(0);

    scheduler.startCronJob(OWNER_A.getRole(), JOB_A);
    assertEquals(PENDING, getOnlyTask(queryJob(OWNER_A, JOB_A)).getStatus());
  }

  @Test(expected = ScheduleException.class)
  public void testStartNonexistentCronJob() throws Exception {
    // Try to start a cron job that doesn't exist.
    control.replay();
    buildScheduler();

    scheduler.startCronJob(OWNER_A.getRole(), JOB_A);
  }

  @Test
  public void testStartNonCronJob() throws Exception {
    // Create a NON cron job and try to start it as though it were a cron job, and ensure that
    // no cron tasks are created.
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
    String taskId = Tasks.id(getOnlyTask(queryJob(OWNER_A, JOB_A)));

    try {
      scheduler.startCronJob(OWNER_A.getRole(), JOB_A);
      fail("Start should have failed.");
    } catch (ScheduleException e) {
      // Expected.
    }

    assertEquals(PENDING, getTask(taskId).getStatus());
    assertFalse(cron.hasJob(JOB_A_KEY));
  }

  @Test(expected = ScheduleException.class)
  public void testStartNonOwnedCronJob() throws Exception {
    // Try to start a cron job that is not owned by us.
    // Should throw an exception.

    ParsedConfiguration job = makeCronJob(OWNER_A, JOB_A, 1, "1 1 1 1 1");
    expect(cronScheduler.schedule(eq(job.get().getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");

    control.replay();
    buildScheduler();

    scheduler.createJob(job);
    assertTaskCount(0);

    scheduler.startCronJob(OWNER_B.getRole(), JOB_A);
  }

  @Test
  public void testStartRunningCronJob() throws Exception {
    // Start a cron job that is already started by an earlier
    // call and is PENDING. Make sure it follows the cron collision policy.
    ParsedConfiguration job = makeCronJob(OWNER_A, JOB_A, 1, "1 1 1 1 1");
    job.get().setCronCollisionPolicy(CronCollisionPolicy.KILL_EXISTING);
    expect(cronScheduler.schedule(eq(job.get().getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");

    control.replay();
    buildScheduler();

    scheduler.createJob(job);
    assertTaskCount(0);
    assertTrue(cron.hasJob(JOB_A_KEY));

    scheduler.startCronJob(OWNER_A.getRole(), JOB_A);
    assertTaskCount(1);

    String taskId = Tasks.id(getOnlyTask(queryJob(OWNER_A, JOB_A)));

    // Now start the same cron job immediately.
    scheduler.startCronJob(OWNER_A.getRole(), JOB_A);
    assertTaskCount(1);
    assertEquals(PENDING, getOnlyTask(queryJob(OWNER_A, JOB_A)).getStatus());

    // Make sure the pending job is the new one.
    String newTaskId = Tasks.id(getOnlyTask(queryJob(OWNER_A, JOB_A)));
    assertFalse(taskId.equals(newTaskId));
  }

  @Test
  public void testStartRunningOverlapCronJob() throws Exception {
    // Start a cron job that is already started by an earlier
    // call and is PENDING. Make sure it follows the cron collision policy.
    ParsedConfiguration job = makeCronJob(OWNER_A, JOB_A, 1, "1 1 1 1 1");
    job.get().setCronCollisionPolicy(CronCollisionPolicy.RUN_OVERLAP);
    expect(cronScheduler.schedule(eq(job.get().getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");

    control.replay();
    buildScheduler();

    scheduler.createJob(job);
    assertTaskCount(0);
    assertTrue(cron.hasJob(JOB_A_KEY));

    scheduler.startCronJob(OWNER_A.getRole(), JOB_A);
    assertTaskCount(1);

    String taskId = Tasks.id(getOnlyTask(queryJob(OWNER_A, JOB_A)));

    // Now start the same cron job immediately.
    scheduler.startCronJob(OWNER_A.getRole(), JOB_A);

    // Since the task never left PENDING, the second run should have been suppressed.
    assertTaskCount(1);
    assertEquals(PENDING, getTask(taskId).getStatus());

    changeStatus(Query.byId(taskId), ASSIGNED);

    scheduler.startCronJob(OWNER_A.getRole(), JOB_A);
    assertTaskCount(2);
    assertEquals(ASSIGNED, getTask(taskId).getStatus());

    getOnlyTask(Query.byStatus(ScheduleStatus.PENDING));
  }

  @Test
  public void testKillCreateCronJob() throws Exception {
    ParsedConfiguration job = makeCronJob(OWNER_A, JOB_A, 1, "1 1 1 1 1");
    expect(cronScheduler.schedule(eq(job.get().getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");
    cronScheduler.deschedule("key");

    ParsedConfiguration updated = makeCronJob(OWNER_A, JOB_A, 1, "1 2 3 4 5");
    expect(
        cronScheduler.schedule(eq(updated.get().getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key2");

    control.replay();
    buildScheduler();

    scheduler.createJob(job);
    assertTrue(cron.hasJob(JOB_A_KEY));

    scheduler.killTasks(queryJob(OWNER_A, JOB_A), OWNER_A.getUser());
    scheduler.createJob(updated);

    JobConfiguration stored = Iterables.getOnlyElement(cron.getJobs());
    assertEquals(updated.get().getCronSchedule(), stored.getCronSchedule());
  }

  @Test
  public void testKillTask() throws Exception {
    driver.killTask(EasyMock.<String>anyObject());
    // We only expect three kills because the first test does not move out of PENDING.
    expectLastCall().times(3);

    control.replay();
    buildScheduler();

    scheduler.registered(FRAMEWORK_ID);

    for (Set<ScheduleStatus> statuses : ImmutableSet.of(
        ImmutableSet.<ScheduleStatus>of(),
        EnumSet.of(ASSIGNED),
        EnumSet.of(ASSIGNED, STARTING),
        EnumSet.of(ASSIGNED, STARTING, RUNNING))) {

      scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
      String taskId = Tasks.id(getOnlyTask(Query.activeQuery(OWNER_A, JOB_A)));

      for (ScheduleStatus status : statuses) {
        changeStatus(taskId, status);
      }

      scheduler.killTasks(queryByOwner(OWNER_A), OWNER_A.getUser());

      if (!statuses.isEmpty()) {
        // If there was no move out of the PENDING state, the task is deleted outright.
        assertEquals(KILLING, getTask(taskId).getStatus());
      }

      // SImulate a KILLED ack from the executor.
      changeStatus(queryByOwner(OWNER_A), KILLED);
      assertTrue(getTasks(Query.activeQuery(OWNER_A, JOB_A)).isEmpty());
    }
  }

  @Test
  public void testDaemonTasksRescheduled() throws Exception {
    control.replay();
    buildScheduler();

    // Schedule 5 daemon and 5 non-daemon tasks.
    scheduler.createJob(makeJob(OWNER_A, JOB_A, 5));
    TwitterTaskInfo task = productionTask("daemon", "true");
    scheduler.createJob(makeJob(OWNER_A, JOB_A + "daemon", task, 5));

    assertEquals(10, getTasksByStatus(PENDING).size());
    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), STARTING);
    assertEquals(10, getTasksByStatus(STARTING).size());

    changeStatus(queryByOwner(OWNER_A), RUNNING);
    assertEquals(10, getTasksByStatus(RUNNING).size());

    // Daemon tasks will move back into PENDING state after finishing.
    changeStatus(queryByOwner(OWNER_A), FINISHED);
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
  public void testDaemonTaskIgnoresMaxFailures() throws Exception {
    control.replay();
    buildScheduler();

    int maxFailures = 5;
    int totalFailures = 10;

    // Schedule a daemon task.
    TwitterTaskInfo task = productionTask(
        "max_task_failures", String.valueOf(maxFailures),
        "daemon", "true");
    scheduler.createJob(makeJob(OWNER_A, JOB_A, task, 1));
    assertTaskCount(1);

    // Fail the task more than maxFailures.
    for (int i = 1; i <= totalFailures; i++) {
      String taskId = Tasks.id(getOnlyTask(Query.activeQuery(OWNER_A, JOB_A)));

      changeStatus(taskId, ASSIGNED);
      changeStatus(taskId, STARTING);
      changeStatus(taskId, RUNNING);
      assertEquals(i - 1, getTask(taskId).getFailureCount());
      changeStatus(taskId, FAILED);

      assertTaskCount(i + 1);
      ScheduledTask rescheduled = getOnlyTask(Query.byStatus(PENDING));
      assertEquals(i, rescheduled.getFailureCount());
    }

    assertEquals(totalFailures, getTasksByStatus(FAILED).size());
    assertEquals(1, getTasksByStatus(PENDING).size());
  }

  @Test
  public void testTaskRescheduleOnKill() throws Exception {
    control.replay();
    buildScheduler();

    // Create 5 non-daemon and 5 daemon tasks.
    scheduler.createJob(makeJob(OWNER_A, JOB_A, 5));
    TwitterTaskInfo task = productionTask("daemon", "true");
    scheduler.createJob(makeJob(OWNER_A, JOB_A + "daemon", task, 5));

    assertEquals(10, getTasksByStatus(PENDING).size());
    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), STARTING);
    assertEquals(10, getTasksByStatus(STARTING).size());
    changeStatus(queryByOwner(OWNER_A), RUNNING);
    assertEquals(10, getTasksByStatus(RUNNING).size());

    // All tasks will move back into PENDING state after getting KILLED.
    changeStatus(queryByOwner(OWNER_A), KILLED);
    Set<ScheduledTask> newTasks = getTasksByStatus(PENDING);
    assertEquals(10, newTasks.size());
    assertEquals(10, getTasksByStatus(KILLED).size());
  }

  @Test
  public void testNoTransitionFromTerminalState() throws Exception {
    expectKillTask(1);

    control.replay();
    buildScheduler();

    scheduler.registered(FRAMEWORK_ID);
    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), STARTING);
    changeStatus(queryByOwner(OWNER_A), RUNNING);
    scheduler.killTasks(queryByOwner(OWNER_A), OWNER_A.getUser());
    changeStatus(queryByOwner(OWNER_A), KILLED);

    String taskId = Tasks.id(getOnlyTask(queryByOwner(OWNER_A)));

    // This transition should be rejected.
    changeStatus(queryByOwner(OWNER_A), LOST);
    assertEquals(KILLED, getTask(taskId).getStatus());
  }

  @Test
  public void testFailedTaskIncrementsFailureCount() throws Exception {
    int maxFailures = 5;
    control.replay();
    buildScheduler();

    TwitterTaskInfo task = productionTask("max_task_failures", String.valueOf(maxFailures));
    scheduler.createJob(makeJob(OWNER_A, JOB_A, task, 1));
    assertTaskCount(1);

    assertEquals(1, getTasks(queryJob(OWNER_A, JOB_A)).size());

    for (int i = 1; i <= maxFailures; i++) {
      String taskId = Tasks.id(getOnlyTask(Query.activeQuery(OWNER_A, JOB_A)));

      changeStatus(taskId, ASSIGNED);
      changeStatus(taskId, STARTING);
      changeStatus(taskId, RUNNING);
      assertEquals(i - 1, getTask(taskId).getFailureCount());
      changeStatus(taskId, FAILED);

      if (i != maxFailures) {
        assertTaskCount(i + 1);
        ScheduledTask rescheduled = getOnlyTask(Query.byStatus(PENDING));
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
    ParsedConfiguration job = makeCronJob(OWNER_A, JOB_A, 10, "1 1 1 1 1");
    expect(cronScheduler.schedule(eq(job.get().getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");

    control.replay();
    buildScheduler();

    scheduler.createJob(job);
    assertTaskCount(0);
    assertTrue(cron.hasJob(JOB_A_KEY));

    // Simulate a triggering of the cron job.
    scheduler.startCronJob(OWNER_A.getRole(), JOB_A);
    assertTaskCount(10);
    assertEquals(10, getTasks(
        new TaskQuery()
            .setOwner(OWNER_A)
            .setJobName(JOB_A)
            .setStatuses(ImmutableSet.of(PENDING)))
        .size());

    assertTaskCount(10);

    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), STARTING);
    assertTaskCount(10);
    changeStatus(queryByOwner(OWNER_A), RUNNING);
    assertTaskCount(10);
    changeStatus(queryByOwner(OWNER_A), FINISHED);
  }

  @Test
  public void testCronNoSuicide() throws Exception {
    ParsedConfiguration job = makeCronJob(OWNER_A, JOB_A, 10, "1 1 1 1 1");
    job.get().setCronCollisionPolicy(CronCollisionPolicy.KILL_EXISTING);
    expect(cronScheduler.schedule(eq(job.get().getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");

    control.replay();
    buildScheduler();

    scheduler.createJob(job);
    assertTaskCount(0);

    try {
      scheduler.createJob(job);
      fail();
    } catch (ScheduleException e) {
      // Expected.
    }
    assertTrue(cron.hasJob(JOB_A_KEY));

    // Simulate a triggering of the cron job.
    scheduler.startCronJob(OWNER_A.getRole(), JOB_A);
    assertTaskCount(10);

    Set<String> taskIds = Tasks.ids(getTasksOwnedBy(OWNER_A));

    // Simulate a triggering of the cron job.
    scheduler.startCronJob(OWNER_A.getRole(), JOB_A);
    assertTaskCount(10);
    assertTrue(Sets.intersection(taskIds, Tasks.ids(getTasksOwnedBy(OWNER_A))).isEmpty());

    try {
      scheduler.createJob(job);
      fail();
    } catch (ScheduleException e) {
      // Expected.
    }
    assertTrue(cron.hasJob(JOB_A_KEY));
  }

  @Test
  public void testKillPendingTask() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
    assertTaskCount(1);

    Set<ScheduledTask> tasks = scheduler.getTasks(queryJob(OWNER_A, JOB_A));
    assertEquals(1, tasks.size());

    String taskId = Tasks.id(Iterables.get(tasks, 0));

    scheduler.killTasks(Query.byId(taskId), OWNER_A.getUser());
    assertTaskCount(0);
  }

  @Test
  public void testKillRunningTask() throws Exception {
    expectKillTask(1);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
    String taskId = Tasks.id(getOnlyTask(queryByOwner(OWNER_A)));
    changeStatus(taskId, ASSIGNED);
    changeStatus(taskId, STARTING);
    changeStatus(taskId, RUNNING);
    scheduler.killTasks(query(taskId), OWNER_A.getUser());
    assertEquals(KILLING, getTask(taskId).getStatus());
    assertEquals(1, getTasks(queryByOwner(OWNER_A)).size());
    changeStatus(taskId, KILLED);
    assertEquals(KILLED, getTask(taskId).getStatus());
  }

  @Test
  public void testKillCronTask() throws Exception {
    ParsedConfiguration job = makeCronJob(OWNER_A, JOB_A, 1, "1 1 1 1 1");
    job.get().setCronCollisionPolicy(CronCollisionPolicy.KILL_EXISTING);
    expect(cronScheduler.schedule(eq(job.get().getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");
    cronScheduler.deschedule("key");

    control.replay();
    buildScheduler();
    scheduler.createJob(makeCronJob(OWNER_A, JOB_A, 1, "1 1 1 1 1"));

    // This will fail if the cron task could not be found.
    scheduler.killTasks(queryJob(OWNER_A, JOB_A), OWNER_A.getUser());
  }

  @Test
  public void testLostTaskRescheduled() throws Exception {
    expectKillTask(2);

    control.replay();
    buildScheduler();

    int maxFailures = 5;
    TwitterTaskInfo task = productionTask("max_task_failures", String.valueOf(maxFailures));
    scheduler.createJob(makeJob(OWNER_A, JOB_A, task, 1));
    assertTaskCount(1);

    Set<ScheduledTask> tasks = scheduler.getTasks(queryJob(OWNER_A, JOB_A));
    assertEquals(1, tasks.size());

    changeStatus(Query.byStatus(PENDING), ASSIGNED);

    TaskQuery pendingQuery = Query.byStatus(PENDING);
    changeStatus(Query.byStatus(ASSIGNED), LOST);
    assertEquals(PENDING, getOnlyTask(pendingQuery).getStatus());
    assertTaskCount(2);

    changeStatus(Query.byStatus(PENDING), ASSIGNED);
    changeStatus(Query.byStatus(ASSIGNED), LOST);
    assertEquals(PENDING, getOnlyTask(pendingQuery).getStatus());
    assertTaskCount(3);
  }

  @Test
  public void testKillJob() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 10));
    assertTaskCount(10);

    scheduler.killTasks(queryJob(OWNER_A, JOB_A), OWNER_A.getUser());
    assertTaskCount(0);
  }

  @Test
  public void testKillJob2() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 5));
    assertTaskCount(5);

    scheduler.createJob(makeJob(OWNER_A, JOB_A + "2", 5));
    assertTaskCount(10);

    scheduler.killTasks(queryJob(OWNER_A, JOB_A + "2"), OWNER_A.getUser());
    assertTaskCount(5);

    for (ScheduledTask state : scheduler.getTasks(Query.GET_ALL)) {
      assertEquals(JOB_A, state.getAssignedTask().getTask().getJobName());
    }
  }

  @Test
  public void testSlaveDeletesTasks() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 2));

    String taskId1 = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 0)));
    String taskId2 = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 1)));

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
    assertEquals(LOST, getOnlyTask(Query.byId(taskId1)).getStatus());
    assertTrue(getTasks(Query.byId(taskId2)).isEmpty());

    ScheduledTask rescheduled = Iterables.getOnlyElement(getTasksByStatus(PENDING));
    assertEquals(taskId1, rescheduled.getAncestorId());
  }

  @Test
  public void testStartAndFinishUpdate() throws Exception {
    control.replay();
    buildScheduler();

    ParsedConfiguration job = makeJob(OWNER_A, JOB_A, 1);
    scheduler.createJob(job);
    Optional<String> updateToken = scheduler.initiateJobUpdate(job);
    scheduler.finishUpdate(OWNER_A.getRole(), JOB_A, updateToken, SUCCESS);

    // If the finish update succeeded internally, we should be able to start a new update.
    assertTrue(scheduler.initiateJobUpdate(job).isPresent());
  }

  @Test
  public void testUpdateCronJob() throws Exception {
    ParsedConfiguration job = makeCronJob(OWNER_A, JOB_A, 1, "1 1 1 1 1");
    expect(cronScheduler.schedule(eq(job.get().getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");
    cronScheduler.deschedule("key");

    ParsedConfiguration updated = makeCronJob(OWNER_A, JOB_A, 5, "1 2 3 4 5");
    expect(
        cronScheduler.schedule(eq(updated.get().getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key2");

    control.replay();
    buildScheduler();

    scheduler.createJob(job);
    assertFalse(scheduler.initiateJobUpdate(updated).isPresent());
    scheduler.startCronJob(OWNER_A.getRole(), JOB_A);
    assertTaskCount(5);
  }

  @Test(expected = ScheduleException.class)
  public void testInvalidStartUpdate() throws Exception {
    expectKillTask(1);
    control.replay();
    buildScheduler();

    ParsedConfiguration job = makeJob(OWNER_A, JOB_A, 1);
    scheduler.createJob(job);

    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), STARTING);
    changeStatus(queryByOwner(OWNER_A), RUNNING);
    scheduler.initiateJobUpdate(job);
    changeStatus(queryByOwner(OWNER_A), UPDATING);

    scheduler.initiateJobUpdate(job);
  }

  @Test
  public void testFinishUpdateNotFound() throws Exception {
    control.replay();
    buildScheduler();

    try {
      scheduler.finishUpdate("foo", "foo", Optional.of("foo"), SUCCESS);
      fail("Call should have failed.");
    } catch (ScheduleException e) {
      // Expected.
    }

    try {
      scheduler.finishUpdate("foo", "foo", Optional.<String>absent(), SUCCESS);
      fail("Call should have failed.");
    } catch (ScheduleException e) {
      // Expected.
    }
  }

  @Test
  public void testFinishUpdateInvalidToken() throws Exception {
    control.replay();
    buildScheduler();

    ParsedConfiguration job = makeJob(OWNER_A, JOB_A, 1);
    scheduler.createJob(job);
    Optional<String> token = scheduler.initiateJobUpdate(job);

    try {
      scheduler.finishUpdate(OWNER_B.getRole(), JOB_A, Optional.of("foo"), SUCCESS);
      fail("Finish update should have failed.");
    } catch (ScheduleException e) {
      // expected.
    }

    scheduler.finishUpdate(OWNER_A.getRole(), JOB_A, token, SUCCESS);
  }

  @Test
  public void testRejectsSimultaneousUpdates() throws Exception {
    control.replay();
    buildScheduler();

    ParsedConfiguration job = makeJob(OWNER_A, JOB_A, 1);
    scheduler.createJob(job);
    Optional<String> token = scheduler.initiateJobUpdate(job);

    try {
      scheduler.initiateJobUpdate(job);
      fail("Second update should have failed.");
    } catch (ScheduleException e) {
      // expected.
    }

    scheduler.finishUpdate(OWNER_A.getRole(), JOB_A, token, SUCCESS);
  }

  private void verifyUpdate(Set<ScheduledTask> tasks, JobConfiguration job,
      Closure<ScheduledTask> updatedTaskChecker) {
    Map<Integer, ScheduledTask> fetchedShards =
        Maps.uniqueIndex(tasks, Tasks.SCHEDULED_TO_SHARD_ID);
    Map<Integer, TwitterTaskInfo> originalConfigsByShard =
        Maps.uniqueIndex(job.getTaskConfigs(), Tasks.INFO_TO_SHARD_ID);
    assertEquals(originalConfigsByShard.keySet(), fetchedShards.keySet());
    for (ScheduledTask task : tasks) {
      updatedTaskChecker.execute(task);
    }
  }

  // TODO(William Farner): Get rid of this - it's a nightmare to follow.
  private abstract class UpdaterTest {
    UpdaterTest(int numTasks, int additionalTasks) throws Exception {
      control.replay();
      buildScheduler();

      ParsedConfiguration job = makeJob(OWNER_A, JOB_A, productionTask().deepCopy(), numTasks);
      for (TwitterTaskInfo config : job.get().getTaskConfigs()) {
        String command = OLD_COMMAND.apply(config.getShardId());
        config.setStartCommand(command);
        config.putToConfiguration("start_command", command);
      }
      scheduler.createJob(job);

      ParsedConfiguration updatedJob =
          makeJob(OWNER_A, JOB_A, productionTask().deepCopy(), numTasks + additionalTasks);
      for (TwitterTaskInfo config : updatedJob.get().getTaskConfigs()) {
        String command = NEW_COMMAND.apply(config.getShardId());
        config.setStartCommand(command);
        config.putToConfiguration("start_command", command);
      }
      Optional<String> updateToken = scheduler.initiateJobUpdate(updatedJob);

      Set<Integer> jobShards = FluentIterable.from(updatedJob.get().getTaskConfigs())
          .transform(Tasks.INFO_TO_SHARD_ID).toImmutableSet();

      UpdateResult result = performRegisteredUpdate(
          updatedJob.get(),
          updateToken.get(),
          jobShards,
          numTasks,
          additionalTasks);

      scheduler.finishUpdate(OWNER_A.role, JOB_A, updateToken, result);
      postUpdate();
      Set<ScheduledTask> tasks = getTasks(Query.activeQuery(Tasks.jobKey(OWNER_A, JOB_A)));
      verify(tasks, job.get(), updatedJob.get());
      scheduler.initiateJobUpdate(job);
    }

    abstract UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
        Set<Integer> jobShards, int numTasks, int additionalTasks) throws Exception;

    void postUpdate() {
      // Default no-op.
    }

    abstract void verify(Set<ScheduledTask> tasks, JobConfiguration oldJob,
        JobConfiguration updatedJob);
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
        changeStatus(queryByOwner(OWNER_A), ASSIGNED);
        changeStatus(queryByOwner(OWNER_A), RUNNING);

        ImmutableMap.Builder<Integer, ShardUpdateResult> expected = ImmutableMap.builder();
        StateManagerImpl.putResults(expected, ShardUpdateResult.RESTARTING, jobShards);
        assertEquals(
            expected.build(),
            scheduler.updateShards(OWNER_A.role, JOB_A, jobShards, updateToken));
        assertEquals(numTasks, getTasksByStatus(UPDATING).size());

        changeStatus(queryByOwner(OWNER_A), FINISHED);
        changeStatus(Query.byStatus(PENDING), ASSIGNED);
        changeStatus(Query.byStatus(ASSIGNED), RUNNING);

        return SUCCESS;
      }

      @Override void verify(Set<ScheduledTask> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {
        verifyUpdate(tasks, oldJob, new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask state) {
            TwitterTaskInfo task = Tasks.SCHEDULED_TO_INFO.apply(state);
            assertEquals(NEW_COMMAND.apply(task.getShardId()), task.getStartCommand());
          }
        });
      }
    };
  }

  @Test
  public void testAddingShards() throws Exception {
    expectKillTask(1);

    control.replay();
    buildScheduler();

    // Use command line wildcards to detect bugs where command lines with populated wildcards
    // make tasks appear different.
    TwitterTaskInfo task = productionTask("start_command", "%port:foo% %task_id% %shard_id%")
        .setRequestedPorts(ImmutableSet.of("foo"));
    scheduler.createJob(makeJob(OWNER_A, JOB_A, task, 3));
    List<String> taskIds = Ordering.natural().sortedCopy(Tasks.ids(getTasksOwnedBy(OWNER_A)));

    Set<Integer> port = ImmutableSet.of(80);
    assignTask(taskIds.get(0), SLAVE_ID, SLAVE_HOST_1, port);
    assignTask(taskIds.get(1), SLAVE_ID, SLAVE_HOST_1, port);
    assignTask(taskIds.get(2), SLAVE_ID, SLAVE_HOST_1, port);
    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), RUNNING);

    ParsedConfiguration updatedJob = makeJob(OWNER_A, JOB_A, task, 10);
    // Change the start command on shard 1 to ensure that it (and only it) gets restarted as a
    // part of the update.
    Iterables.getOnlyElement(Iterables.filter(updatedJob.get().getTaskConfigs(),
        Predicates.compose(Predicates.equalTo(1), Tasks.INFO_TO_SHARD_ID)))
        .putToConfiguration("start_command", "echo");

    Optional<String> updateToken = scheduler.initiateJobUpdate(updatedJob);
    String role = OWNER_A.getRole();
    String jobKey = Tasks.jobKey(OWNER_A, JOB_A);

    ImmutableMap.Builder<Integer, ShardUpdateResult> expected = ImmutableMap.builder();
    expected.put(1, ShardUpdateResult.RESTARTING);
    StateManagerImpl.putResults(expected, ShardUpdateResult.UNCHANGED, ImmutableSet.of(0, 2));
    assertEquals(
        expected.build(),
        scheduler.updateShards(role, JOB_A, ImmutableSet.of(0, 1, 2), updateToken.get()));
    // Move a few tasks into RUNNING to ensure that the scheduler does not try to kill them
    // in subsequent update batches.
    changeStatus(Query.liveShard(jobKey, 0), FINISHED);
    changeStatus(Query.liveShard(jobKey, 0), ASSIGNED);
    changeStatus(Query.liveShard(jobKey, 0), RUNNING);
    changeStatus(Query.liveShard(jobKey, 1), FINISHED);
    changeStatus(Query.liveShard(jobKey, 1), ASSIGNED);
    changeStatus(Query.liveShard(jobKey, 1), RUNNING);

    expected = ImmutableMap.builder();
    StateManagerImpl.putResults(expected, ShardUpdateResult.ADDED, ImmutableSet.of(3, 4, 5));
    assertEquals(
        expected.build(),
        scheduler.updateShards(role, JOB_A, ImmutableSet.of(3, 4, 5), updateToken.get()));

    expected = ImmutableMap.builder();
    StateManagerImpl.putResults(expected, ShardUpdateResult.ADDED, ImmutableSet.of(6, 7, 8));
    assertEquals(
        expected.build(),
        scheduler.updateShards(role, JOB_A, ImmutableSet.of(6, 7, 8), updateToken.get()));

    expected = ImmutableMap.builder();
    StateManagerImpl.putResults(expected, ShardUpdateResult.ADDED, ImmutableSet.of(9));
    assertEquals(
        expected.build(),
        scheduler.updateShards(role, JOB_A, ImmutableSet.of(9), updateToken.get()));
    scheduler.finishUpdate(role, JOB_A, updateToken, UpdateResult.SUCCESS);

    assertEquals("echo",
        Iterables.getOnlyElement(getTasks(Query.liveShard(jobKey, 1)))
            .getAssignedTask().getTask().getStartCommand());
  }

  @Test
  public void testRollback() throws Exception {
    int numTasks = 4;
    // Kill Tasks called at RUNNING->UPDATING.
    expectKillTask(numTasks);

    new UpdaterTest(numTasks, 0) {
      @Override UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
          Set<Integer> jobShards, int numTasks, int additionalTask) throws Exception {
        changeStatus(queryByOwner(OWNER_A), ASSIGNED);
        changeStatus(queryByOwner(OWNER_A), RUNNING);

        ImmutableMap.Builder<Integer, ShardUpdateResult> expected = ImmutableMap.builder();
        StateManagerImpl.putResults(
            expected,
            ShardUpdateResult.RESTARTING,
            ImmutableSet.of(0, 1, 2, 3));
        assertEquals(
            expected.build(),
            scheduler.updateShards(OWNER_A.role, JOB_A, jobShards, updateToken));
        assertEquals(numTasks, getTasksByStatus(UPDATING).size());

        changeStatus(queryByOwner(OWNER_A), KILLED);

        expected = ImmutableMap.builder();
        StateManagerImpl.putResults(
            expected,
            ShardUpdateResult.RESTARTING,
            ImmutableSet.of(0, 1, 2, 3));
        assertEquals(
            expected.build(),
            scheduler.rollbackShards(OWNER_A.role, JOB_A, jobShards, updateToken));

        changeStatus(Query.byStatus(PENDING), ASSIGNED);
        changeStatus(Query.byStatus(ASSIGNED), RUNNING);

        return UpdateResult.FAILED;
      }

      @Override void verify(Set<ScheduledTask> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {
        verifyUpdate(tasks, oldJob, new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask state) {
            TwitterTaskInfo task = Tasks.SCHEDULED_TO_INFO.apply(state);
            assertEquals(OLD_COMMAND.apply(task.getShardId()), task.getStartCommand());
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
    ParsedConfiguration job = makeJob(OWNER_A, JOB_A, productionTask(), numTasks);
    scheduler.createJob(job);
    List<String> taskIds = Ordering.natural().sortedCopy(Tasks.ids(getTasksOwnedBy(OWNER_A)));

    assignTask(taskIds.get(0), SLAVE_ID, SLAVE_HOST_1);
    assignTask(taskIds.get(1), SLAVE_ID, SLAVE_HOST_1);
    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), RUNNING);

    Optional<String> updateToken = scheduler.initiateJobUpdate(job);
    String role = OWNER_A.getRole();

    assertEquals(
        shardResults(numTasks, ShardUpdateResult.UNCHANGED),
        scheduler.updateShards(role, JOB_A, ImmutableSet.of(0, 1), updateToken.get()));

    assertEquals(
        shardResults(numTasks, ShardUpdateResult.UNCHANGED),
        scheduler.rollbackShards(role, JOB_A, ImmutableSet.of(0, 1), updateToken.get()));

    scheduler.finishUpdate(role, JOB_A, updateToken, UpdateResult.FAILED);
  }

  @Test
  public void testInvalidTransition() throws Exception {
    // Kill Tasks called at RUNNING->UPDATING and UPDATING->RUNNING (Invalid).
    final int numTasks = 4;
    int expectedKillTasks = 8;
    expectKillTask(expectedKillTasks);

    new UpdaterTest(numTasks, 0) {
      @Override UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
          Set<Integer> jobShards, int numTasks, int additionalTasks) throws Exception {
        changeStatus(queryByOwner(OWNER_A), ASSIGNED);
        changeStatus(queryByOwner(OWNER_A), RUNNING);

        assertEquals(
            shardResults(numTasks, ShardUpdateResult.RESTARTING),
            scheduler.updateShards(OWNER_A.role, JOB_A, jobShards, updateToken));
        assertEquals(numTasks, getTasksByStatus(UPDATING).size());

        changeStatus(queryByOwner(OWNER_A), RUNNING);
        changeStatus(queryByOwner(OWNER_A), KILLED);
        changeStatus(Query.byStatus(PENDING), ASSIGNED);
        changeStatus(Query.byStatus(ASSIGNED), RUNNING);

        return SUCCESS;
      }

      @Override void verify(Set<ScheduledTask> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {
        verifyUpdate(tasks, oldJob, new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask state) {
            TwitterTaskInfo task = Tasks.SCHEDULED_TO_INFO.apply(state);
            assertEquals(NEW_COMMAND.apply(task.getShardId()), task.getStartCommand());
          }
        });
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
            scheduler.updateShards(OWNER_A.role, JOB_A, jobShards, updateToken));
        assertEquals(numTasks, getTasksByStatus(PENDING).size());

        return SUCCESS;
      }

      @Override void verify(Set<ScheduledTask> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {
        verifyUpdate(tasks, oldJob, new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask state) {
            TwitterTaskInfo task = Tasks.SCHEDULED_TO_INFO.apply(state);
            assertEquals(NEW_COMMAND.apply(task.getShardId()), task.getStartCommand());
          }
        });
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
        changeStatus(queryByOwner(OWNER_A), ASSIGNED);
        changeStatus(queryByOwner(OWNER_A), RUNNING);

        Map<Integer, ShardUpdateResult> expected =
            ImmutableMap.<Integer, ShardUpdateResult>builder()
                .putAll(shardResults(numTasks, ShardUpdateResult.RESTARTING))
                .putAll(shardResults(2, 3, ShardUpdateResult.ADDED))
                .build();
        assertEquals(
            expected,
            scheduler.updateShards(OWNER_A.role, JOB_A, jobShards, updateToken));
        changeStatus(Query.byStatus(UPDATING), KILLED);

        assertEquals(numTasks + additionalTasks, getTasksByStatus(PENDING).size());

        return SUCCESS;
      }

      @Override void verify(Set<ScheduledTask> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {
        verifyUpdate(tasks, updatedJob, new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask state) {
            TwitterTaskInfo task = Tasks.SCHEDULED_TO_INFO.apply(state);
            assertEquals(NEW_COMMAND.apply(task.getShardId()), task.getStartCommand());
          }
        });
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
        changeStatus(queryByOwner(OWNER_A), ASSIGNED);
        changeStatus(queryByOwner(OWNER_A), RUNNING);

        assertEquals(
            shardResults(2, ShardUpdateResult.RESTARTING),
            scheduler.updateShards(OWNER_A.role, JOB_A, jobShards, updateToken));
        changeStatus(Query.byStatus(UPDATING), FINISHED);

        assertEquals(numTasks + additionalTasks, getTasksByStatus(PENDING).size());
        return SUCCESS;
      }

      @Override void postUpdate() {
        changeStatus(Query.byStatus(KILLING), FINISHED);
      }

      @Override void verify(Set<ScheduledTask> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {

        verifyUpdate(tasks, updatedJob, new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask state) {
            TwitterTaskInfo task = Tasks.SCHEDULED_TO_INFO.apply(state);
            assertEquals(NEW_COMMAND.apply(task.getShardId()), task.getStartCommand());
          }
        });
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
        changeStatus(queryByOwner(OWNER_A), ASSIGNED);
        changeStatus(queryByOwner(OWNER_A), RUNNING);

        Map<Integer, ShardUpdateResult> expected =
            ImmutableMap.<Integer, ShardUpdateResult>builder()
                .putAll(shardResults(numTasks, ShardUpdateResult.RESTARTING))
                .putAll(shardResults(2, 3, ShardUpdateResult.ADDED))
                .build();
        assertEquals(
            expected,
            scheduler.updateShards(OWNER_A.role, JOB_A, jobShards, updateToken));
        changeStatus(Query.byStatus(UPDATING), KILLED);

        assertEquals(numTasks + additionalTasks, getTasksByStatus(PENDING).size());

        assertEquals(
            shardResults(numTasks, ShardUpdateResult.RESTARTING),
            scheduler.rollbackShards(OWNER_A.role, JOB_A, ImmutableSet.of(0, 1), updateToken));

        return UpdateResult.FAILED;
      }

      @Override void postUpdate() {
        changeStatus(Query.byStatus(KILLING), FINISHED);
        assertEquals(numTasks, getTasks(Query.activeQuery(Tasks.jobKey(OWNER_A, JOB_A))).size());
      }

      @Override void verify(Set<ScheduledTask> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {

        verifyUpdate(tasks, oldJob, new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask state) {
            TwitterTaskInfo task = Tasks.SCHEDULED_TO_INFO.apply(state);
            assertEquals(OLD_COMMAND.apply(task.getShardId()), task.getStartCommand());
          }
        });
      }
    };
  }

  @Test(expected = ScheduleException.class)
  public void testIncreaseShardsExceedsQuota() throws Exception {
    int numTasks = DEFAULT_TASKS_IN_QUOTA;
    int additionalTasks = 1;

    control.replay();
    buildScheduler();

    ParsedConfiguration job = makeJob(OWNER_A, JOB_A, productionTask().deepCopy(), numTasks);
    for (TwitterTaskInfo config : job.get().getTaskConfigs()) {
      String command = OLD_COMMAND.apply(config.getShardId());
      config.setStartCommand(command);
      config.putToConfiguration("start_command", command);
    }
    scheduler.createJob(job);

    ParsedConfiguration updatedJob =
        makeJob(OWNER_A, JOB_A, productionTask().deepCopy(), numTasks + additionalTasks);
    for (TwitterTaskInfo config : updatedJob.get().getTaskConfigs()) {
      config.setStartCommand(NEW_COMMAND.apply(config.getShardId()));
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
        changeStatus(queryByOwner(OWNER_A), ASSIGNED);
        changeStatus(queryByOwner(OWNER_A), RUNNING);

        assertEquals(
            shardResults(2, ShardUpdateResult.RESTARTING),
            scheduler.updateShards(OWNER_A.role, JOB_A, jobShards, updateToken));
        changeStatus(Query.byStatus(UPDATING), KILLED);

        assertEquals(numTasks + additionalTasks, getTasksByStatus(PENDING).size());

        assertEquals(
            shardResults(2, ShardUpdateResult.RESTARTING),
            scheduler.rollbackShards(OWNER_A.role, JOB_A, jobShards, updateToken));

        return UpdateResult.FAILED;
      }

      @Override void postUpdate() {
        assertEquals(numTasks, getTasks(Query.activeQuery(Tasks.jobKey(OWNER_A, JOB_A))).size());
      }

      @Override void verify(Set<ScheduledTask> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {
        verifyUpdate(tasks, oldJob, new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask state) {
            TwitterTaskInfo task = Tasks.SCHEDULED_TO_INFO.apply(state);
            assertEquals(OLD_COMMAND.apply(task.getShardId()), task.getStartCommand());
          }
        });
      }
    };
  }

  @Test
  public void testTaskIdExpansion() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo config = productionTask("start_command", "%task_id%");

    scheduler.createJob(makeJob(OWNER_A, JOB_A, config, 1));

    String taskId = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 0)));
    assignTask(taskId, SLAVE_ID, SLAVE_HOST_1);

    AssignedTask task = getTask(taskId).getAssignedTask();
    assertEquals(taskId, task.getTask().getStartCommand());
  }

  @Test
  public void testShardIdExpansion() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo config = productionTask("start_command", "%shard_id%");

    scheduler.createJob(makeJob(OWNER_A, JOB_A, config, 1));

    String taskId = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 0)));
    assignTask(taskId, SLAVE_ID, SLAVE_HOST_1);

    AssignedTask task = getTask(taskId).getAssignedTask();
    assertEquals("0", task.getTask().getStartCommand());
  }

  @Test
  public void testPortResource() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo config = productionTask("start_command", "%port:one% %port:two% %port:three%")
        .setRequestedPorts(ImmutableSet.of("one", "two", "three"));

    scheduler.createJob(makeJob(OWNER_A, JOB_A, config, 1));

    String taskId = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 0)));

    assignTask(taskId, SLAVE_ID, SLAVE_HOST_1, ImmutableSet.of(80, 81, 82));

    AssignedTask task = getTask(taskId).getAssignedTask();
    assertEquals(
        ImmutableSet.of("80", "81", "82"),
        ImmutableSet.copyOf(Splitter.on(" ").split(task.getTask().getStartCommand())));
  }

  @Test
  public void testPortResourceResetAfterReschedule() throws Exception {
    expectKillTask(1);

    control.replay();
    buildScheduler();

    TwitterTaskInfo config = productionTask("start_command", "%port:one%")
        .setRequestedPorts(ImmutableSet.of("one"));

    scheduler.createJob(makeJob(OWNER_A, JOB_A, config, 1));

    String taskId = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 0)));

    assignTask(taskId, SLAVE_ID, SLAVE_HOST_1, ImmutableSet.of(80));

    AssignedTask task = getTask(taskId).getAssignedTask();
    assertEquals("80", task.getTask().getStartCommand());

    // The task should be rescheduled.
    changeStatus(taskId, LOST);

    String newTaskId = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 0)));
    assertEquals("%port:one%", getTask(newTaskId).getAssignedTask().getTask().getStartCommand());

    assignTask(newTaskId, SLAVE_ID, SLAVE_HOST_1, ImmutableSet.of(86));

    task = getTask(newTaskId).getAssignedTask();
    assertEquals("86", task.getTask().getStartCommand());
  }

  @Test
  public void testBadExpansion() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo task = productionTask("start_command", "%port%");
    scheduler.createJob(makeJob(OWNER_A, "foo", task, 1));
  }

  @Test
  public void testAuditMessage() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));

    String taskId = Tasks.id(getOnlyTask(queryByOwner(OWNER_A)));
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
    assertEquals(numTasks, scheduler.getTasks(Query.GET_ALL).size());
  }

  private static ParsedConfiguration makeCronJob(
      Identity owner,
      String jobName,
      int numDefaultTasks,
      String cronSchedule) throws TaskDescriptionException {

    ParsedConfiguration job = makeJob(owner, jobName, numDefaultTasks);
    job.get().setCronSchedule(cronSchedule);
    return job;
  }

  private static ParsedConfiguration makeJob(
      Identity owner,
      String jobName,
      int numDefaultTasks) throws TaskDescriptionException  {
    return makeJob(owner, jobName, productionTask(), numDefaultTasks);
  }

  private static ParsedConfiguration makeJob(
      Identity owner,
      String jobName,
      TwitterTaskInfo task,
      int numTasks) throws TaskDescriptionException  {

    List<TwitterTaskInfo> tasks = Lists.newArrayList();
    for (int i = 0; i < numTasks; i++) {
      tasks.add(new TwitterTaskInfo(task).setOwner(owner).setJobName(jobName));
    }
    return makeJob(owner, jobName, tasks);
  }

  private static ParsedConfiguration makeJob(
      Identity owner,
      String jobName,
      Iterable<TwitterTaskInfo> tasks) throws TaskDescriptionException {

    JobConfiguration job = new JobConfiguration();
    job.setOwner(owner).setName(jobName);
    int i = 0;
    for (TwitterTaskInfo task : tasks) {
      job.addToTaskConfigs(new TwitterTaskInfo(task).setShardId(i++));
    }
    return ParsedConfiguration.fromUnparsed(job);
  }

  private static TwitterTaskInfo defaultTask(boolean production, String... additionalParams) {
    Preconditions.checkArgument((additionalParams.length % 2) == 0,
        "Additional params count must be even.");

    Map<String, String> params = Maps.newHashMap(ImmutableMap.<String, String>builder()
        .put("start_command", "date")
        .put("num_cpus", "1.0")
        .put("ram_mb", "1024")
        .put("disk_mb", "1024")
        .put("hdfs_path", "/fake/path")
        .put("production", Boolean.toString(production))
        .build());

    for (int i = 0; i < additionalParams.length; i += 2) {
      params.put(additionalParams[i], additionalParams[i + 1]);
    }

    TwitterTaskInfo task = new TwitterTaskInfo().setConfiguration(ImmutableMap.copyOf(params));

    // Avoid hitting per-host scheduling constraints.
    task.addToConstraints(hostLimitConstraint(100));
    return task;
  }

  private static TwitterTaskInfo productionTask(String... additionalParams) {
    return defaultTask(true, additionalParams);
  }

  private static TwitterTaskInfo nonProductionTask(String... additionalParams) {
    return defaultTask(false, additionalParams);
  }

  private ScheduledTask getTask(String taskId) {
    return getOnlyTask(query(taskId));
  }

  private ScheduledTask getOnlyTask(TaskQuery query) {
    return Iterables.getOnlyElement(scheduler.getTasks(query));
  }

  private Set<ScheduledTask> getTasks(TaskQuery query) {
    return scheduler.getTasks(query);
  }

  private Set<ScheduledTask> getTasksByStatus(ScheduleStatus status) {
    return scheduler.getTasks(Query.byStatus(status));
  }

  private Set<ScheduledTask> getTasksOwnedBy(Identity owner) {
    return scheduler.getTasks(query(owner, null, null));
  }

  private TaskQuery query(Iterable<String> taskIds) {
    return query(null, null, taskIds);
  }

  private TaskQuery query(String... taskIds) {
    return query(null, null, ImmutableList.copyOf(taskIds));
  }

  private TaskQuery queryByOwner(Identity owner) {
    return query(owner, null, null);
  }

  private TaskQuery queryJob(Identity owner, String jobName) {
    return query(owner, jobName, null);
  }

  private TaskQuery query(
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

    return query;
  }

  public void changeStatus(TaskQuery query, ScheduleStatus status, Optional<String> message) {
    scheduler.setTaskStatus(query, status, message);
  }

  public void changeStatus(TaskQuery query, ScheduleStatus status) {
    changeStatus(query, status, Optional.<String>absent());
  }

  public void changeStatus(String taskId, ScheduleStatus status) {
    changeStatus(taskId, status, Optional.<String>absent());
  }

  public void changeStatus(String taskId, ScheduleStatus status, Optional<String> message) {
    changeStatus(query(Arrays.asList(taskId)), status, message);
  }
}

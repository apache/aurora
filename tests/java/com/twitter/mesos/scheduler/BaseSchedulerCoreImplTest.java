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
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
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
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.testing.TearDownRegistry;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Constraint;
import com.twitter.mesos.gen.CronCollisionPolicy;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.LimitConstraint;
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
import com.twitter.mesos.scheduler.SchedulerCore.RestartException;
import com.twitter.mesos.scheduler.StateManagerVars.MutableState;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import com.twitter.mesos.scheduler.events.TaskPubsubEvent;
import com.twitter.mesos.scheduler.quota.QuotaManager;
import com.twitter.mesos.scheduler.quota.QuotaManager.QuotaManagerImpl;
import com.twitter.mesos.scheduler.quota.Quotas;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;

import static org.easymock.EasyMock.expectLastCall;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static com.twitter.mesos.gen.ScheduleStatus.ASSIGNED;
import static com.twitter.mesos.gen.ScheduleStatus.FAILED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLING;
import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.PREEMPTING;
import static com.twitter.mesos.gen.ScheduleStatus.RESTARTING;
import static com.twitter.mesos.gen.ScheduleStatus.ROLLBACK;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static com.twitter.mesos.gen.ScheduleStatus.STARTING;
import static com.twitter.mesos.gen.ScheduleStatus.UPDATING;
import static com.twitter.mesos.gen.UpdateResult.SUCCESS;
import static com.twitter.mesos.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;
import static com.twitter.mesos.scheduler.configuration.ConfigurationManager.MAX_TASKS_PER_JOB;
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
  private CronJobManager cron;
  private QuotaManager quotaManager;
  private FakeClock clock;
  private Closure<TaskPubsubEvent> eventSink;

  @Before
  public void setUp() throws Exception {
    driver = createMock(Driver.class);
    clock = new FakeClock();
    eventSink = createMock(new Clazz<Closure<TaskPubsubEvent>>() { });
    eventSink.execute(EasyMock.<TaskPubsubEvent>anyObject());
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
    cron = new CronJobManager(storage, new TearDownRegistry(this));
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

  @Test(expected = TaskDescriptionException.class)
  public void testCreateJobNoResources() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo task = productionTask().deepCopy();
    task.getConfiguration().remove("num_cpus");
    task.getConfiguration().remove("ram_mb");
    task.getConfiguration().remove("disk_mb");

    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 1);
    scheduler.createJob(job);
  }

  @Test(expected = TaskDescriptionException.class)
  public void testCreateJobBadCpu() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo task = productionTask().deepCopy();
    task.getConfiguration().put("num_cpus", "0.0");

    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 1);
    scheduler.createJob(job);
  }

  @Test(expected = TaskDescriptionException.class)
  public void testCreateJobBadRam() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo task = productionTask().deepCopy();
    task.getConfiguration().put("ram_mb", "-123");

    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 1);
    scheduler.createJob(job);
  }

  @Test(expected = TaskDescriptionException.class)
  public void testCreateJobBadDisk() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo task = productionTask().deepCopy();
    task.getConfiguration().put("disk_mb", "0");

    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 1);
    scheduler.createJob(job);
  }

  @Test(expected = ScheduleException.class)
  public void testCreateJobNoQuota() throws Exception {
    control.replay();
    buildScheduler();

    quotaManager.setQuota(OWNER_A.getRole(), Quotas.NO_QUOTA);

    JobConfiguration job = makeJob(OWNER_A, JOB_A, 1);
    scheduler.createJob(job);
  }

  @Test
  public void testCreateNonproductionJobNoQuota() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo task = nonProductionTask();
    scheduler.createJob(makeJob(OWNER_A, JOB_A, task, 100));
    assertThat(getTasks(Query.byRole(OWNER_A.getRole())).size(), is(100));
  }

  @Test(expected = ScheduleException.class)
  public void testCreateJobExceedsQuota() throws Exception {
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, DEFAULT_TASKS_IN_QUOTA + 1);
    scheduler.createJob(job);
  }

  @Test(expected = TaskDescriptionException.class)
  public void testCreateJobExceedsTaskLimit() throws Exception {
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, nonProductionTask(),
        MAX_TASKS_PER_JOB.get() + 1);
    scheduler.createJob(job);
  }

  @Test(expected = TaskDescriptionException.class)
  public void testUpdateJobExceedsTaskLimit() throws Exception {
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, nonProductionTask(),
        MAX_TASKS_PER_JOB.get());
    scheduler.createJob(job);

    job = makeJob(OWNER_A, JOB_A, nonProductionTask(),
        MAX_TASKS_PER_JOB.get() + 1);
    scheduler.initiateJobUpdate(job);
  }

  @Test
  public void testCreateJob() throws Exception {
    int numTasks = 10;

    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, numTasks);
    scheduler.createJob(job);
    assertTaskCount(numTasks);

    Set<ScheduledTask> tasks = scheduler.getTasks(queryJob(OWNER_A, JOB_A));
    assertThat(tasks.size(), is(numTasks));
    for (ScheduledTask state : tasks) {
      assertThat(state.getStatus(), is(PENDING));
      assertThat(state.getAssignedTask().isSetTaskId(), is(true));
      assertThat(state.getAssignedTask().isSetSlaveId(), is(false));
      // Need to clear shard ID since that was assigned in our makeJob function.
      assertThat(state.getAssignedTask().getTask().setShardId(0),
          is(populateFields(job,
              productionTask().setShardId(0))));
    }
  }

  private static Constraint dedicatedConstraint(Set<String> values) {
    return new Constraint(DEDICATED_ATTRIBUTE,
        TaskConstraint.value(new ValueConstraint(false, values)));
  }

  private static Constraint dedicatedConstraint(int value) {
    return new Constraint(DEDICATED_ATTRIBUTE, TaskConstraint.limit(new LimitConstraint(value)));
  }

  @Test
  public void testDedicatedJob() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo newTask = nonProductionTask();
    newTask.addToConstraints(dedicatedConstraint(ImmutableSet.of(OWNER_A.getRole())));
    scheduler.createJob(makeJob(OWNER_A, JOB_A, ImmutableSet.of(newTask)));

    assertThat(getOnlyTask(queryJob(OWNER_A, JOB_A)).getStatus(), is(PENDING));
  }

  @Test
  public void testDedicatedJobKey() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo newTask = nonProductionTask();
    newTask.addToConstraints(dedicatedConstraint(ImmutableSet.of(Tasks.jobKey(OWNER_A, JOB_A))));
    scheduler.createJob(makeJob(OWNER_A, JOB_A, ImmutableSet.of(newTask)));

    assertThat(getOnlyTask(queryJob(OWNER_A, JOB_A)).getStatus(), is(PENDING));
  }

  @Test(expected = TaskDescriptionException.class)
  public void testLimitConstraintForDedicatedJob() throws Exception {
    control.replay();
    buildScheduler();
    TwitterTaskInfo task = nonProductionTask();
    task.addToConstraints(dedicatedConstraint(1));
    scheduler.createJob(makeJob(OWNER_A, JOB_A, ImmutableSet.of(task)));
  }

  @Test(expected = TaskDescriptionException.class)
  public void testMultipleValueConstraintForDedicatedJob() throws Exception {
    control.replay();
    buildScheduler();
    TwitterTaskInfo task = nonProductionTask();
    task.addToConstraints(dedicatedConstraint(ImmutableSet.of("mesos", "test")));
    scheduler.createJob(makeJob(OWNER_A, JOB_A, ImmutableSet.of(task)));
  }

  @Test(expected = TaskDescriptionException.class)
  public void testUnauthorizedDedicatedJob() throws Exception {
    control.replay();
    buildScheduler();
    TwitterTaskInfo task = nonProductionTask();
    task.addToConstraints(dedicatedConstraint(ImmutableSet.of("mesos")));
    scheduler.createJob(makeJob(OWNER_A, JOB_A, ImmutableSet.of(task)));
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
        .setAvoidJobs(ImmutableSet.<String>of())
        .setConstraints(ImmutableSet.<Constraint>of());

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
    assertThat(Iterables.getLast(getTask(storedTaskId).getTaskEvents()).getStatus(), is(PENDING));

    assignTask(storedTaskId, SLAVE_ID, SLAVE_HOST_1);

    // Since task fields are backfilled with defaults, the production flag and thermos config
    // should be filled.
    assertThat(getTask(storedTaskId).getAssignedTask().getTask(),
        is(new TwitterTaskInfo(storedTask).setProduction(false).setThermosConfig(new byte[] {})));

    assertThat(getTask(storedTaskId).getStatus(), is(ASSIGNED));
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

    JobConfiguration job = makeJob(OWNER_A, JOB_A, 10);
    final Set<ScheduledTask> badTasks = ImmutableSet.copyOf(Iterables
        .transform(job.getTaskConfigs(),
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

    assertThat(getTasks(Query.byStatus(RUNNING)).size(), is(1));
    assertThat(getTasks(Query.byStatus(KILLED)).size(), is(9));
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
        .setStartCommand("ls %port:foo%")
        .setAvoidJobs(ImmutableSet.<String>of());

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
        .setStartCommand("ls %port:foo%")
        .setAvoidJobs(ImmutableSet.<String>of());

    storage.doInWriteTransaction(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getJobStore().saveAcceptedJob(
            CronJobManager.MANAGER_KEY, makeJob(OWNER_A, JOB_A, storedTask, 1)
            .setCronSchedule("1 1 1 1 1"));
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

    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 1);
    scheduler.createJob(job);
    assertTaskCount(1);

    TwitterTaskInfo task2 = new TwitterTaskInfo().setConfiguration(
        ImmutableMap.<String, String>builder()
            .put("start_command", "date")
            .put("hdfs_path", "")
            .put("num_cpus", "1.0")
            .put("ram_mb", "1024")
            .put("disk_mb", "1024")
            .build());
    JobConfiguration job2 = makeJob(OWNER_A, JOB_B, task2, 1);
    scheduler.createJob(job2);
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
      assertThat(task.getAssignedTask().getTask().getOwner(), is(OWNER_A));
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
    control.replay();
    buildScheduler();

    // Cron jobs are scheduled on a delay, so this job's tasks will not be scheduled immediately,
    // but duplicate jobs should still be rejected.
    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1)
        .setCronSchedule("1 1 1 1 1"));
    assertTaskCount(0);

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
  }

  @Test
  public void testStartCronJob() throws Exception {
    // Create a cron job, ask the scheduler to start it, and ensure that the tasks exist
    // in the PENDING state.
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1)
        .setCronSchedule("1 1 1 1 1"));
    assertTaskCount(0);

    scheduler.startCronJob(OWNER_A.getRole(), JOB_A);
    assertThat(getOnlyTask(queryJob(OWNER_A, JOB_A)).getStatus(), is(PENDING));
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

    assertThat(getTask(taskId).getStatus(), is(PENDING));
    assertThat(cron.hasJob(JOB_A_KEY), is(false));
  }

  @Test(expected = ScheduleException.class)
  public void testStartNonOwnedCronJob() throws Exception {
    // Try to start a cron job that is not owned by us.
    // Should throw an exception.
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1)
        .setCronSchedule("1 1 1 1 1"));
    assertTaskCount(0);

    scheduler.startCronJob(OWNER_B.getRole(), JOB_A);
  }

  @Test
  public void testStartRunningCronJob() throws Exception {
    // Start a cron job that is already started by an earlier
    // call and is PENDING. Make sure it follows the cron collision policy.
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, 1)
        .setCronSchedule("1 1 1 1 1")
        .setCronCollisionPolicy(CronCollisionPolicy.KILL_EXISTING);

    scheduler.createJob(job);
    assertTaskCount(0);
    assertThat(cron.hasJob(JOB_A_KEY), is(true));

    cron.cronTriggered(job);
    assertTaskCount(1);

    String taskId = Tasks.id(getOnlyTask(queryJob(OWNER_A, JOB_A)));

    // Now start the same cron job immediately.
    scheduler.startCronJob(OWNER_A.getRole(), JOB_A);
    assertTaskCount(1);
    assertThat(getOnlyTask(queryJob(OWNER_A, JOB_A)).getStatus(), is(PENDING));

    // Make sure the pending job is the new one.
    String newTaskId = Tasks.id(getOnlyTask(queryJob(OWNER_A, JOB_A)));
    assertThat(taskId.equals(newTaskId), is(false));
  }

  @Test
  public void testStartRunningOverlapCronJob() throws Exception {
    // Start a cron job that is already started by an earlier
    // call and is PENDING. Make sure it follows the cron collision policy.
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, 1)
        .setCronSchedule("1 1 1 1 1")
        .setCronCollisionPolicy(CronCollisionPolicy.RUN_OVERLAP);

    scheduler.createJob(job);
    assertTaskCount(0);
    assertThat(cron.hasJob(JOB_A_KEY), is(true));

    cron.cronTriggered(job);
    assertTaskCount(1);

    String taskId = Tasks.id(getOnlyTask(queryJob(OWNER_A, JOB_A)));

    // Now start the same cron job immediately.
    cron.cronTriggered(job);

    // Since the task never left PENDING, the second run should have been suppressed.
    assertTaskCount(1);
    assertThat(getTask(taskId).getStatus(), is(PENDING));

    changeStatus(Query.byId(taskId), ASSIGNED);

    cron.cronTriggered(job);
    assertTaskCount(2);
    assertThat(getTask(taskId).getStatus(), is(ASSIGNED));

    getOnlyTask(Query.byStatus(ScheduleStatus.PENDING));
  }

  @Test(expected = TaskDescriptionException.class)
  public void testCreateEmptyJob() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(new JobConfiguration().setOwner(OWNER_A).setName(JOB_A));
  }

  @Test(expected = TaskDescriptionException.class)
  public void testRejectsMixedProductionMode() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo nonProduction = nonProductionTask();
    TwitterTaskInfo production = productionTask();
    scheduler.createJob(makeJob(OWNER_A, JOB_A,
        ImmutableList.of(nonProduction, production)));
  }

  @Test(expected = TaskDescriptionException.class)
  public void testCreateJobMissingShardIds() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(new JobConfiguration().setOwner(OWNER_A).setName(JOB_A).setTaskConfigs(
        ImmutableSet.of(productionTask())));
  }

  @Test(expected = TaskDescriptionException.class)
  public void testCreateJobDuplicateShardIds() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(new JobConfiguration().setOwner(OWNER_A).setName(JOB_A).setTaskConfigs(
        ImmutableSet.of(
            productionTask().setShardId(0).setStartCommand("foo"),
            productionTask().setShardId(0).setStartCommand("bar"))));
  }

  @Test(expected = TaskDescriptionException.class)
  public void testCreateJobShardIdHole() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(new JobConfiguration().setOwner(OWNER_A).setName(JOB_A).setTaskConfigs(
        ImmutableSet.of(
            productionTask().setShardId(0),
            productionTask().setShardId(2))));
  }

  @Test
  public void testRestartTask() throws Exception {
    expectKillTask(1);

    control.replay();
    buildScheduler();

    scheduler.registered(FRAMEWORK_ID);
    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), STARTING);
    changeStatus(queryByOwner(OWNER_A), RUNNING);

    String taskId = Tasks.id(getOnlyTask(queryByOwner(OWNER_A)));

    Set<String> restartRequest = ImmutableSet.of(taskId);
    scheduler.restartTasks(restartRequest);

    // Mimick the master notifying the scheduler of a task state change.
    changeStatus(query(restartRequest), KILLED);

    assertThat(getTasks(Query.activeQuery(Tasks.jobKey(OWNER_A, JOB_A))).size(), is(1));

    ScheduledTask restartedTask = getOnlyTask(Query.byStatus(KILLED));
    assertThat(Tasks.id(restartedTask), is(taskId));

    ScheduledTask newTask = getOnlyTask(Query.byStatus(PENDING));
    assertThat(newTask.getAncestorId(), is(taskId));
    assertThat(newTask.getAssignedTask().getTask().getShardId(),
        is(restartedTask.getAssignedTask().getTask().getShardId()));
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
        assertThat(getTask(taskId).getStatus(), is(KILLING));
      }

      // SImulate a KILLED ack from the executor.
      changeStatus(queryByOwner(OWNER_A), KILLED);

      assertThat(getTasks(Query.activeQuery(OWNER_A, JOB_A)).size(), is(0));
    }
  }

  @Test(expected = RestartException.class)
  public void testRestartUnknownTask() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), STARTING);
    changeStatus(queryByOwner(OWNER_A), RUNNING);

    String taskId = Tasks.id(getOnlyTask(queryByOwner(OWNER_A)));

    Set<String> restartRequest = Sets.newHashSet(taskId + 1);
    scheduler.restartTasks(restartRequest);
  }

  @Test(expected = RestartException.class)
  public void testRestartInactiveTask() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), STARTING);
    changeStatus(queryByOwner(OWNER_A), RUNNING);
    changeStatus(queryByOwner(OWNER_A), FINISHED);

    String taskId = Tasks.id(getOnlyTask(queryByOwner(OWNER_A)));
    scheduler.restartTasks(ImmutableSet.of(taskId));
  }

  @Test
  public void testDaemonTasksRescheduled() throws Exception {
    control.replay();
    buildScheduler();

    // Schedule 5 daemon and 5 non-daemon tasks.
    scheduler.createJob(makeJob(OWNER_A, JOB_A, 5));
    TwitterTaskInfo task = productionTask("daemon", "true");
    scheduler.createJob(makeJob(OWNER_A, JOB_A + "daemon", task, 5));

    assertThat(getTasksByStatus(PENDING).size(), is(10));
    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), STARTING);
    assertThat(getTasksByStatus(STARTING).size(), is(10));

    changeStatus(queryByOwner(OWNER_A), RUNNING);
    assertThat(getTasksByStatus(RUNNING).size(), is(10));

    // Daemon tasks will move back into PENDING state after finishing.
    changeStatus(queryByOwner(OWNER_A), FINISHED);
    Set<ScheduledTask> newTasks = getTasksByStatus(PENDING);
    assertThat(newTasks.size(), is(5));
    for (ScheduledTask state : newTasks) {
      assertThat(state.getAssignedTask().getTask().getShardId(),
          is(getTask(state.getAncestorId()).getAssignedTask().getTask().getShardId()));
    }

    assertThat(getTasksByStatus(FINISHED).size(), is(10));
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
      assertThat(getTask(taskId).getFailureCount(), is(i - 1));
      changeStatus(taskId, FAILED);

      assertTaskCount(i + 1);
      ScheduledTask rescheduled = getOnlyTask(Query.byStatus(PENDING));
      assertThat(rescheduled.getFailureCount(), is(i));
    }

    assertThat(getTasksByStatus(FAILED).size(), is(totalFailures));
    assertThat(getTasksByStatus(PENDING).size(), is(1));
  }

  @Test
  public void testTaskRescheduleOnKill() throws Exception {
    control.replay();
    buildScheduler();

    // Create 5 non-daemon and 5 daemon tasks.
    scheduler.createJob(makeJob(OWNER_A, JOB_A, 5));
    TwitterTaskInfo task = productionTask("daemon", "true");
    scheduler.createJob(makeJob(OWNER_A, JOB_A + "daemon", task, 5));

    assertThat(getTasksByStatus(PENDING).size(), is(10));
    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), STARTING);
    assertThat(getTasksByStatus(STARTING).size(), is(10));
    changeStatus(queryByOwner(OWNER_A), RUNNING);
    assertThat(getTasksByStatus(RUNNING).size(), is(10));

    // All tasks will move back into PENDING state after getting KILLED.
    changeStatus(queryByOwner(OWNER_A), KILLED);
    Set<ScheduledTask> newTasks = getTasksByStatus(PENDING);
    assertThat(newTasks.size(), is(10));
    assertThat(getTasksByStatus(KILLED).size(), is(10));
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

    assertThat(getTask(taskId).getStatus(), is(KILLED));
  }

  @Test
  public void testFailedTaskIncrementsFailureCount() throws Exception {
    int maxFailures = 5;
    control.replay();
    buildScheduler();

    TwitterTaskInfo task = productionTask("max_task_failures", String.valueOf(maxFailures));
    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 1);
    scheduler.createJob(job);
    assertTaskCount(1);

    Set<ScheduledTask> tasks = scheduler.getTasks(queryJob(OWNER_A, JOB_A));
    assertThat(tasks.size(), is(1));

    for (int i = 1; i <= maxFailures; i++) {
      String taskId = Tasks.id(getOnlyTask(Query.activeQuery(OWNER_A, JOB_A)));

      changeStatus(taskId, ASSIGNED);
      changeStatus(taskId, STARTING);
      changeStatus(taskId, RUNNING);
      assertThat(getTask(taskId).getFailureCount(), is(i - 1));
      changeStatus(taskId, FAILED);

      if (i != maxFailures) {
        assertTaskCount(i + 1);
        ScheduledTask rescheduled = getOnlyTask(Query.byStatus(PENDING));
        assertThat(rescheduled.getFailureCount(), is(i));
      } else {
        assertTaskCount(maxFailures);
      }
    }

    assertThat(getTasksByStatus(FAILED).size(), is(maxFailures));
    assertThat(getTasksByStatus(PENDING).size(), is(0));
  }

  @Test
  public void testCronJobLifeCycle() throws Exception {
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, 10);
    job.setCronSchedule("1 1 1 1 1");
    scheduler.createJob(job);
    assertTaskCount(0);
    assertThat(cron.hasJob(JOB_A_KEY), is(true));

    // Simulate a triggering of the cron job.
    cron.cronTriggered(job);
    assertTaskCount(10);
    assertThat(getTasks(new TaskQuery()
        .setOwner(OWNER_A).setJobName(JOB_A).setStatuses(Sets.newHashSet(PENDING))).size(),
        is(10));

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
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, 10);
    job.setCronSchedule("1 1 1 1 1")
        .setCronCollisionPolicy(CronCollisionPolicy.KILL_EXISTING);
    scheduler.createJob(job);
    assertTaskCount(0);

    try {
      scheduler.createJob(job);
      fail();
    } catch (ScheduleException e) {
      // Expected.
    }
    assertThat(cron.hasJob(JOB_A_KEY), is(true));

    // Simulate a triggering of the cron job.
    cron.cronTriggered(job);
    assertTaskCount(10);

    Set<String> taskIds = Tasks.ids(getTasksOwnedBy(OWNER_A));

    // Simulate a triggering of the cron job.
    cron.cronTriggered(job);
    assertTaskCount(10);

    Set<String> newTaskIds = Tasks.ids(getTasksOwnedBy(OWNER_A));

    assertThat(Sets.intersection(taskIds, newTaskIds).isEmpty(), is(true));

    try {
      scheduler.createJob(job);
      fail();
    } catch (ScheduleException e) {
      // Expected.
    }
    assertThat(cron.hasJob(JOB_A_KEY), is(true));
  }

  @Test
  public void testKillPendingTask() throws Exception {
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, 1);
    scheduler.createJob(job);
    assertTaskCount(1);

    Set<ScheduledTask> tasks = scheduler.getTasks(queryJob(OWNER_A, JOB_A));
    assertThat(tasks.size(), is(1));

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
    assertThat(getTask(taskId).getStatus(), is(KILLING));
    assertThat(getTasks(queryByOwner(OWNER_A)).size(), is(1));
    changeStatus(taskId, KILLED);
    assertThat(getTask(taskId).getStatus(), is(KILLED));
  }

  @Test
  public void testKillCronTask() throws Exception {
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, 1);
    job.setCronSchedule("1 1 1 1 1");
    scheduler.createJob(job);

    // This will fail if the cron task could not be found.
    scheduler.killTasks(queryJob(OWNER_A, JOB_A), OWNER_A.getUser());
  }

  @Test
  public void testLostTaskRescheduled() throws Exception {
    control.replay();
    buildScheduler();

    int maxFailures = 5;
    TwitterTaskInfo task = productionTask("max_task_failures", String.valueOf(maxFailures));
    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 1);
    scheduler.createJob(job);
    assertTaskCount(1);

    Set<ScheduledTask> tasks = scheduler.getTasks(queryJob(OWNER_A, JOB_A));
    assertThat(tasks.size(), is(1));

    changeStatus(Query.byStatus(PENDING), ASSIGNED);

    TaskQuery pendingQuery = Query.byStatus(PENDING);
    changeStatus(Query.byStatus(ASSIGNED), LOST);
    assertThat(getOnlyTask(pendingQuery).getStatus(), is(PENDING));
    assertTaskCount(2);

    changeStatus(Query.byStatus(PENDING), ASSIGNED);
    changeStatus(Query.byStatus(ASSIGNED), LOST);
    assertThat(getOnlyTask(pendingQuery).getStatus(), is(PENDING));
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
      assertThat(state.getAssignedTask().getTask().getJobName(), is(JOB_A));
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
    assertThat(getOnlyTask(Query.byId(taskId1)).getStatus(), is(LOST));
    assertThat(getTasks(Query.byId(taskId2)).size(), is(0));

    ScheduledTask rescheduled = Iterables.getOnlyElement(getTasksByStatus(PENDING));
    assertThat(rescheduled.getAncestorId(), is(taskId1));
  }

  @Test
  public void testStartAndFinishUpdate() throws Exception {
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, 1);
    scheduler.createJob(job);
    Optional<String> updateToken = scheduler.initiateJobUpdate(job);
    scheduler.finishUpdate(OWNER_A.getRole(), job.getName(), updateToken, SUCCESS);

    // If the finish update succeeded internally, we should be able to start a new update.
    assertTrue(scheduler.initiateJobUpdate(job).isPresent());
  }

  @Test
  public void testUpdateCronJob() throws Exception {
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, 1)
        .setCronSchedule("1 1 1 1 1");
    scheduler.createJob(job);
    JobConfiguration updated = makeJob(OWNER_A, JOB_A, 5)
        .setCronSchedule("1 1 1 1 1");
    assertFalse(scheduler.initiateJobUpdate(updated).isPresent());
    scheduler.startCronJob(OWNER_A.getRole(), JOB_A);
    assertTaskCount(5);
  }

  @Test(expected = ScheduleException.class)
  public void testInvalidStartUpdate() throws Exception {
    expectKillTask(1);
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, 1);
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

    JobConfiguration job = makeJob(OWNER_A, JOB_A, 1);
    scheduler.createJob(job);
    Optional<String> token = scheduler.initiateJobUpdate(job);

    try {
      scheduler.finishUpdate(OWNER_B.getRole(), job.getName(), Optional.of("foo"), SUCCESS);
      fail("Finish update should have failed.");
    } catch (ScheduleException e) {
      // expected.
    }

    scheduler.finishUpdate(OWNER_A.getRole(), job.getName(), token, SUCCESS);
  }

  @Test
  public void testRejectsSimultaneousUpdates() throws Exception {
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, 1);
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
    assertThat(fetchedShards.keySet(), is(originalConfigsByShard.keySet()));
    for (ScheduledTask task : tasks) {
      updatedTaskChecker.execute(task);
    }
  }

  // TODO(William Farner): Get rid of this - it's a nightmare to follow.
  private abstract class UpdaterTest {
    UpdaterTest(int numTasks, int additionalTasks) throws Exception {
      control.replay();
      buildScheduler();

      JobConfiguration job = makeJob(OWNER_A, JOB_A, productionTask().deepCopy(), numTasks);
      for (TwitterTaskInfo config : job.getTaskConfigs()) {
        config.putToConfiguration("start_command", OLD_COMMAND.apply(config.getShardId()));
      }
      scheduler.createJob(job);

      JobConfiguration updatedJob =
          makeJob(OWNER_A, JOB_A, productionTask().deepCopy(), numTasks + additionalTasks);
      for (TwitterTaskInfo config : updatedJob.getTaskConfigs()) {
        config.putToConfiguration("start_command", NEW_COMMAND.apply(config.getShardId()));
      }
      Optional<String> updateToken = scheduler.initiateJobUpdate(updatedJob);

      Set<Integer> jobShards = ImmutableSet.copyOf(Iterables.transform(
          updatedJob.getTaskConfigs(), Tasks.INFO_TO_SHARD_ID));

      UpdateResult result = performRegisteredUpdate(
          updatedJob,
          updateToken.get(),
          jobShards,
          numTasks,
          additionalTasks);

      scheduler.finishUpdate(OWNER_A.role, JOB_A, updateToken, result);
      postUpdate();
      Set<ScheduledTask> tasks = getTasks(Query.activeQuery(Tasks.jobKey(OWNER_A, JOB_A)));
      verify(tasks, job, updatedJob);
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
        assertThat(getTasks(Query.byStatus(UPDATING)).size(), is(numTasks));

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
            assertThat(task.getStartCommand(), is(NEW_COMMAND.apply(task.getShardId())));
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

    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 3);
    scheduler.createJob(job);
    List<String> taskIds = Ordering.natural().sortedCopy(Tasks.ids(getTasksOwnedBy(OWNER_A)));

    Set<Integer> port = ImmutableSet.of(80);
    assignTask(taskIds.get(0), SLAVE_ID, SLAVE_HOST_1, port);
    assignTask(taskIds.get(1), SLAVE_ID, SLAVE_HOST_1, port);
    assignTask(taskIds.get(2), SLAVE_ID, SLAVE_HOST_1, port);
    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), RUNNING);

    JobConfiguration updatedJob = makeJob(OWNER_A, JOB_A, task, 10);
    // Change the start command on shard 1 to ensure that it (and only it) gets restarted as a
    // part of the update.
    Iterables.getOnlyElement(Iterables.filter(updatedJob.getTaskConfigs(),
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
        assertThat(getTasks(Query.byStatus(UPDATING)).size(), is(numTasks));

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
            assertThat(task.getStartCommand(), is(OLD_COMMAND.apply(task.getShardId())));
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
    JobConfiguration job = makeJob(OWNER_A, JOB_A, productionTask(), numTasks);
    scheduler.createJob(job);
    List<String> taskIds = Ordering.natural().sortedCopy(Tasks.ids(getTasksOwnedBy(OWNER_A)));

    assignTask(taskIds.get(0), SLAVE_ID, SLAVE_HOST_1);
    assignTask(taskIds.get(1), SLAVE_ID, SLAVE_HOST_1);
    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), RUNNING);

    JobConfiguration updatedJob = job.deepCopy();

    Optional<String> updateToken = scheduler.initiateJobUpdate(updatedJob);
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
        assertThat(getTasks(Query.byStatus(UPDATING)).size(), is(numTasks));

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
            assertThat(task.getStartCommand(), is(NEW_COMMAND.apply(task.getShardId())));
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
        assertThat(getTasks(Query.byStatus(PENDING)).size(), is(numTasks));

        return SUCCESS;
      }

      @Override void verify(Set<ScheduledTask> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {
        verifyUpdate(tasks, oldJob, new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask state) {
            TwitterTaskInfo task = Tasks.SCHEDULED_TO_INFO.apply(state);
            assertThat(task.getStartCommand(), is(NEW_COMMAND.apply(task.getShardId())));
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

        assertThat(getTasks(Query.byStatus(PENDING)).size(), is(numTasks + additionalTasks));

        return SUCCESS;
      }

      @Override void verify(Set<ScheduledTask> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {
        verifyUpdate(tasks, updatedJob, new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask state) {
            TwitterTaskInfo task = Tasks.SCHEDULED_TO_INFO.apply(state);
            assertThat(task.getStartCommand(), is(NEW_COMMAND.apply(task.getShardId())));
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

        assertThat(getTasks(Query.byStatus(PENDING)).size(), is(numTasks + additionalTasks));
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
            assertThat(task.getStartCommand(), is(NEW_COMMAND.apply(task.getShardId())));
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

        assertThat(getTasks(Query.byStatus(PENDING)).size(), is(numTasks + additionalTasks));

        assertEquals(
            shardResults(numTasks, ShardUpdateResult.RESTARTING),
            scheduler.rollbackShards(OWNER_A.role, JOB_A, ImmutableSet.of(0, 1), updateToken));

        return UpdateResult.FAILED;
      }

      @Override void postUpdate() {
        changeStatus(Query.byStatus(KILLING), FINISHED);
        assertThat(getTasks(Query.activeQuery(Tasks.jobKey(OWNER_A, JOB_A))).size(), is(numTasks));
      }

      @Override void verify(Set<ScheduledTask> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {

        verifyUpdate(tasks, oldJob, new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask state) {
            TwitterTaskInfo task = Tasks.SCHEDULED_TO_INFO.apply(state);
            assertThat(task.getStartCommand(), is(OLD_COMMAND.apply(task.getShardId())));
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

    JobConfiguration job = makeJob(OWNER_A, JOB_A, productionTask().deepCopy(), numTasks);
    for (TwitterTaskInfo config : job.getTaskConfigs()) {
      config.putToConfiguration("start_command", OLD_COMMAND.apply(config.getShardId()));
    }
    scheduler.createJob(job);

    JobConfiguration updatedJob =
        makeJob(OWNER_A, JOB_A, productionTask().deepCopy(), numTasks + additionalTasks);
    for (TwitterTaskInfo config : updatedJob.getTaskConfigs()) {
      config.putToConfiguration("start_command", NEW_COMMAND.apply(config.getShardId()));
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

        assertThat(getTasks(Query.byStatus(PENDING)).size(), is(numTasks + additionalTasks));

        assertEquals(
            shardResults(2, ShardUpdateResult.RESTARTING),
            scheduler.rollbackShards(OWNER_A.role, JOB_A, jobShards, updateToken));

        return UpdateResult.FAILED;
      }

      @Override void postUpdate() {
        assertThat(getTasks(Query.activeQuery(Tasks.jobKey(OWNER_A, JOB_A))).size(), is(numTasks));
      }

      @Override void verify(Set<ScheduledTask> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {
        verifyUpdate(tasks, oldJob, new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask state) {
            TwitterTaskInfo task = Tasks.SCHEDULED_TO_INFO.apply(state);
            assertThat(task.getStartCommand(), is(OLD_COMMAND.apply(task.getShardId())));
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
    assertThat(task.getTask().getStartCommand(), is(taskId));
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
    assertThat(task.getTask().getStartCommand(), is("0"));
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

    assertThat(ImmutableSet.copyOf(Splitter.on(" ").split(task.getTask().getStartCommand())),
        is(ImmutableSet.of("80", "81", "82")));
  }

  @Test
  public void testPortResourceResetAfterReschedule() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo config = productionTask("start_command", "%port:one%")
        .setRequestedPorts(ImmutableSet.of("one"));

    scheduler.createJob(makeJob(OWNER_A, JOB_A, config, 1));

    String taskId = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 0)));

    assignTask(taskId, SLAVE_ID, SLAVE_HOST_1, ImmutableSet.of(80));

    AssignedTask task = getTask(taskId).getAssignedTask();
    assertThat(task.getTask().getStartCommand(), is("80"));

    // The task should be rescheduled.
    changeStatus(taskId, LOST);

    String newTaskId = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 0)));
    assertThat(getTask(newTaskId).getAssignedTask().getTask().getStartCommand(), is("%port:one%"));

    assignTask(newTaskId, SLAVE_ID, SLAVE_HOST_1, ImmutableSet.of(86));

    task = getTask(newTaskId).getAssignedTask();
    assertThat(task.getTask().getStartCommand(), is("86"));
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

  @Test
  public void testStuckInKilling() throws Exception {
    // One kill from transition to KILLING, another when the task is observed stuck.
    expectKillTask(2);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
    String taskId = Tasks.id(getOnlyTask(queryByOwner(OWNER_A)));
    changeStatus(taskId, ASSIGNED);
    changeStatus(taskId, STARTING);
    changeStatus(taskId, KILLING);

    clock.advance(StateManagerImpl.TRANSIENT_TASK_STATE_TIMEOUT.get());
    clock.advance(StateManagerImpl.TRANSIENT_TASK_STATE_TIMEOUT.get());

    stateManager.scanOutstandingTasks();

    assertEquals(1, getTasksByStatus(LOST).size());
  }

  @Test
  public void testStuckInAssigned() throws Exception {
    expectKillTask(1);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
    String taskId = Tasks.id(getOnlyTask(queryByOwner(OWNER_A)));
    changeStatus(taskId, ASSIGNED);

    clock.advance(StateManagerImpl.TRANSIENT_TASK_STATE_TIMEOUT.get());
    clock.advance(StateManagerImpl.TRANSIENT_TASK_STATE_TIMEOUT.get());

    stateManager.scanOutstandingTasks();

    assertTaskCount(2);
    assertEquals(1, getTasksByStatus(PENDING).size());
    assertEquals(1, getTasksByStatus(LOST).size());
  }

  private void checkOutstandingTimer(ScheduleStatus status, long expectedValue) {
    AtomicLong timer = stateManager.maxOutstandingTimes.getIfPresent(status);
    if (timer == null) {
      fail("Timer not present for " + status);
    } else {
      assertEquals(expectedValue, timer.get());
    }
  }

  @Test
  public void testOutstandingTimers() throws Exception {
    expectKillTask(1);

    control.replay();
    buildScheduler();

    stateManager.scanOutstandingTasks();
    checkOutstandingTimer(ASSIGNED, 0);
    checkOutstandingTimer(STARTING, 0);
    checkOutstandingTimer(PREEMPTING, 0);
    checkOutstandingTimer(RESTARTING, 0);
    checkOutstandingTimer(KILLING, 0);
    checkOutstandingTimer(UPDATING, 0);
    checkOutstandingTimer(ROLLBACK, 0);

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
    scheduler.createJob(makeJob(OWNER_B, JOB_A, 1));
    String taskA = Tasks.id(getOnlyTask(queryByOwner(OWNER_A)));
    String taskB = Tasks.id(getOnlyTask(queryByOwner(OWNER_B)));
    changeStatus(taskA, ASSIGNED);

    Amount<Long, Time> tick = Amount.of(10L, Time.SECONDS);
    clock.advance(tick);

    stateManager.scanOutstandingTasks();
    checkOutstandingTimer(ASSIGNED, tick.as(Time.MILLISECONDS));

    clock.advance(tick);
    changeStatus(taskB, ASSIGNED);

    clock.advance(tick);
    stateManager.scanOutstandingTasks();
    checkOutstandingTimer(ASSIGNED, tick.as(Time.MILLISECONDS) * 3);

    changeStatus(taskA, RUNNING);
    changeStatus(taskB, STARTING);
    clock.advance(tick);
    changeStatus(taskA, KILLING);
    clock.advance(tick);

    stateManager.scanOutstandingTasks();
    checkOutstandingTimer(ASSIGNED, 0);
    checkOutstandingTimer(STARTING, tick.as(Time.MILLISECONDS) * 2);
    checkOutstandingTimer(KILLING, tick.as(Time.MILLISECONDS));
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
    assertThat(scheduler.getTasks(Query.GET_ALL).size(), is(numTasks));
  }

  private static JobConfiguration makeJob(Identity owner, String jobName, TwitterTaskInfo task,
      int numTasks) {
    List<TwitterTaskInfo> tasks = Lists.newArrayList();
    for (int i = 0; i < numTasks; i++) {
      tasks.add(new TwitterTaskInfo(task).setOwner(owner).setJobName(jobName));
    }
    return makeJob(owner, jobName, tasks);
  }

  private static JobConfiguration makeJob(Identity owner, String jobName, int numDefaultTasks) {
    return makeJob(owner, jobName, productionTask(), numDefaultTasks);
  }

  private static JobConfiguration makeJob(Identity owner, String jobName,
      Iterable<TwitterTaskInfo> tasks) {
    JobConfiguration job = new JobConfiguration();
    job.setOwner(owner).setName(jobName);
    int i = 0;
    for (TwitterTaskInfo task : tasks) {
      job.addToTaskConfigs(new TwitterTaskInfo(task).setShardId(i++));
    }
    return job;
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

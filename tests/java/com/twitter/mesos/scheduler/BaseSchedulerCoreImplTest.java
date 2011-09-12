package com.twitter.mesos.scheduler;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Resource.Range;
import org.apache.mesos.Protos.Resource.Ranges;
import org.apache.mesos.Protos.Resource.Scalar;
import org.apache.mesos.Protos.Resource.Type;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.SlaveOffer;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.base.Closure;
import com.twitter.common.collections.Pair;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.ExecutorKey;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.CronCollisionPolicy;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.UpdateResult;
import com.twitter.mesos.gen.comm.StateUpdateResponse;
import com.twitter.mesos.gen.comm.TaskStateUpdate;
import com.twitter.mesos.scheduler.SchedulerCore.RestartException;
import com.twitter.mesos.scheduler.SchedulerCore.TwitterTask;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import com.twitter.mesos.scheduler.quota.QuotaManager;
import com.twitter.mesos.scheduler.quota.QuotaManager.QuotaManagerImpl;
import com.twitter.mesos.scheduler.quota.Quotas;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.Work.NoResult;

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
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Base integration test for the SchedulerCoreImpl, subclasses should supply a concrete Storage
 * system.
 *
 * TODO(William Farner): Test all the different cases for setTaskStaus:
 *    - Killed tasks get removed.
 *    - Failed tasks have failed count incremented.
 *    - Tasks above maxTaskFailures have _all_ tasks in the job removed.
 *    - Daemon tasks are rescheduled.
 *
 * TODO(William Farner): Add test cases for when the storage has pre-loaded task data.
 *
 * @author William Farner
 */
public abstract class BaseSchedulerCoreImplTest extends EasyMockTest {

  private static final String FRAMEWORK_ID = "framework_id";

  private static final Identity OWNER_A = new Identity("Test_Role_A", "Test_User_A");
  private static final String JOB_A = "Test_Job_A";
  private static final String JOB_A_KEY = Tasks.jobKey(OWNER_A, JOB_A);
  private static final TwitterTaskInfo DEFAULT_TASK = defaultTask();
  private static final Quota DEFAULT_TASK_QUOTA = new Quota(1.0, 1024, 1024);
  private static final int DEFAULT_TASKS_IN_QUOTA = 10;

  private static final Identity OWNER_B = new Identity("Test_Role_B", "Test_User_B");
  private static final String JOB_B = "Test_Job_B";

  private static final SlaveID SLAVE_ID = SlaveID.newBuilder().setValue("SlaveId").build();
  private static final ExecutorID EXECUTOR_ID =
      ExecutorID.newBuilder().setValue("ExecutorId").build();
  private static final String SLAVE_HOST_1 = "SlaveHost1";
  private static final String SLAVE_HOST_2 = "SlaveHost2";
  private static final ExecutorKey SLAVE_HOST_1_KEY =
      new ExecutorKey(SLAVE_ID, EXECUTOR_ID, SLAVE_HOST_1);
  private static final ExecutorKey SLAVE_HOST_2_KEY =
      new ExecutorKey(SLAVE_ID, EXECUTOR_ID, SLAVE_HOST_2);

  private SchedulingFilter schedulingFilter;
  private Closure<String> killTask;
  private SchedulerCoreImpl scheduler;
  private CronJobManager cron;
  private PulseMonitor<ExecutorKey> executorPulseMonitor;
  private Function<TwitterTaskInfo, TwitterTaskInfo> executorResourceAugmenter;
  private QuotaManager quotaManager;
  private FakeClock clock;

  @Before
  public void setUp() throws Exception {
    schedulingFilter = createMock(SchedulingFilter.class);
    killTask = createMock(new Clazz<Closure<String>>() {});
    executorPulseMonitor = createMock(new Clazz<PulseMonitor<ExecutorKey>>() {});
    executorResourceAugmenter =
        createMock(new Clazz<Function<TwitterTaskInfo, TwitterTaskInfo>>() {});
    clock = new FakeClock();
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
    cron = new CronJobManager(storage);
    StateManager stateManager = new StateManager(storage, clock);
    quotaManager = new QuotaManagerImpl(storage);
    scheduler = new SchedulerCoreImpl(cron, immediateManager, stateManager, schedulingFilter,
        executorPulseMonitor, executorResourceAugmenter, quotaManager);
    cron.schedulerCore = scheduler;
    immediateManager.schedulerCore = scheduler;
    scheduler.initialize();
    scheduler.start(killTask);

    // Apply a default quota for users so we don't have to give quota for every test.
    quotaManager.setQuota(OWNER_A.getRole(),
        scale(DEFAULT_TASK_QUOTA, DEFAULT_TASKS_IN_QUOTA));
    quotaManager.setQuota(OWNER_B.getRole(),
        scale(DEFAULT_TASK_QUOTA, DEFAULT_TASKS_IN_QUOTA));
  }

  @Test(expected = TaskDescriptionException.class)
  public void testCreateJobNoResources() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo task = new TwitterTaskInfo(DEFAULT_TASK);
    task.getConfiguration().remove("num_cpus");
    task.getConfiguration().remove("ram_mb");
    task.getConfiguration().remove("disk_mb");

    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 1);
    scheduler.createJob(job);
  }

  @Test(expected = ScheduleException.class)
  public void testCreateJobNoQuota() throws Exception {
    control.replay();
    buildScheduler();

    quotaManager.setQuota(OWNER_A.getRole(), Quotas.NO_QUOTA);

    JobConfiguration job = makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1);
    scheduler.createJob(job);
  }

  @Test
  public void testCreateNonproductionJobNoQuota() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo task = DEFAULT_TASK.deepCopy();
    task.putToConfiguration("production", "false");
    scheduler.createJob(makeJob(OWNER_A, JOB_A, task, 1000));
    assertThat(getTasks(Query.byRole(OWNER_A.getRole())).size(), is(1000));
  }

  @Test(expected = ScheduleException.class)
  public void testCreateJobExceedsQuota() throws Exception {
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, DEFAULT_TASK, DEFAULT_TASKS_IN_QUOTA + 1);
    scheduler.createJob(job);
  }

  @Test
  public void testCreateJob() throws Exception {
    int numTasks = 10;

    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, DEFAULT_TASK, numTasks);
    scheduler.createJob(job);
    assertTaskCount(numTasks);

    Set<TaskState> tasks = scheduler.getTasks(queryJob(OWNER_A, JOB_A));
    assertThat(tasks.size(), is(numTasks));
    for (TaskState state : tasks) {
      assertThat(state.task.getStatus(), is(PENDING));
      assertThat(state.task.getAssignedTask().isSetTaskId(), is(true));
      assertThat(state.task.getAssignedTask().isSetSlaveId(), is(false));
      // Need to clear shard ID since that was assigned in our makeJob function.
      assertThat(state.task.getAssignedTask().getTask().setShardId(0),
          is(ConfigurationManager.populateFields(job,
              new TwitterTaskInfo(DEFAULT_TASK).setShardId(0))));
    }
  }

  @Test
  public void testLoadTasksFromStorage() throws Exception {
    final String storedTaskId = "task_on_disk";

    expectOffer(true);

    control.replay();

    Storage storage = createStorage();

    storage.start(new NoResult.Quiet() {
      @Override
      protected void execute(Storage.StoreProvider storeProvider) {
      }
    });

    final TwitterTaskInfo storedTask = new TwitterTaskInfo()
        .setOwner(OWNER_A)
        .setJobName(JOB_A)
        .setNumCpus(1.0)
        .setRamMb(1024)
        .setShardId(0)
        .setStartCommand("ls")
        .setAvoidJobs(ImmutableSet.<String>of());

    storage.doInTransaction(new NoResult.Quiet() {
      @Override protected void execute(Storage.StoreProvider storeProvider) {
        storeProvider.getTaskStore().saveTasks(ImmutableSet.of(new ScheduledTask()
            .setStatus(PENDING)
            .setAssignedTask(
                new AssignedTask()
                    .setTaskId(storedTaskId)
                    .setTask(storedTask))));
      }
    });

    buildScheduler(storage);

    SlaveOffer slaveOffer = createSlaveOffer(SLAVE_ID, SLAVE_HOST_1, 4, 4096);
    TwitterTask launchedTask = scheduler.offer(slaveOffer, EXECUTOR_ID);

    // Since task fields are backfilled with defaults, the production flag should be filled.
    assertThat(launchedTask.task.getTask(),
        is(new TwitterTaskInfo(storedTask).setProduction(false)));

    assertThat(getTask(storedTaskId).task.getStatus(), is(ASSIGNED));
  }

  @Test
  public void testCreateJobNoHdfs() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo task = new TwitterTaskInfo().setConfiguration(
        ImmutableMap.<String, String> builder()
            .put("start_command", "date")
            .put("num_cpus", "1.0")
            .put("ram_mb", "1024")
            .put("disk_mb", "1024")
            .build());

    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 1);
    scheduler.createJob(job);
    assertTaskCount(1);

    TwitterTaskInfo task2 = new TwitterTaskInfo().setConfiguration(
        ImmutableMap.<String, String> builder()
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
    String[] invalidJobs = { "&baz", "/baz", "baz&", "" };

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
      scheduler.createJob(makeJob(owner, jobName, DEFAULT_TASK, 1));
      fail("Job owner/name should have been rejected.");
    } catch (TaskDescriptionException e) {
      // Expected.
    }
  }

  @Test
  public void testSortableTaskIds() throws Exception {
    control.replay();
    buildScheduler();

    for (TaskState task : getTasks(Query.GET_ALL)) {
      assertThat(task.task.getAssignedTask().getTask().getOwner(), is(OWNER_A));
    }
  }

  @Test(expected = ScheduleException.class)
  public void testCreateDuplicateJob() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1));
    assertTaskCount(1);

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1));
  }

  @Test(expected = ScheduleException.class)
  public void testCreateDuplicateCronJob() throws Exception {
    control.replay();
    buildScheduler();

    // Cron jobs are scheduled on a delay, so this job's tasks will not be scheduled immediately,
    // but duplicate jobs should still be rejected.
    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1)
        .setCronSchedule("1 1 1 1 1"));
    assertTaskCount(0);

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1));
  }

  @Test
  public void testStartCronJob() throws Exception {
    // Create a cron job, ask the scheduler to start it, and ensure that the tasks exist
    // in the PENDING state.
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1)
        .setCronSchedule("1 1 1 1 1"));
    assertTaskCount(0);

    scheduler.startCronJob(OWNER_A.getRole(), JOB_A);
    assertThat(getOnlyTask(queryJob(OWNER_A, JOB_A)).task.getStatus(), is(PENDING));
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

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1));
    String taskId = Tasks.id(getOnlyTask(queryJob(OWNER_A,JOB_A)).task);

    try {
      scheduler.startCronJob(OWNER_A.getRole(), JOB_A);
      fail("Start should have failed.");
    } catch (ScheduleException e) {
      // Expected.
    }

    assertThat(getTask(taskId).task.getStatus(), is(PENDING));
    assertThat(cron.hasJob(JOB_A_KEY), is(false));
  }

  @Test(expected = ScheduleException.class)
  public void testStartNonOwnedCronJob() throws Exception {
    // Try to start a cron job that is not owned by us.
    // Should throw an exception.
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1)
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

    JobConfiguration job = makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1)
        .setCronSchedule("1 1 1 1 1")
        .setCronCollisionPolicy(CronCollisionPolicy.KILL_EXISTING);

    scheduler.createJob(job);
    assertTaskCount(0);
    assertThat(cron.hasJob(JOB_A_KEY), is(true));

    cron.cronTriggered(job);
    assertTaskCount(1);

    String taskId = Tasks.id(getOnlyTask(queryJob(OWNER_A,JOB_A)).task);

    // Now start the same cron job immediately.
    scheduler.startCronJob(OWNER_A.getRole(), JOB_A);
    assertTaskCount(1);
    assertThat(getOnlyTask(queryJob(OWNER_A, JOB_A)).task.getStatus(), is(PENDING));

    // Make sure the pending job is the new one.
    String newTaskId = Tasks.id(getOnlyTask(queryJob(OWNER_A,JOB_A)).task);
    assertThat(taskId.equals(newTaskId), is(false));
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

    TwitterTaskInfo nonProduction = DEFAULT_TASK.deepCopy();
    nonProduction.putToConfiguration("production", "false");
    TwitterTaskInfo production = DEFAULT_TASK.deepCopy();
    production.putToConfiguration("production", "true");
    scheduler.createJob(makeJob(OWNER_A, JOB_A,
        ImmutableList.of(nonProduction, production)));
  }

  @Test(expected = TaskDescriptionException.class)
  public void testCreateJobMissingShardIds() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(new JobConfiguration().setOwner(OWNER_A).setName(JOB_A).setTaskConfigs(
        ImmutableSet.of(new TwitterTaskInfo(DEFAULT_TASK))));
  }

  @Test(expected = TaskDescriptionException.class)
  public void testCreateJobDuplicateShardIds() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(new JobConfiguration().setOwner(OWNER_A).setName(JOB_A).setTaskConfigs(
        ImmutableSet.of(
            new TwitterTaskInfo(DEFAULT_TASK).setShardId(0).setStartCommand("foo"),
            new TwitterTaskInfo(DEFAULT_TASK).setShardId(0).setStartCommand("bar"))));
  }

  @Test(expected = TaskDescriptionException.class)
  public void testCreateJobShardIdHole() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(new JobConfiguration().setOwner(OWNER_A).setName(JOB_A).setTaskConfigs(
        ImmutableSet.of(
            new TwitterTaskInfo(DEFAULT_TASK).setShardId(0),
            new TwitterTaskInfo(DEFAULT_TASK).setShardId(2))));
  }

  @Test
  public void testHonorsScheduleFilter() throws Exception {
    expectOffer(false);
    expectOffer(false);
    expectOffer(false);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 10));

    assertTaskCount(10);

    SlaveOffer slaveOffer = createSlaveOffer(SLAVE_ID, SLAVE_HOST_1, 4, 4096);

    assertNull(scheduler.offer(slaveOffer, EXECUTOR_ID));
    assertNull(scheduler.offer(slaveOffer, EXECUTOR_ID));
    assertNull(scheduler.offer(slaveOffer, EXECUTOR_ID));

    // No tasks should have moved out of the pending state.
    assertThat(getTasksByStatus(PENDING).size(), is(10));
  }

  @Test
  public void testRestartTask() throws Exception {
    expectKillTask(1);

    control.replay();
    buildScheduler();

    scheduler.registered(FRAMEWORK_ID);
    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1));
    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), STARTING);
    changeStatus(queryByOwner(OWNER_A), RUNNING);

    String taskId = Tasks.id(getOnlyTask(queryByOwner(OWNER_A)).task);

    Set<String> restartRequest = ImmutableSet.of(taskId);
    scheduler.restartTasks(restartRequest);

    // Mimick the master notifying the scheduler of a task state change.
    changeStatus(query(restartRequest), KILLED);

    assertThat(getTasks(Query.activeQuery(Tasks.jobKey(OWNER_A, JOB_A))).size(), is(1));

    TaskState restartedTask = getOnlyTask(Query.byStatus(KILLED));
    assertThat(Tasks.id(restartedTask.task), is(taskId));

    TaskState newTask = getOnlyTask(Query.byStatus(PENDING));
    assertThat(newTask.task.getAncestorId(), is(taskId));
    assertThat(newTask.task.getAssignedTask().getTask().getShardId(),
        is(restartedTask.task.getAssignedTask().getTask().getShardId()));
  }

  @Test
  public void testKillTask() throws Exception {
    killTask.execute((String) anyObject());
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

      scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1));
      String taskId = Tasks.id(getOnlyTask(Query.activeQuery(OWNER_A, JOB_A)).task);

      for (ScheduleStatus status : statuses) {
        changeStatus(taskId, status);
      }

      scheduler.killTasks(queryByOwner(OWNER_A));

      if (!statuses.isEmpty()) {
        // If there was no move out of the PENDING state, the task is deleted outright.
        assertThat(getTask(taskId).task.getStatus(), is(KILLING));
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

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1));
    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), STARTING);
    changeStatus(queryByOwner(OWNER_A), RUNNING);

    String taskId = Tasks.id(getOnlyTask(queryByOwner(OWNER_A)).task);

    Set<String> restartRequest = Sets.newHashSet(taskId + 1);
    scheduler.restartTasks(restartRequest);
  }

  @Test(expected = RestartException.class)
  public void testRestartInactiveTask() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1));
    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), STARTING);
    changeStatus(queryByOwner(OWNER_A), RUNNING);
    changeStatus(queryByOwner(OWNER_A), FINISHED);

    String taskId = Iterables.getOnlyElement(Iterables.transform(
        scheduler.getTasks(queryByOwner(OWNER_A)), TaskState.STATE_TO_ID));
    scheduler.restartTasks(ImmutableSet.of(taskId));
  }

  @Test
  public void testDaemonTasksRescheduled() throws Exception {
    control.replay();
    buildScheduler();

    // Schedule 5 daemon and 5 non-daemon tasks.
    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 5));
    TwitterTaskInfo task = new TwitterTaskInfo(DEFAULT_TASK);
    task.putToConfiguration("daemon", "true");
    scheduler.createJob(makeJob(OWNER_A, JOB_A + "daemon", task, 5));

    assertThat(getTasksByStatus(PENDING).size(), is(10));
    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), STARTING);
    assertThat(getTasksByStatus(STARTING).size(), is(10));

    changeStatus(queryByOwner(OWNER_A), RUNNING);
    assertThat(getTasksByStatus(RUNNING).size(), is(10));

    // Daemon tasks will move back into PENDING state after finishing.
    changeStatus(queryByOwner(OWNER_A), FINISHED);
    Set<TaskState> newTasks = getTasksByStatus(PENDING);
    assertThat(newTasks.size(), is(5));
    for (TaskState state : newTasks) {
      assertThat(state.task.getAssignedTask().getTask().getShardId(),
          is(getTask(state.task.getAncestorId()).task.getAssignedTask().getTask().getShardId()));
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
    TwitterTaskInfo task = new TwitterTaskInfo(DEFAULT_TASK);
    task.putToConfiguration("max_task_failures", String.valueOf(maxFailures));
    task.putToConfiguration("daemon", "true");
    scheduler.createJob(makeJob(OWNER_A, JOB_A, task, 1));
    assertTaskCount(1);

    // Fail the task more than maxFailures.
    for (int i = 1; i <= totalFailures; i++) {
      String taskId = TaskState.id(getOnlyTask(Query.activeQuery(OWNER_A, JOB_A)));

      changeStatus(taskId, ASSIGNED);
      changeStatus(taskId, STARTING);
      changeStatus(taskId, RUNNING);
      assertThat(getTask(taskId).task.getFailureCount(), is(i - 1));
      changeStatus(taskId, FAILED);

      assertTaskCount(i + 1);
      TaskState rescheduled = getOnlyTask(Query.byStatus(PENDING));
      assertThat(rescheduled.task.getFailureCount(), is(i));
    }

    assertThat(getTasksByStatus(FAILED).size(), is(totalFailures));
    assertThat(getTasksByStatus(PENDING).size(), is(1));
  }

  @Test
  public void testTaskRescheduleOnKill() throws Exception {
    control.replay();
    buildScheduler();

    // Create 5 non-daemon and 5 daemon tasks.
    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 5));
    TwitterTaskInfo task = new TwitterTaskInfo(DEFAULT_TASK);
    task.putToConfiguration("daemon", "true");
    scheduler.createJob(makeJob(OWNER_A, JOB_A + "daemon", task, 5));

    assertThat(getTasksByStatus(PENDING).size(), is(10));
    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), STARTING);
    assertThat(getTasksByStatus(STARTING).size(), is(10));
    changeStatus(queryByOwner(OWNER_A), RUNNING);
    assertThat(getTasksByStatus(RUNNING).size(), is(10));

    // All tasks will move back into PENDING state after getting KILLED.
    changeStatus(queryByOwner(OWNER_A), KILLED);
    Set<TaskState> newTasks = getTasksByStatus(PENDING);
    assertThat(newTasks.size(), is(10));
    assertThat(getTasksByStatus(KILLED).size(), is(10));
  }

  @Test
  public void testNoTransitionFromTerminalState() throws Exception {
    expectKillTask(1);

    control.replay();
    buildScheduler();

    scheduler.registered(FRAMEWORK_ID);
    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1));
    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), STARTING);
    changeStatus(queryByOwner(OWNER_A), RUNNING);
    scheduler.killTasks(queryByOwner(OWNER_A));
    changeStatus(queryByOwner(OWNER_A), KILLED);

    String taskId = Tasks.id(getOnlyTask(queryByOwner(OWNER_A)).task);

    // This transition should be rejected.
    changeStatus(queryByOwner(OWNER_A), LOST);

    assertThat(getTask(taskId).task.getStatus(), is(KILLED));
  }

  @Test
  public void testFailedTaskIncrementsFailureCount() throws Exception {
    int maxFailures = 5;
    control.replay();
    buildScheduler();

    TwitterTaskInfo task = new TwitterTaskInfo(DEFAULT_TASK);
    task.putToConfiguration("max_task_failures", String.valueOf(maxFailures));
    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 1);
    scheduler.createJob(job);
    assertTaskCount(1);

    Set<TaskState> tasks = scheduler.getTasks(queryJob(OWNER_A, JOB_A));
    assertThat(tasks.size(), is(1));

    for (int i = 1; i <= maxFailures; i++) {
      String taskId = TaskState.id(getOnlyTask(Query.activeQuery(OWNER_A, JOB_A)));

      changeStatus(taskId, ASSIGNED);
      changeStatus(taskId, STARTING);
      changeStatus(taskId, RUNNING);
      assertThat(getTask(taskId).task.getFailureCount(), is(i - 1));
      changeStatus(taskId, FAILED);

      if (i != maxFailures) {
        assertTaskCount(i + 1);
        TaskState rescheduled = getOnlyTask(Query.byStatus(PENDING));
        assertThat(rescheduled.task.getFailureCount(), is(i));
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

    JobConfiguration job = makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 10);
    job.setCronSchedule("1 1 1 1 1");
    scheduler.createJob(job);
    assertTaskCount(0);
    assertThat(cron.hasJob(JOB_A_KEY), is(true));

    // Simulate a triggering of the cron job.
    cron.cronTriggered(job);
    assertTaskCount(10);
    assertThat(getTasks(new Query(new TaskQuery()
        .setOwner(OWNER_A).setJobName(JOB_A).setStatuses(Sets.newHashSet(PENDING)))).size(),
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

    JobConfiguration job = makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 10);
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

    Set<String> taskIds = ImmutableSet.copyOf(Iterables.transform(getTasksOwnedBy(OWNER_A),
        TaskState.STATE_TO_ID));

    // Simulate a triggering of the cron job.
    cron.cronTriggered(job);
    assertTaskCount(10);

    Set<String> newTaskIds = ImmutableSet.copyOf(Iterables.transform(getTasksOwnedBy(OWNER_A),
        TaskState.STATE_TO_ID));

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

    JobConfiguration job = makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1);
    scheduler.createJob(job);
    assertTaskCount(1);

    Set<TaskState> tasks = scheduler.getTasks(queryJob(OWNER_A, JOB_A));
    assertThat(tasks.size(), is(1));

    String taskId = Tasks.id(Iterables.get(tasks, 0).task);

    scheduler.killTasks(Query.byId(taskId));
    assertTaskCount(0);
  }

  @Test
  public void testKillRunningTask() throws Exception {
    expectKillTask(1);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1));
    String taskId = Tasks.id(getOnlyTask(queryByOwner(OWNER_A)).task);
    changeStatus(taskId, ASSIGNED);
    changeStatus(taskId, STARTING);
    changeStatus(taskId, RUNNING);
    scheduler.killTasks(query(taskId));
    assertThat(getTask(taskId).task.getStatus(), is(KILLING));
    assertThat(getTasks(queryByOwner(OWNER_A)).size(), is(1));
    changeStatus(taskId, KILLED);
    assertThat(getTask(taskId).task.getStatus(), is(KILLED));
  }

  @Test
  public void testKillCronTask() throws Exception {
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1);
    job.setCronSchedule("1 1 1 1 1");
    scheduler.createJob(job);

    // This will fail if the cron task could not be found.
    scheduler.killTasks(queryJob(OWNER_A, JOB_A));
  }

  @Test
  public void testLostTaskRescheduled() throws Exception {
    control.replay();
    buildScheduler();

    int maxFailures = 5;
    TwitterTaskInfo task = new TwitterTaskInfo(DEFAULT_TASK);
    task.putToConfiguration("max_task_failures", String.valueOf(maxFailures));
    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 1);
    scheduler.createJob(job);
    assertTaskCount(1);

    Set<TaskState> tasks = scheduler.getTasks(queryJob(OWNER_A, JOB_A));
    assertThat(tasks.size(), is(1));

    changeStatus(Query.byStatus(PENDING), ASSIGNED);

    Query pendingQuery = Query.byStatus(PENDING);
    changeStatus(Query.byStatus(ASSIGNED), LOST);
    assertThat(getOnlyTask(pendingQuery).task.getStatus(), is(PENDING));
    assertTaskCount(2);

    changeStatus(Query.byStatus(PENDING), ASSIGNED);
    changeStatus(Query.byStatus(ASSIGNED), LOST);
    assertThat(getOnlyTask(pendingQuery).task.getStatus(), is(PENDING));
    assertTaskCount(3);
  }

  @Test
  public void testKillJob() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 10));
    assertTaskCount(10);

    scheduler.killTasks(queryJob(OWNER_A, JOB_A));
    assertTaskCount(0);
  }

  @Test
  public void testKillJob2() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 5));
    assertTaskCount(5);

    scheduler.createJob(makeJob(OWNER_A, JOB_A + "2", DEFAULT_TASK, 5));
    assertTaskCount(10);

    scheduler.killTasks(queryJob(OWNER_A, JOB_A + "2"));
    assertTaskCount(5);

    for (TaskState state : scheduler.getTasks(Query.GET_ALL)) {
      assertThat(state.task.getAssignedTask().getTask().getJobName(), is(JOB_A));
    }
  }

  private void sendOffer(SlaveOffer offer, String taskId, SlaveID slave, String slaveHost)
      throws Exception {
    sendOffer(offer, taskId, slave, slaveHost, ImmutableSet.<String>of(),
        ImmutableSet.<Integer>of());
  }

  private void sendOffer(SlaveOffer offer, String taskId, SlaveID slave, String slaveHost,
      Set<String> portNames, Set<Integer> ports) throws Exception {
    AssignedTask task = getTask(taskId).task.getAssignedTask().deepCopy();

    List<Resource> resources = ImmutableList.of(
        Resources.makeMesosResource(Resources.CPUS, task.getTask().getNumCpus()),
        Resources.makeMesosResource(Resources.RAM_MB, task.getTask().getRamMb()),
        Resources.makeMesosRangeResource(Resources.PORTS, ports)
    );

    TwitterTask assigned = scheduler.offer(offer, EXECUTOR_ID);

    assertThat(assigned, is(SchedulerCoreImpl.makeTwitterTask(
        getTask(taskId).task.getAssignedTask(), slave.getValue(), resources)));
    assertThat(assigned.task.getSlaveHost(), is(slaveHost));
    Map<String, Integer> assignedPorts = assigned.task.getAssignedPorts();
    assertThat(assignedPorts.keySet(), is(portNames));
    assertThat(ImmutableSet.copyOf(assignedPorts.values()), is(ports));
  }

  @Test
  public void testExecutorBootstrap() throws Exception {
    expect(executorPulseMonitor.isAlive(SLAVE_HOST_1_KEY)).andReturn(false);

    SlaveOffer offer = createSlaveOffer(SLAVE_ID, SLAVE_HOST_1, 3, 4096);
    final Resources offerResources = Resources.from(offer);

    final TwitterTaskInfo augmented = new TwitterTaskInfo().setNumCpus(3.14).setRamMb(1137);
    expect(executorResourceAugmenter.apply(EasyMock.<TwitterTaskInfo>notNull()))
        .andReturn(augmented);

    final Predicate<TwitterTaskInfo> staticFilter =
        createMock(new Clazz<Predicate<TwitterTaskInfo>>() {});
    expect(staticFilter.apply(augmented)).andReturn(false);
    expect(schedulingFilter.staticFilter(offerResources, SLAVE_HOST_1)).andReturn(staticFilter);

    control.replay();

    buildScheduler();
    assertNull(scheduler.offer(offer, EXECUTOR_ID));
  }

  @Test
  public void testSlaveAdjustsSchedulerTaskState() throws Exception {
    expectOffer(true);
    expectOffer(true);
    executorPulseMonitor.pulse(SLAVE_HOST_1_KEY);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 2));

    String taskId1 = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 0)).task);
    String taskId2 = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 1)).task);

    SlaveOffer slaveOffer = createSlaveOffer(SLAVE_ID, SLAVE_HOST_1, 4, 4096);
    sendOffer(slaveOffer, taskId1, SLAVE_ID, SLAVE_HOST_1);
    sendOffer(slaveOffer, taskId2, SLAVE_ID, SLAVE_HOST_1);

    changeStatus(taskId1, STARTING);
    changeStatus(taskId1, RUNNING);
    changeStatus(taskId2, STARTING);
    changeStatus(taskId2, RUNNING);

    // Simulate state update from the executor telling the scheduler that the task is dead.
    // This can happen if the entire cluster goes down - the scheduler has persisted state
    // listing the task as running, and the executor reads the task state in and marks it as KILLED.
    scheduler.stateUpdate(SLAVE_HOST_1_KEY,
        new StateUpdateResponse().setIncrementalUpdate(false)
            .setExecutorUUID("foo")
            .setState(ImmutableMap.of(
                taskId1, new TaskStateUpdate().setStatus(ScheduleStatus.LOST),
                taskId2, new TaskStateUpdate().setStatus(ScheduleStatus.FINISHED))));

    // The expected outcome is that one task is rescheduled, and the old task is moved into the
    // LOST state. The FINISHED task's state is updated on the scheduler.
    assertTaskCount(3);
    assertThat(getOnlyTask(Query.byId(taskId1)).task.getStatus(), is(LOST));
    assertThat(getOnlyTask(Query.byId(taskId2)).task.getStatus(), is(FINISHED));

    TaskState rescheduled = Iterables.getOnlyElement(getTasksByStatus(PENDING));
    assertThat(rescheduled.task.getAncestorId(), is(taskId1));
  }

  @Test
  public void testSlaveCannotModifyTasksForOtherSlave() throws Exception {
    expectOffer(true);
    expectOffer(true);
    executorPulseMonitor.pulse(SLAVE_HOST_2_KEY);

    control.replay();
    buildScheduler();

    SlaveOffer slaveOffer1 = createSlaveOffer(SLAVE_ID, SLAVE_HOST_1, 4, 4096);
    SlaveOffer slaveOffer2 = createSlaveOffer(SLAVE_ID, SLAVE_HOST_2, 4, 4096);

    // Offer resources for the scheduler to accept.
    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1));
    String taskIdA = Tasks.id(Iterables.get(getTasksOwnedBy(OWNER_A), 0).task);
    sendOffer(slaveOffer1, taskIdA, SLAVE_ID, SLAVE_HOST_1);

    scheduler.createJob(makeJob(OWNER_B, JOB_B, DEFAULT_TASK, 1));
    String taskIdB = Tasks.id(Iterables.get(getTasksOwnedBy(OWNER_B), 0).task);
    sendOffer(slaveOffer2, taskIdB, SLAVE_ID, SLAVE_HOST_2);

    changeStatus(taskIdA, STARTING);
    changeStatus(taskIdA, RUNNING);
    changeStatus(taskIdB, STARTING);
    changeStatus(taskIdB, RUNNING);

    assertThat(getTask(taskIdA).task.getAssignedTask().getSlaveHost(), is(SLAVE_HOST_1));

    scheduler.stateUpdate(SLAVE_HOST_2_KEY,
        new StateUpdateResponse().setIncrementalUpdate(false)
            .setExecutorUUID("foo")
            .setState(ImmutableMap.of(
                taskIdA, new TaskStateUpdate().setStatus(FAILED),
                taskIdB, new TaskStateUpdate().setStatus(RUNNING))));

    assertThat(getTasksByStatus(RUNNING).size(), is(2));
    assertTaskCount(2);
  }

  @Test
  public void testSlaveStopsReportingRunningTask() throws Exception {
    expectOffer(true);
    expectOffer(true);
    expectOffer(true);
    expectOffer(true);
    executorPulseMonitor.pulse(SLAVE_HOST_1_KEY);
    executorPulseMonitor.pulse(SLAVE_HOST_2_KEY);

    control.replay();
    buildScheduler();

    SlaveOffer slaveOffer1 = createSlaveOffer(SLAVE_ID, SLAVE_HOST_1, 4, 4096);
    SlaveOffer slaveOffer2 = createSlaveOffer(SLAVE_ID, SLAVE_HOST_2, 4, 4096);

    // Offer resources for the scheduler to accept.
    TwitterTaskInfo daemonTask = new TwitterTaskInfo(DEFAULT_TASK);
    daemonTask.putToConfiguration("daemon", "true");

    scheduler.createJob(makeJob(OWNER_A, JOB_A, daemonTask, 2));
    String taskIdA = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 0)).task);
    String taskIdB = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 1)).task);
    sendOffer(slaveOffer1, taskIdA, SLAVE_ID, SLAVE_HOST_1);
    sendOffer(slaveOffer1, taskIdB, SLAVE_ID, SLAVE_HOST_1);

    scheduler.createJob(makeJob(OWNER_B, JOB_B, DEFAULT_TASK, 2));
    String taskIdC = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_B, JOB_B), 0)).task);
    String taskIdD = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_B, JOB_B), 1)).task);
    sendOffer(slaveOffer2, taskIdC, SLAVE_ID, SLAVE_HOST_2);
    sendOffer(slaveOffer2, taskIdD, SLAVE_ID, SLAVE_HOST_2);

    changeStatus(taskIdA, STARTING);
    changeStatus(taskIdA, RUNNING);
    changeStatus(taskIdB, STARTING);
    changeStatus(taskIdB, FINISHED);
    assertThat(getTasks(new Query(new TaskQuery().setOwner(OWNER_A).setJobName(JOB_A)
            .setStatuses(EnumSet.of(PENDING)))).size(),
        is(1));

    changeStatus(taskIdC, STARTING);
    changeStatus(taskIdC, RUNNING);
    changeStatus(taskIdD, FAILED);

    Function<TaskState, String> getAncestorId = new Function<TaskState, String>() {
      @Override public String apply(TaskState state) { return state.task.getAncestorId(); }
    };

    // Since job A is a daemon, its missing RUNNING task should be rescheduled.
    scheduler.stateUpdate(SLAVE_HOST_1_KEY,
        new StateUpdateResponse().setIncrementalUpdate(false)
            .setExecutorUUID("foo")
            .setState(ImmutableMap.<String, TaskStateUpdate>of()));
    Set<TaskState> rescheduledTasks = getTasks(new Query(new TaskQuery()
        .setOwner(OWNER_A).setJobName(JOB_A).setStatuses(EnumSet.of(PENDING))));
    assertThat(rescheduledTasks.size(), is(2));
    Set<String> rescheduledTaskAncestors = Sets.newHashSet(Iterables.transform(rescheduledTasks,
        getAncestorId));
    assertThat(rescheduledTaskAncestors, is((Set<String>) Sets.newHashSet(taskIdA, taskIdB)));

    // Send an update from host 2 that does not include the FAILED task.
    scheduler.stateUpdate(SLAVE_HOST_2_KEY,
        new StateUpdateResponse().setIncrementalUpdate(false)
            .setExecutorUUID("foo")
            .setState(ImmutableMap.<String, TaskStateUpdate>of()));
    rescheduledTasks = getTasks(new Query(new TaskQuery()
        .setOwner(OWNER_B).setJobName(JOB_B).setStatuses(EnumSet.of(PENDING))));
    assertThat(rescheduledTasks.size(), is(1));
    rescheduledTaskAncestors = Sets.newHashSet(Iterables.transform(rescheduledTasks,
        getAncestorId));
    assertThat(rescheduledTaskAncestors, is((Set<String>) Sets.newHashSet(taskIdC)));
    assertThat(Iterables.isEmpty(getTasks(taskIdD)), is(true));
  }

  @Test
  public void testIncrementalStateUpdates() throws Exception {
    expectOffer(true);
    expectOffer(true);
    expectKillTask(1);  // Rogue task gets killed.
    executorPulseMonitor.pulse(SLAVE_HOST_1_KEY);
    executorPulseMonitor.pulse(SLAVE_HOST_2_KEY);
    expectLastCall().times(2);

    control.replay();
    buildScheduler();

    SlaveOffer slaveOffer1 = createSlaveOffer(SLAVE_ID, SLAVE_HOST_1, 4, 4096);
    SlaveOffer slaveOffer2 = createSlaveOffer(SLAVE_ID, SLAVE_HOST_2, 4, 4096);

    // Offer resources for the scheduler to accept.
    TwitterTaskInfo daemonTask = new TwitterTaskInfo(DEFAULT_TASK);
    daemonTask.putToConfiguration("daemon", "true");

    scheduler.createJob(makeJob(OWNER_A, JOB_A, daemonTask, 1));
    String taskIdA = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 0)).task);
    sendOffer(slaveOffer1, taskIdA, SLAVE_ID, SLAVE_HOST_1);

    scheduler.createJob(makeJob(OWNER_B, JOB_B, DEFAULT_TASK, 1));
    String taskIdB = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_B, JOB_B), 0)).task);
    sendOffer(slaveOffer2, taskIdB, SLAVE_ID, SLAVE_HOST_2);

    changeStatus(taskIdA, STARTING);

    scheduler.stateUpdate(SLAVE_HOST_1_KEY,
        new StateUpdateResponse().setIncrementalUpdate(true)
            .setExecutorUUID("foo")
            .setState(ImmutableMap.of(
                taskIdA, new TaskStateUpdate().setStatus(RUNNING),
                "rogue_task", new TaskStateUpdate().setStatus(RUNNING))));
    assertThat(getTask(taskIdA).task.getStatus(), is(RUNNING));

    scheduler.stateUpdate(SLAVE_HOST_2_KEY,
        new StateUpdateResponse().setIncrementalUpdate(true)
            .setExecutorUUID("foo")
            .setState(ImmutableMap.of(taskIdB, new TaskStateUpdate().setStatus(STARTING))));
    assertThat(getTask(taskIdB).task.getStatus(), is(STARTING));

    scheduler.stateUpdate(SLAVE_HOST_2_KEY,
        new StateUpdateResponse().setIncrementalUpdate(true)
            .setExecutorUUID("foo")
            .setState(ImmutableMap.of(taskIdB, new TaskStateUpdate().setDeleted(true))));
    assertThat(getTask(taskIdB).task.getStatus(), is(LOST));
    // Lost task should be restarted.
    assertThat(getTasks(Query.activeQuery(Tasks.jobKey(OWNER_B, JOB_B))).size(), is(1));
  }

  @Test
  public void testSchedulingOrder() throws Exception {
    expectOffer(true);
    expectOffer(true);
    expectOffer(true);
    expectOffer(true);
    expectOffer(true);
    expectOffer(true);

    control.replay();
    buildScheduler();

    TwitterTaskInfo task1 = new TwitterTaskInfo(DEFAULT_TASK);
    task1.putToConfiguration("priority", "10");
    task1.putToConfiguration("production", "false");
    TwitterTaskInfo task2 = new TwitterTaskInfo(DEFAULT_TASK);
    task2.putToConfiguration("priority", "0");
    TwitterTaskInfo task3 = new TwitterTaskInfo(DEFAULT_TASK);
    task3.putToConfiguration("priority", "11");
    task3.putToConfiguration("production", "false");

    scheduler.createJob(makeJob(OWNER_A, JOB_A, task1, 2));
    scheduler.createJob(makeJob(OWNER_B, JOB_A, task2, 2));
    scheduler.createJob(makeJob(OWNER_A, JOB_B, task3, 2));

    String taskId1a = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 0)).task);
    String taskId1b = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 1)).task);
    String taskId2a = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_B, JOB_A), 0)).task);
    String taskId2b = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_B, JOB_A), 1)).task);
    String taskId3a = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_B), 0)).task);
    String taskId3b = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_B), 1)).task);

    SlaveOffer slaveOffer = createSlaveOffer(SLAVE_ID, SLAVE_HOST_1, 4, 4096);
    sendOffer(slaveOffer, taskId2a, SLAVE_ID, SLAVE_HOST_1);
    sendOffer(slaveOffer, taskId2b, SLAVE_ID, SLAVE_HOST_1);
    sendOffer(slaveOffer, taskId1a, SLAVE_ID, SLAVE_HOST_1);
    sendOffer(slaveOffer, taskId1b, SLAVE_ID, SLAVE_HOST_1);
    sendOffer(slaveOffer, taskId3a, SLAVE_ID, SLAVE_HOST_1);
    sendOffer(slaveOffer, taskId3b, SLAVE_ID, SLAVE_HOST_1);
  }

  @Test
  public void testStartAndFinishUpdate() throws Exception {
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1);
    scheduler.createJob(job);
    String updateToken = scheduler.startUpdate(job);
    scheduler.finishUpdate(OWNER_A.getRole(), job.getName(), updateToken, SUCCESS);

    // If the finish update succeeded internally, we should be able to start a new update.
    scheduler.startUpdate(job);
  }

  @Test
  public void testFinishUpdateNotFound() throws Exception {
    control.replay();
    buildScheduler();

    try {
      scheduler.finishUpdate("foo", "foo", "foo", SUCCESS);
      fail("Call should have failed.");
    } catch (ScheduleException e) {
      // Expected.
    }

    try {
      scheduler.finishUpdate("foo", "foo", null, SUCCESS);
      fail("Call should have failed.");
    } catch (ScheduleException e) {
      // Expected.
    }
  }

  @Test
  public void testFinishUpdateInvalidToken() throws Exception {
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1);
    scheduler.createJob(job);
    String token = scheduler.startUpdate(job);

    try {
      scheduler.finishUpdate(OWNER_B.getRole(), job.getName(), "foo", SUCCESS);
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

    JobConfiguration job = makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1);
    scheduler.createJob(job);
    String token = scheduler.startUpdate(job);

    try {
      scheduler.startUpdate(job);
      fail("Second update should have failed.");
    } catch (ScheduleException e) {
      // expected.
    }

    scheduler.finishUpdate(OWNER_A.getRole(), JOB_A, token, SUCCESS);
  }

  private Function<Integer, String> newCommandFactory = new Function<Integer, String>() {
    @Override public String apply(Integer shardId) {
      return "updated start command for shard " + shardId;
    }
  };

  private Function<Integer, String> oldCommandFactory = new Function<Integer, String>() {
    @Override public String apply(Integer shardId) {
      return "start command for shard " + shardId;
    }
  };

  private void verifyUpdate(Set<TaskState> tasks, JobConfiguration job,
      Closure<TaskState> updatedTaskChecker) {
    Map<Integer, TaskState> fetchedShards = Maps.uniqueIndex(tasks, TaskState.STATE_TO_SHARD_ID);
    Map<Integer, TwitterTaskInfo> originalConfigsByShard =
        Maps.uniqueIndex(job.getTaskConfigs(), Tasks.INFO_TO_SHARD_ID);
    assertThat(fetchedShards.keySet(), is(originalConfigsByShard.keySet()));
    for (TaskState task : tasks) {
      updatedTaskChecker.execute(task);
    }
  }

  private abstract class UpdaterTest {
    void runTest(int numTasks, int additionalTasks) throws Exception {
      control.replay();
      buildScheduler();

      JobConfiguration job = makeJob(OWNER_A, JOB_A, DEFAULT_TASK, numTasks);
      for (TwitterTaskInfo config : job.getTaskConfigs()) {
        config.putToConfiguration("start_command", oldCommandFactory.apply(config.getShardId()));
      }
      scheduler.createJob(job);

      JobConfiguration updatedJob =
          makeJob(OWNER_A, JOB_A, DEFAULT_TASK, numTasks + additionalTasks);
      for (TwitterTaskInfo config : updatedJob.getTaskConfigs()) {
        config.putToConfiguration("start_command", newCommandFactory.apply(config.getShardId()));
      }
      String updateToken = scheduler.startUpdate(updatedJob);

      Set<Integer> jobShards = ImmutableSet.copyOf(Iterables.transform(
          updatedJob.getTaskConfigs(), Tasks.INFO_TO_SHARD_ID));

      UpdateResult result =
          performRegisteredUpdate(updatedJob, updateToken, jobShards, numTasks, additionalTasks);

      Set<TaskState> tasks = getTasks(Query.byStatus(RUNNING));
      verify(tasks, job, updatedJob);

      scheduler.finishUpdate(OWNER_A.role, JOB_A, updateToken, result);
      scheduler.startUpdate(job);
    }

    abstract UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
        Set<Integer> jobShards, int numTasks, int additionalTasks) throws Exception;

    abstract void verify(Set<TaskState> tasks, JobConfiguration oldJob,
        JobConfiguration updatedJob);

  }

  @Test
  public void testUpdateShards() throws Exception {
    int numTasks = 10;
    int additionalTasks = 0;
    // Kill Tasks called at RUNNING->UPDATING
    expectKillTask(numTasks);

    new UpdaterTest() {
      @Override UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
          Set<Integer> jobShards, int numTasks, int additionalTasks) throws Exception{
        changeStatus(queryByOwner(OWNER_A), ASSIGNED);
        changeStatus(queryByOwner(OWNER_A), RUNNING);

        scheduler.updateShards(OWNER_A.role, JOB_A, jobShards, updateToken);
        assertThat(getTasks(Query.byStatus(UPDATING)).size(), is(numTasks));

        changeStatus(queryByOwner(OWNER_A), FINISHED);
        changeStatus(Query.byStatus(PENDING), ASSIGNED);
        changeStatus(Query.byStatus(ASSIGNED), RUNNING);

        return SUCCESS;
      }

      @Override void verify(Set<TaskState> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {
        verifyUpdate(tasks, oldJob, new Closure<TaskState>() {
          @Override public void execute(TaskState state) {
            TwitterTaskInfo task = TaskState.STATE_TO_INFO.apply(state);
            assertThat(task.getStartCommand(), is(newCommandFactory.apply(task.getShardId())));
          }
        });
      }
    }.runTest(numTasks, additionalTasks);
  }

  @Test
  public void testRollback() throws Exception {
    int numTasks = 10;
    int additionalTasks = 0;
    // Kill Tasks called at RUNNING->UPDATING.
    expectKillTask(numTasks);

    new UpdaterTest() {
      @Override UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
          Set<Integer> jobShards, int numTasks, int additionalTask) throws Exception{
        changeStatus(queryByOwner(OWNER_A), ASSIGNED);
        changeStatus(queryByOwner(OWNER_A), RUNNING);

        scheduler.updateShards(OWNER_A.role, JOB_A, jobShards, updateToken);
        assertThat(getTasks(Query.byStatus(UPDATING)).size(), is(numTasks));

        changeStatus(queryByOwner(OWNER_A), KILLED);

        scheduler.rollbackShards(OWNER_A.role, JOB_A, jobShards, updateToken);

        changeStatus(Query.byStatus(PENDING), ASSIGNED);
        changeStatus(Query.byStatus(ASSIGNED), RUNNING);

        return UpdateResult.FAILED;
      }

      @Override void verify(Set<TaskState> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {
        verifyUpdate(tasks, oldJob, new Closure<TaskState>() {
          @Override public void execute(TaskState state) {
            TwitterTaskInfo task = TaskState.STATE_TO_INFO.apply(state);
            assertThat(task.getStartCommand(), is(oldCommandFactory.apply(task.getShardId())));
          }
        });
      }
    }.runTest(numTasks, additionalTasks);
  }

  @Test
  public void testInvalidTransition() throws Exception {
    int numTasks = 10;
    int additionalTasks = 0;
    // Kill Tasks called at RUNNING->UPDATING and UPDATING->RUNNING (Invalid).
    int expectedKillTasks = 20;
    expectKillTask(expectedKillTasks);

    new UpdaterTest() {
      @Override UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
          Set<Integer> jobShards, int numTasks, int additionalTasks) throws Exception{
        changeStatus(queryByOwner(OWNER_A), ASSIGNED);
        changeStatus(queryByOwner(OWNER_A), RUNNING);

        scheduler.updateShards(OWNER_A.role, JOB_A, jobShards, updateToken);
        assertThat(getTasks(Query.byStatus(UPDATING)).size(), is(numTasks));

        changeStatus(queryByOwner(OWNER_A), RUNNING);
        changeStatus(queryByOwner(OWNER_A), KILLED);
        changeStatus(Query.byStatus(PENDING), ASSIGNED);
        changeStatus(Query.byStatus(ASSIGNED), RUNNING);

        return SUCCESS;
      }

      @Override void verify(Set<TaskState> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {
        verifyUpdate(tasks, oldJob, new Closure<TaskState>() {
          @Override public void execute(TaskState state) {
            TwitterTaskInfo task = TaskState.STATE_TO_INFO.apply(state);
            assertThat(task.getStartCommand(), is(newCommandFactory.apply(task.getShardId())));
          }
        });
      }
    }.runTest(numTasks, additionalTasks);
  }

  @Test
  public void testPendingToUpdating() throws Exception {
    int numTasks = 10;
    int additionalTasks = 0;

    new UpdaterTest() {
      @Override UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
          Set<Integer> jobShards, int numTasks, int additionalTasks) throws Exception{
        scheduler.updateShards(OWNER_A.role, JOB_A, jobShards, updateToken);
        assertThat(getTasks(Query.byStatus(PENDING)).size(), is(numTasks));

        changeStatus(Query.byStatus(PENDING), ASSIGNED);
        changeStatus(Query.byStatus(ASSIGNED), RUNNING);

        return SUCCESS;
      }

      @Override void verify(Set<TaskState> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {
        verifyUpdate(tasks, oldJob, new Closure<TaskState>() {
          @Override public void execute(TaskState state) {
            TwitterTaskInfo task = TaskState.STATE_TO_INFO.apply(state);
            assertThat(task.getStartCommand(), is(newCommandFactory.apply(task.getShardId())));
          }
        });
      }
    }.runTest(numTasks, additionalTasks);
  }

  @Test
  public void testIncreaseShardsUpdate() throws Exception {
    int numTasks = 5;
    int additionalTasks = 5;
    // Kill Tasks called at RUNNING->UPDATING.
    expectKillTask(numTasks);

    new UpdaterTest() {
      @Override UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
          Set<Integer> jobShards, int numTasks, int additionalTasks) throws Exception{
        changeStatus(queryByOwner(OWNER_A), ASSIGNED);
        changeStatus(queryByOwner(OWNER_A), RUNNING);

        scheduler.updateShards(OWNER_A.role, JOB_A, jobShards, updateToken);

        changeStatus(Query.byStatus(UPDATING), KILLED);

        assertThat(getTasks(Query.byStatus(PENDING)).size(), is(numTasks + additionalTasks));

        changeStatus(Query.byStatus(PENDING), ASSIGNED);
        changeStatus(Query.byStatus(ASSIGNED), RUNNING);

        return SUCCESS;
      }

      @Override void verify(Set<TaskState> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {
        verifyUpdate(tasks, updatedJob, new Closure<TaskState>() {
          @Override public void execute(TaskState state) {
            TwitterTaskInfo task = TaskState.STATE_TO_INFO.apply(state);
            assertThat(task.getStartCommand(), is(newCommandFactory.apply(task.getShardId())));
          }
        });
      }
    }.runTest(numTasks, additionalTasks);
  }

  @Test
  public void testDecreaseShardsUpdate() throws Exception {
    int numTasks = 10;
    int additionalTasks = -5;
    // Kill Tasks called at RUNNING->UPDATING.
    int expectedKillTasks = 10;
    expectKillTask(expectedKillTasks);

    new UpdaterTest() {
      @Override UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
          Set<Integer> jobShards, int numTasks, int additionalTasks) throws Exception{
        changeStatus(queryByOwner(OWNER_A), ASSIGNED);
        changeStatus(queryByOwner(OWNER_A), RUNNING);

        scheduler.updateShards(OWNER_A.role, JOB_A, jobShards, updateToken);

        changeStatus(Query.byStatus(UPDATING), KILLED);

        assertThat(getTasks(Query.byStatus(PENDING)).size(), is(numTasks + additionalTasks));

        changeStatus(Query.byStatus(RUNNING), KILLING);
        changeStatus(Query.byStatus(PENDING), ASSIGNED);
        changeStatus(Query.byStatus(ASSIGNED), RUNNING);

        return SUCCESS;
      }

      @Override void verify(Set<TaskState> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {
        verifyUpdate(tasks, updatedJob, new Closure<TaskState>() {
          @Override public void execute(TaskState state) {
            TwitterTaskInfo task = TaskState.STATE_TO_INFO.apply(state);
            assertThat(task.getStartCommand(), is(newCommandFactory.apply(task.getShardId())));
          }
        });
      }
    }.runTest(numTasks, additionalTasks);
  }

  @Test
  public void testIncreaseShardsRollback() throws Exception {
    int numTasks = 5;
    int additionalTasks = 5;
    // Kill Tasks called at RUNNING->UPDATING.
    expectKillTask(numTasks);

    new UpdaterTest() {
      @Override UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
          Set<Integer> jobShards, int numTasks, int additionalTasks) throws Exception{
        changeStatus(queryByOwner(OWNER_A), ASSIGNED);
        changeStatus(queryByOwner(OWNER_A), RUNNING);

        scheduler.updateShards(OWNER_A.role, JOB_A, jobShards, updateToken);

        changeStatus(Query.byStatus(UPDATING), KILLED);

        assertThat(getTasks(Query.byStatus(PENDING)).size(), is(numTasks + additionalTasks));

        scheduler.rollbackShards(OWNER_A.role, JOB_A, jobShards, updateToken);

        changeStatus(Query.byStatus(PENDING), ASSIGNED);
        changeStatus(Query.byStatus(ASSIGNED), RUNNING);

        return UpdateResult.FAILED;
      }

      @Override void verify(Set<TaskState> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {
        verifyUpdate(tasks, oldJob, new Closure<TaskState>() {
          @Override public void execute(TaskState state) {
            TwitterTaskInfo task = TaskState.STATE_TO_INFO.apply(state);
            assertThat(task.getStartCommand(), is(oldCommandFactory.apply(task.getShardId())));
          }
        });
      }
    }.runTest(numTasks, additionalTasks);
  }

  @Test(expected = ScheduleException.class)
  public void testIncreaseShardsExceedsQuota() throws Exception {
    int numTasks = 10;
    int additionalTasks = 1;

    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, DEFAULT_TASK, numTasks);
    for (TwitterTaskInfo config : job.getTaskConfigs()) {
      config.putToConfiguration("start_command", oldCommandFactory.apply(config.getShardId()));
    }
    scheduler.createJob(job);

    JobConfiguration updatedJob =
        makeJob(OWNER_A, JOB_A, DEFAULT_TASK, numTasks + additionalTasks);
    for (TwitterTaskInfo config : updatedJob.getTaskConfigs()) {
      config.putToConfiguration("start_command", newCommandFactory.apply(config.getShardId()));
    }
    scheduler.startUpdate(updatedJob);
  }

  @Test
  public void testDecreaseShardsRollback() throws Exception {
    int numTasks = 10;
    int additionalTasks = -5;
    // Kill Tasks called at RUNNING->UPDATING and PENDING->ROLLBACK
    int expectedKillTasks = 5;
    expectKillTask(expectedKillTasks);

    new UpdaterTest() {
      @Override UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
          Set<Integer> jobShards, int numTasks, int additionalTasks) throws Exception{
        changeStatus(queryByOwner(OWNER_A), ASSIGNED);
        changeStatus(queryByOwner(OWNER_A), RUNNING);

        scheduler.updateShards(OWNER_A.role, JOB_A, jobShards, updateToken);

        changeStatus(Query.byStatus(UPDATING), KILLED);

        assertThat(getTasks(Query.byStatus(PENDING)).size(), is(numTasks + additionalTasks));

        scheduler.rollbackShards(OWNER_A.role, JOB_A, jobShards, updateToken);

        changeStatus(Query.byStatus(PENDING), ASSIGNED);
        changeStatus(Query.byStatus(ASSIGNED), RUNNING);

        return UpdateResult.FAILED;
      }

      @Override void verify(Set<TaskState> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {
        verifyUpdate(tasks, oldJob, new Closure<TaskState>() {
          @Override public void execute(TaskState state) {
            TwitterTaskInfo task = TaskState.STATE_TO_INFO.apply(state);
            assertThat(task.getStartCommand(), is(oldCommandFactory.apply(task.getShardId())));
          }
        });
      }
    }.runTest(numTasks, additionalTasks);
  }

  @Test
  public void testTaskIdExpansion() throws Exception {
    expectOffer(true);

    control.replay();
    buildScheduler();

    TwitterTaskInfo config = new TwitterTaskInfo(DEFAULT_TASK);
    config.putToConfiguration("start_command", "%task_id%");

    scheduler.createJob(makeJob(OWNER_A, JOB_A, config, 1));

    String taskId = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 0)).task);
    SlaveOffer offer = createSlaveOffer(SLAVE_ID, SLAVE_HOST_1, 4, 4096);
    sendOffer(offer, taskId, SLAVE_ID, SLAVE_HOST_1);

    AssignedTask task = getTask(taskId).task.getAssignedTask();
    assertThat(task.getTask().getStartCommand(), is(taskId));
  }

  @Test
  public void testShardIdExpansion() throws Exception {
    expectOffer(true);

    control.replay();
    buildScheduler();

    TwitterTaskInfo config = new TwitterTaskInfo(DEFAULT_TASK);
    config.putToConfiguration("start_command", "%shard_id%");

    scheduler.createJob(makeJob(OWNER_A, JOB_A, config, 1));

    String taskId = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 0)).task);
    SlaveOffer offer = createSlaveOffer(SLAVE_ID, SLAVE_HOST_1, 4, 4096);
    sendOffer(offer, taskId, SLAVE_ID, SLAVE_HOST_1);

    AssignedTask task = getTask(taskId).task.getAssignedTask();
    assertThat(task.getTask().getStartCommand(), is("0"));
  }

  @Test
  public void testPortResource() throws Exception {
    expectOffer(true);

    control.replay();
    buildScheduler();

    TwitterTaskInfo config = new TwitterTaskInfo(DEFAULT_TASK);
    config.putToConfiguration("start_command", "%port:one% %port:two% %port:three%");

    scheduler.createJob(makeJob(OWNER_A, JOB_A, config, 1));

    String taskId = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 0)).task);

    Set<Integer> assignedPorts = ImmutableSet.of(80, 81, 82);
    SlaveOffer threePorts = createSlaveOffer(SLAVE_ID, SLAVE_HOST_1, 4, 4096, ImmutableSet.of(Pair.of(80, 82)));
    sendOffer(threePorts, taskId, SLAVE_ID, SLAVE_HOST_1,
        ImmutableSet.of("one", "two", "three"), assignedPorts);

    AssignedTask task = getTask(taskId).task.getAssignedTask();

    assertThat(ImmutableSet.copyOf(Splitter.on(" ").split(task.getTask().getStartCommand())),
        is(ImmutableSet.of("80", "81", "82")));
  }

  @Test
  public void testBadExpansion() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo task = new TwitterTaskInfo(DEFAULT_TASK);
    task.putToConfiguration("start_command", "%port%");
    scheduler.createJob(makeJob(OWNER_A, "foo", task, 1));
  }

  // TODO(William Farner): Inject a task ID generation function into StateManager so that we can
  //     expect specific task IDs to be killed here.
  private void expectKillTask(int numTasks) {
    killTask.execute((String) anyObject());
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

  private static SlaveOffer createSlaveOffer(SlaveID slave, String slaveHost, double cpu,
      double ramMb) {
    return createSlaveOffer(slave, slaveHost, cpu, ramMb,
        ImmutableSet.<Pair<Integer, Integer>>of());
  }

  private static SlaveOffer createSlaveOffer(SlaveID slave, String slaveHost, double cpu,
      double ramMb, Set<Pair<Integer, Integer>> ports) {

    Ranges portRanges = Ranges.newBuilder()
        .addAllRange(Iterables.transform(ports, new Function<Pair<Integer, Integer>, Range>() {
          @Override public Range apply(Pair<Integer, Integer> range) {
            return Range.newBuilder().setBegin(range.getFirst()).setEnd(range.getSecond()).build();
          }
        }))
        .build();

    return SlaveOffer.newBuilder()
        .addResources(Resource.newBuilder().setType(Type.SCALAR).setName(Resources.CPUS)
            .setScalar(Scalar.newBuilder().setValue(cpu)))
        .addResources(Resource.newBuilder().setType(Type.SCALAR).setName(Resources.RAM_MB)
            .setScalar(Scalar.newBuilder().setValue(ramMb)))
        .addResources(Resource.newBuilder().setType(Type.RANGES).setName(Resources.PORTS)
            .setRanges(portRanges))
        .setSlaveId(slave)
        .setHostname(slaveHost)
        .build();
  }

  private static TwitterTaskInfo defaultTask() {
    return new TwitterTaskInfo().setConfiguration(ImmutableMap.<String, String>builder()
        .put("start_command", "date")
        .put("num_cpus", "1.0")
        .put("ram_mb", "1024")
        .put("disk_mb", "1024")
        .put("hdfs_path", "/fake/path")
        .put("production", "true")
        .build());
  }

  private TaskState getTask(String taskId) {
    return getOnlyTask(query(taskId));
  }

  private TaskState getOnlyTask(Query query) {
    return Iterables.getOnlyElement(scheduler.getTasks((query)));
  }

  private Set<TaskState> getTasks(Query query) {
    return scheduler.getTasks(query);
  }

  private Set<TaskState> getTasks(String... taskIds) {
    return scheduler.getTasks(query(taskIds));
  }

  private Set<TaskState> getTasksByStatus(ScheduleStatus status) {
    return scheduler.getTasks(Query.byStatus(status));
  }

  private Set<TaskState> getTasksOwnedBy(Identity owner) {
    return scheduler.getTasks(query(owner, null, null));
  }

  private Query query(Iterable<String> taskIds) {
    return query(null, null, taskIds);
  }

  private Query query(String... taskIds) {
    return query(null, null, ImmutableList.copyOf(taskIds));
  }

  private Query queryByOwner(Identity owner) {
    return query(owner, null, null);
  }

  private Query queryJob(Identity owner, String jobName) {
    return query(owner, jobName, null);
  }

  private Query query(@Nullable Identity owner, @Nullable String jobName,
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

    return new Query(query);
  }

  public void changeStatus(Query query, ScheduleStatus status) {
    scheduler.setTaskStatus(query, status, null);
  }

  public void changeStatus(String taskId, ScheduleStatus status) {
    scheduler.setTaskStatus(query(Arrays.asList(taskId)), status, null);
  }

  private void expectOffer(boolean passFilter) {
    expect(executorPulseMonitor.isAlive(EasyMock.<ExecutorKey>anyObject())).andReturn(true);
    expect(schedulingFilter.staticFilter(EasyMock.<Resources>anyObject(),
        EasyMock.<String>anyObject()))
        .andReturn(passFilter ? Predicates.<TwitterTaskInfo> alwaysTrue()
            : Predicates.<TwitterTaskInfo> alwaysFalse());
    @SuppressWarnings("unchecked")
    Function<Query, Iterable<TwitterTaskInfo>> anyTaskFetcher =
        (Function<Query, Iterable<TwitterTaskInfo>>) anyObject();
    expect(schedulingFilter.dynamicHostFilter(
        anyTaskFetcher, (String) anyObject()))
        .andReturn(passFilter ? Predicates.<TwitterTaskInfo> alwaysTrue()
            : Predicates.<TwitterTaskInfo> alwaysFalse());
  }
}

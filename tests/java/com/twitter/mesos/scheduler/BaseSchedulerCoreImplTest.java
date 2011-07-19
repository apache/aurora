package com.twitter.mesos.scheduler;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Resource.Scalar;
import org.apache.mesos.Protos.Resource.Type;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.SlaveOffer;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.base.Closure;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.CronCollisionPolicy;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.LiveTaskInfo;
import com.twitter.mesos.gen.RegisteredTaskUpdate;
import com.twitter.mesos.gen.ResourceConsumption;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.SchedulerCore.RestartException;
import com.twitter.mesos.scheduler.SchedulerCore.TwitterTask;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import com.twitter.mesos.scheduler.storage.JobStore;
import com.twitter.mesos.scheduler.storage.SchedulerStore;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.Work.NoResult;
import com.twitter.mesos.scheduler.storage.TaskStore;

import static com.twitter.mesos.gen.ScheduleStatus.ASSIGNED;
import static com.twitter.mesos.gen.ScheduleStatus.FAILED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED_BY_CLIENT;
import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static com.twitter.mesos.gen.ScheduleStatus.STARTING;
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

  private static final Identity OWNER_B = new Identity("Test_Role_B", "Test_User_B");
  private static final String JOB_B = "Test_Job_B";

  private static final SlaveID SLAVE_ID = SlaveID.newBuilder().setValue("SlaveId").build();
  private static final String SLAVE_HOST_1 = "SlaveHost1";
  private static final String SLAVE_HOST_2 = "SlaveHost2";

  private SchedulingFilter schedulingFilter;
  private Closure<String> killTask;
  private SchedulerCoreImpl scheduler;
  private CronJobManager cron;
  private PulseMonitor<String> executorPulseMonitor;
  private Function<TwitterTaskInfo, TwitterTaskInfo> executorResourceAugmenter;
  private FakeClock clock;

  @Before
  public void setUp() throws Exception {
    schedulingFilter = createMock(SchedulingFilter.class);
    killTask = createMock(new Clazz<Closure<String>>() {});
    executorPulseMonitor = createMock(new Clazz<PulseMonitor<String>>() {});
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

  private void buildScheduler(Storage storage) throws Exception {
    ImmediateJobManager immediateManager = new ImmediateJobManager();
    cron = new CronJobManager(storage);
    StateManager stateManager = new StateManager(storage, clock);
    scheduler = new SchedulerCoreImpl(cron, immediateManager, stateManager, schedulingFilter,
        executorPulseMonitor, executorResourceAugmenter);
    cron.schedulerCore = scheduler;
    immediateManager.schedulerCore = scheduler;
    scheduler.initialize();
    scheduler.start(killTask);
  }

  @Test
  public void testCreateJob() throws Exception {
    control.replay();
    buildScheduler();

    int numTasks = 10;
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
      @Override protected void execute(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) {}
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
      @Override protected void execute(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) {
        taskStore.add(ImmutableSet.of(new ScheduledTask()
            .setStatus(PENDING)
            .setAssignedTask(
                new AssignedTask()
                    .setTaskId(storedTaskId)
                    .setTask(storedTask))));
      }
    });

    buildScheduler(storage);

    SlaveOffer slaveOffer = createSlaveOffer(SLAVE_ID, SLAVE_HOST_1, 4, 4096);
    TwitterTask launchedTask = scheduler.offer(slaveOffer);
    assertThat(launchedTask.task.getTask(), is(storedTask));
    assertThat(getTask(storedTaskId).task.getStatus(), is(ASSIGNED));
  }

  @Test
  public void testCreateJobNoHdfs() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo task = new TwitterTaskInfo().setConfiguration(
        ImmutableMap.<String, String> builder()
            .put("start_command", "date")
            .put("cpus", "1.0")
            .put("ram_mb", "1024")
            .build());

    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 1);
    scheduler.createJob(job);
    assertTaskCount(1);

    TwitterTaskInfo task2 = new TwitterTaskInfo().setConfiguration(
        ImmutableMap.<String, String> builder()
            .put("start_command", "date")
            .put("hdfs_path", "")
            .put("cpus", "1.0")
            .put("ram_mb", "1024")
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
    assertThat(taskId == newTaskId, is(false));
  }

  @Test(expected = TaskDescriptionException.class)
  public void testCreateEmptyJob() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(new JobConfiguration().setOwner(OWNER_A).setName(JOB_A));
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

    assertNull(scheduler.offer(slaveOffer));
    assertNull(scheduler.offer(slaveOffer));
    assertNull(scheduler.offer(slaveOffer));

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
        assertThat(getTask(taskId).task.getStatus(), is(KILLED_BY_CLIENT));
      }
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

    String taskId = Tasks.id(getOnlyTask(queryByOwner(OWNER_A)).task);

    // This transition should be rejected.
    changeStatus(queryByOwner(OWNER_A), LOST);

    assertThat(getTask(taskId).task.getStatus(), is(KILLED_BY_CLIENT));
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
    changeStatus(query(taskId), ASSIGNED);
    changeStatus(query(taskId), STARTING);
    changeStatus(query(taskId), RUNNING);
    scheduler.killTasks(query(taskId));
    assertThat(getTask(taskId).task.getStatus(), is(KILLED_BY_CLIENT));
    assertThat(getTasks(queryByOwner(OWNER_A)).size(), is(1));
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

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 10));
    assertTaskCount(10);

    scheduler.createJob(makeJob(OWNER_A, JOB_A + "2", DEFAULT_TASK, 10));
    assertTaskCount(20);

    scheduler.killTasks(queryJob(OWNER_A, JOB_A + "2"));
    assertTaskCount(10);

    for (TaskState state : scheduler.getTasks(Query.GET_ALL)) {
      assertThat(state.task.getAssignedTask().getTask().getJobName(), is(JOB_A));
    }
  }

  private void sendOffer(SlaveOffer offer, String taskId, SlaveID slave, String slaveHost)
      throws Exception {
    AssignedTask task = getTask(taskId).task.getAssignedTask().deepCopy();
    task.setSlaveId(slave.getValue());
    task.setSlaveHost(slaveHost);

    List<Resource> resources = ImmutableList.of(
        Resources.makeResource(Resources.CPUS, task.getTask().getNumCpus()),
        Resources.makeResource(Resources.RAM_MB, task.getTask().getRamMb())
    );

    assertThat(scheduler.offer(offer),
        is(SchedulerCoreImpl.makeTwitterTask(task, slave.getValue(), resources)));
  }

  @Test
  public void testExecutorBootstrap() throws Exception {
    expect(executorPulseMonitor.isAlive(SLAVE_HOST_1)).andReturn(false);

    SlaveOffer slaveOffer = createSlaveOffer(SLAVE_ID, SLAVE_HOST_1, 3, 4096);
    final TwitterTaskInfo offerInfo = ConfigurationManager.makeConcrete(slaveOffer);

    final TwitterTaskInfo augmented = new TwitterTaskInfo().setNumCpus(3.14).setRamMb(1137);
    expect(executorResourceAugmenter.apply(EasyMock.<TwitterTaskInfo>notNull()))
        .andReturn(augmented);

    final Predicate<TwitterTaskInfo> staticFilter =
        createMock(new Clazz<Predicate<TwitterTaskInfo>>() {});
    expect(staticFilter.apply(augmented)).andReturn(false);
    expect(schedulingFilter.staticFilter(offerInfo, SLAVE_HOST_1)).andReturn(staticFilter);

    control.replay();

    buildScheduler();
    assertNull(scheduler.offer(slaveOffer));
  }

  @Test
  public void testResourceUpdate() throws Exception {
    expectOffer(true);
    executorPulseMonitor.pulse(SLAVE_HOST_1);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1));
    String taskId = Tasks.id(getOnlyTask(queryByOwner(OWNER_A)).task);

    SlaveOffer slaveOffer = createSlaveOffer(SLAVE_ID, SLAVE_HOST_1, 4, 4096);
    sendOffer(slaveOffer, taskId, SLAVE_ID, SLAVE_HOST_1);

    changeStatus(taskId, RUNNING);

    assertNull(Iterables.getOnlyElement(scheduler.getTasks(query(taskId))).volatileState.resources);

    RegisteredTaskUpdate update = new RegisteredTaskUpdate()
        .setSlaveHost(SLAVE_HOST_1);
    ResourceConsumption resources = new ResourceConsumption()
        .setDiskUsedMb(100)
        .setMemUsedMb(10)
        .setCpusUsed(4)
        .setLeasedPorts(ImmutableMap.<String, Integer>of("health", 50000))
        .setNiceLevel(5);
    update.addToTaskInfos(new LiveTaskInfo()
        .setTaskId(taskId)
        .setStatus(RUNNING)
        .setResources(resources));
    scheduler.updateRegisteredTasks(update);

    assertThat(Iterables.getOnlyElement(scheduler.getTasks(query(taskId))).volatileState.resources,
        is(resources));
  }

  @Test
  public void testSlaveAdjustsSchedulerTaskState() throws Exception {
    expectOffer(true);
    expectOffer(true);
    executorPulseMonitor.pulse(SLAVE_HOST_1);

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
    RegisteredTaskUpdate update = new RegisteredTaskUpdate().setSlaveHost(SLAVE_HOST_1);
    update.addToTaskInfos(new LiveTaskInfo().setTaskId(taskId1).setStatus(LOST));
    update.addToTaskInfos(new LiveTaskInfo().setTaskId(taskId2).setStatus(FINISHED));
    scheduler.updateRegisteredTasks(update);

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
    executorPulseMonitor.pulse(SLAVE_HOST_2);

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

    scheduler.updateRegisteredTasks(new RegisteredTaskUpdate().setSlaveHost(SLAVE_HOST_2)
        .setTaskInfos(Arrays.asList(
            new LiveTaskInfo().setTaskId(taskIdA).setStatus(FAILED),
            new LiveTaskInfo().setTaskId(taskIdB).setStatus(RUNNING))));

    assertThat(getTasksByStatus(RUNNING).size(), is(2));
    assertTaskCount(2);
  }

  @Test
  public void testSlaveStopsReportingRunningTask() throws Exception {
    expectOffer(true);
    expectOffer(true);
    expectOffer(true);
    expectOffer(true);
    executorPulseMonitor.pulse(SLAVE_HOST_1);
    executorPulseMonitor.pulse(SLAVE_HOST_2);

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
    scheduler.updateRegisteredTasks(new RegisteredTaskUpdate().setSlaveHost(SLAVE_HOST_1)
        .setTaskInfos(ImmutableList.<LiveTaskInfo>of()));
    Set<TaskState> rescheduledTasks = getTasks(new Query(new TaskQuery()
        .setOwner(OWNER_A).setJobName(JOB_A).setStatuses(EnumSet.of(PENDING))));
    assertThat(rescheduledTasks.size(), is(2));
    Set<String> rescheduledTaskAncestors = Sets.newHashSet(Iterables.transform(rescheduledTasks,
        getAncestorId));
    assertThat(rescheduledTaskAncestors, is((Set<String>) Sets.newHashSet(taskIdA, taskIdB)));

    // Send an update from host 2 that does not include the FAILED task.
    scheduler.updateRegisteredTasks(new RegisteredTaskUpdate().setSlaveHost(SLAVE_HOST_2)
        .setTaskInfos(ImmutableList.<LiveTaskInfo>of()));
    rescheduledTasks = getTasks(new Query(new TaskQuery()
        .setOwner(OWNER_B).setJobName(JOB_B).setStatuses(EnumSet.of(PENDING))));
    assertThat(rescheduledTasks.size(), is(1));
    rescheduledTaskAncestors = Sets.newHashSet(Iterables.transform(rescheduledTasks,
        getAncestorId));
    assertThat(rescheduledTaskAncestors, is((Set<String>) Sets.newHashSet(taskIdC)));
    assertThat(Iterables.isEmpty(getTasks(taskIdD)), is(true));
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
    TwitterTaskInfo task2 = new TwitterTaskInfo(DEFAULT_TASK);
    task2.putToConfiguration("priority", "12");
    TwitterTaskInfo task3 = new TwitterTaskInfo(DEFAULT_TASK);
    task3.putToConfiguration("priority", "11");

    scheduler.createJob(makeJob(OWNER_A, JOB_A, task1, 2));
    scheduler.createJob(makeJob(OWNER_A, JOB_B, task2, 2));
    scheduler.createJob(makeJob(OWNER_B, JOB_A, task3, 2));

    String taskId1a = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 0)).task);
    String taskId1b = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 1)).task);
    String taskId2a = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_B), 0)).task);
    String taskId2b = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_B), 1)).task);
    String taskId3a = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_B, JOB_A), 0)).task);
    String taskId3b = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_B, JOB_A), 1)).task);

    SlaveOffer slaveOffer = createSlaveOffer(SLAVE_ID, SLAVE_HOST_1, 4, 4096);
    sendOffer(slaveOffer, taskId2a, SLAVE_ID, SLAVE_HOST_1);
    sendOffer(slaveOffer, taskId2b, SLAVE_ID, SLAVE_HOST_1);
    sendOffer(slaveOffer, taskId3a, SLAVE_ID, SLAVE_HOST_1);
    sendOffer(slaveOffer, taskId3b, SLAVE_ID, SLAVE_HOST_1);
    sendOffer(slaveOffer, taskId1a, SLAVE_ID, SLAVE_HOST_1);
    sendOffer(slaveOffer, taskId1b, SLAVE_ID, SLAVE_HOST_1);
  }

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
    return SlaveOffer.newBuilder()
        .addResources(Resource.newBuilder().setType(Type.SCALAR).setName(Resources.CPUS)
            .setScalar(Scalar.newBuilder().setValue(cpu)))
        .addResources(Resource.newBuilder().setType(Type.SCALAR).setName(Resources.RAM_MB)
            .setScalar(Scalar.newBuilder().setValue(ramMb)))
        .setSlaveId(slave)
        .setHostname(slaveHost)
        .build();
  }

  private static TwitterTaskInfo defaultTask() {
    return new TwitterTaskInfo().setConfiguration(ImmutableMap.<String, String>builder()
        .put("start_command", "date")
        .put("cpus", "1.0")
        .put("ram_mb", "1024")
        .put("hdfs_path", "/fake/path")
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
    expect(executorPulseMonitor.isAlive((String) anyObject())).andReturn(true);
    expect(schedulingFilter.staticFilter((TwitterTaskInfo) anyObject(), (String) anyObject()))
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

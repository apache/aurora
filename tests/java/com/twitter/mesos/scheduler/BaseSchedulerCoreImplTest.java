package com.twitter.mesos.scheduler;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.twitter.common.base.Closure;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.CronCollisionPolicy;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.LiveTaskInfo;
import com.twitter.mesos.gen.RegisteredTaskUpdate;
import com.twitter.mesos.gen.ResourceConsumption;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.UpdateConfig;
import com.twitter.mesos.scheduler.JobManager.JobUpdateResult;
import com.twitter.mesos.scheduler.SchedulerCore.RestartException;
import com.twitter.mesos.scheduler.SchedulerCore.UpdateException;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import com.twitter.mesos.scheduler.storage.Storage;

import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Resource.Scalar;
import org.apache.mesos.Protos.Resource.Type;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.SlaveOffer;
import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.twitter.mesos.gen.ScheduleStatus.FAILED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED_BY_CLIENT;
import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static com.twitter.mesos.gen.ScheduleStatus.STARTING;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
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
 * @author William Farner
 */
public abstract class BaseSchedulerCoreImplTest extends EasyMockTest {

  private static final String FRAMEWORK_ID = "framework_id";

  private static final String OWNER_A = "Test_Owner_A";
  private static final String JOB_A = "Test_Job_A";
  private static final String JOB_A_KEY = Tasks.jobKey(OWNER_A, JOB_A);
  private static final TwitterTaskInfo DEFAULT_TASK = defaultTask();

  private static final String OWNER_B = "Test_Owner_B";
  private static final String JOB_B = "Test_Job_B";

  private static final SlaveID SLAVE_ID = SlaveID.newBuilder().setValue("SlaveId").build();
  private static final String SLAVE_HOST_1 = "SlaveHost1";
  private static final String SLAVE_HOST_2 = "SlaveHost2";

  private SchedulingFilter schedulingFilter;
  private Closure<String> killTask;
  private SchedulerCoreImpl scheduler;
  private CronJobManager cron;
  private Function<String, TwitterTaskInfo> updateTaskBuilder;

  @Before
  public void setUp() throws Exception {
    schedulingFilter = createMock(SchedulingFilter.class);
    killTask = createMock(new Clazz<Closure<String>>() {});
    updateTaskBuilder = createMock(new Clazz<Function<String, TwitterTaskInfo>>() {});
  }

  /**
   * Subclasses should create the {@code Storage} implementation to be used by the
   * {@link SchedulerCoreImpl} under test.
   *
   * @return the {@code Storage} to for the SchedulerCoreImpl to use under tests
   * @throws Exception if there is a problem creating the storage implementation
   */
  protected abstract Storage createStorage() throws Exception;

  /**
   * Called by the test when 1 restore operation is expected from the underlying {@link Storage}.
   *
   * @throws Exception if the underlying restore operation throws
   */
  protected abstract void expectRestore() throws Exception;

  /**
   * Called by the test to allow the underlying {@link Storage} to verify the number of atomic
   * persists that occur under the test.
   *
   * @param count The number of atomic persists to expect.
   * @throws Exception if the underlying persist operation throws
   */
  protected abstract void expectPersists(int count) throws Exception;

  private void buildScheduler() throws Exception {
    Storage storage = createStorage();

    ImmediateJobManager immediateManager = new ImmediateJobManager();
    cron = new CronJobManager(storage);

    scheduler = new SchedulerCoreImpl(cron, immediateManager, storage, schedulingFilter,
        updateTaskBuilder);
    cron.schedulerCore = scheduler;
    immediateManager.schedulerCore = scheduler;
    scheduler.initialize();
    scheduler.start(killTask);
  }

  @Test
  public void testCreateJob() throws Exception {
    expectRestore();
    expectPersists(1);

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
  public void testCreateJobNoHdfs() throws Exception {
    expectRestore();
    expectPersists(2);

    control.replay();
    buildScheduler();

    TwitterTaskInfo task = new TwitterTaskInfo().setConfiguration(
        ImmutableMap.<String, String>builder()
        .put("start_command", "date")
        .put("cpus", "1.0")
        .put("ram_mb", "1024")
        .build());

    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 1);
    scheduler.createJob(job);
    assertTaskCount(1);

    TwitterTaskInfo task2 = new TwitterTaskInfo().setConfiguration(
        ImmutableMap.<String, String>builder()
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
    expectRestore();

    control.replay();
    buildScheduler();

    expectRejected("foo/bar", "bar/foo");
    expectRejected("foo/bar", "bar/foo");
    expectRejected("foo&bar", "bar/foo&");
    expectRejected("foo", "");
    expectRejected("", "bar");
  }

  private void expectRejected(String owner, String jobName) throws ScheduleException {
    try {
      scheduler.createJob(makeJob(owner, jobName, DEFAULT_TASK, 1));
      fail("Job owner/name should have been rejected.");
    } catch (TaskDescriptionException e) {
      // Expected.
    }
  }

  @Test
  public void testSortableTaskIds() throws Exception {
    expectRestore();

    control.replay();
    buildScheduler();

    int i = 0;
    for (TaskState task : getTasks(Query.GET_ALL)) {
      assertThat(task.task.getAssignedTask().getTask().getOwner(), is(OWNER_A + i));
    }
  }

  @Test(expected = ScheduleException.class)
  public void testCreateDuplicateJob() throws Exception {
    expectRestore();
    expectPersists(1);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1));
    assertTaskCount(1);

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1));
  }

  @Test(expected = ScheduleException.class)
  public void testCreateDuplicateCronJob() throws Exception {
    expectRestore();
    expectPersists(1);

    control.replay();
    buildScheduler();

    // Cron jobs are scheduled on a delay, so this job's tasks will not be scheduled immediately,
    // but duplicate jobs should still be rejected.
    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1)
        .setCronSchedule("1 1 1 1 1"));
    assertTaskCount(0);

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1));
  }

  @Test(expected = TaskDescriptionException.class)
  public void testCreateEmptyJob() throws Exception {
    expectRestore();

    control.replay();
    buildScheduler();

    scheduler.createJob(new JobConfiguration().setOwner(OWNER_A).setName(JOB_A));
  }

  @Test(expected = TaskDescriptionException.class)
  public void testCreateJobMissingShardIds() throws Exception {
    expectRestore();

    control.replay();
    buildScheduler();

    scheduler.createJob(new JobConfiguration().setOwner(OWNER_A).setName(JOB_A).setTaskConfigs(
        ImmutableSet.of(new TwitterTaskInfo(DEFAULT_TASK))));
  }

  @Test(expected = TaskDescriptionException.class)
  public void testCreateJobDuplicateShardIds() throws Exception {
    expectRestore();

    control.replay();
    buildScheduler();

    scheduler.createJob(new JobConfiguration().setOwner(OWNER_A).setName(JOB_A).setTaskConfigs(
        ImmutableSet.of(
            new TwitterTaskInfo(DEFAULT_TASK).setShardId(0).setStartCommand("foo"),
            new TwitterTaskInfo(DEFAULT_TASK).setShardId(0).setStartCommand("bar"))));
  }

  @Test(expected = TaskDescriptionException.class)
  public void testCreateJobShardIdHole() throws Exception {
    expectRestore();

    control.replay();
    buildScheduler();

    scheduler.createJob(new JobConfiguration().setOwner(OWNER_A).setName(JOB_A).setTaskConfigs(
        ImmutableSet.of(
            new TwitterTaskInfo(DEFAULT_TASK).setShardId(0),
            new TwitterTaskInfo(DEFAULT_TASK).setShardId(2))));
  }

  @Test
  public void testHonorsScheduleFilter() throws Exception {
    expectRestore();
    expectPersists(1);
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
    expectRestore();
    expectPersists(5);
    expectKillTask(1);

    control.replay();
    buildScheduler();

    scheduler.registered(FRAMEWORK_ID);
    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1));
    changeStatus(queryByOwner(OWNER_A), STARTING);
    changeStatus(queryByOwner(OWNER_A), RUNNING);

    String taskId = getOnlyTask(queryByOwner(OWNER_A)).task.getAssignedTask().getTaskId();

    Set<String> restartRequest = Sets.newHashSet(taskId);
    String restarted = Iterables.getOnlyElement(scheduler.restartTasks(restartRequest));

    assertThat(restarted, is(taskId));

    // Mimick the master notifying the scheduler of a task state change.
    changeStatus(query(restartRequest), KILLED);

    TaskState restartedTask = getOnlyTask(Query.byStatus(KILLED_BY_CLIENT));
    assertThat(restartedTask.task.getAssignedTask().getTaskId(), is(restarted));

    TaskState newTask = getOnlyTask(Query.byStatus(PENDING));
    assertThat(newTask.task.getAncestorId(), is(restarted));
    assertThat(newTask.task.getAssignedTask().getTask().getShardId(),
        is(restartedTask.task.getAssignedTask().getTask().getShardId()));
  }

  @Test(expected = RestartException.class)
  public void testRestartUnknownTask() throws Exception {
    expectRestore();
    expectPersists(3);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1));
    changeStatus(queryByOwner(OWNER_A), STARTING);
    changeStatus(queryByOwner(OWNER_A), RUNNING);

    String taskId = getOnlyTask(queryByOwner(OWNER_A)).task.getAssignedTask().getTaskId();

    Set<String> restartRequest = Sets.newHashSet(taskId + 1);
    scheduler.restartTasks(restartRequest);
  }

  @Test
  public void testRestartInactiveTask() throws Exception {
    expectRestore();
    expectPersists(4);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1));
    changeStatus(queryByOwner(OWNER_A), STARTING);
    changeStatus(queryByOwner(OWNER_A), RUNNING);
    changeStatus(queryByOwner(OWNER_A), FINISHED);

    String taskId = Iterables.getOnlyElement(Iterables.transform(
        scheduler.getTasks(queryByOwner(OWNER_A)), TaskState.STATE_TO_ID));

    Set<String> restartRequest = Sets.newHashSet(taskId);
    Set<String> restarted = scheduler.restartTasks(restartRequest);

    assertThat(restarted.isEmpty(), is(true));
  }

  @Test
  public void testDaemonTasksRescheduled() throws Exception {
    expectRestore();
    expectPersists(5);

    control.replay();
    buildScheduler();

    // Schedule 5 daemon and 5 non-daemon tasks.
    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 5));
    TwitterTaskInfo task = new TwitterTaskInfo(DEFAULT_TASK);
    task.putToConfiguration("daemon", "true");
    scheduler.createJob(makeJob(OWNER_A, JOB_A + "daemon", task, 5));

    assertThat(getTasksByStatus(PENDING).size(), is(10));

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
    expectRestore();
    expectPersists(5);

    expectKillTask(1);

    control.replay();
    buildScheduler();

    scheduler.registered(FRAMEWORK_ID);
    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1));
    changeStatus(queryByOwner(OWNER_A), STARTING);
    changeStatus(queryByOwner(OWNER_A), RUNNING);
    scheduler.killTasks(queryByOwner(OWNER_A));

    String taskId = getOnlyTask(queryByOwner(OWNER_A)).task.getAssignedTask().getTaskId();

    // This transition should be rejected.
    changeStatus(queryByOwner(OWNER_A), LOST);

    assertThat(getTask(taskId).task.getStatus(), is(KILLED_BY_CLIENT));
  }

  @Test
  public void testFailedTaskIncrementsFailureCount() throws Exception {
    int maxFailures = 5;
    expectRestore();
    expectPersists(10);

    control.replay();
    buildScheduler();

    TwitterTaskInfo task = new TwitterTaskInfo(DEFAULT_TASK);
    task.putToConfiguration("max_task_failures", String.valueOf(maxFailures));
    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 1);
    scheduler.createJob(job);
    assertTaskCount(1);

    Set<TaskState> tasks = scheduler.getTasks(queryJob(OWNER_A, JOB_A));
    assertThat(tasks.size(), is(1));

    for (int i = 0; i < maxFailures - 1; i++) {
      String taskId = TaskState.id(getOnlyTask(Query.activeQuery(OWNER_A, JOB_A)));

      changeStatus(taskId, RUNNING);
      assertThat(getTask(taskId).task.getFailureCount(), is(i));
      changeStatus(taskId, FAILED);

      assertTaskCount(i + 2);

      TaskState rescheduled = getOnlyTask(Query.byStatus(PENDING));
      assertThat(rescheduled.task.getFailureCount(), is(i + 1));
    }

    changeStatus(Query.byStatus(PENDING), FAILED);
    assertThat(getTasksByStatus(FAILED).size(), is(maxFailures));
    assertThat(getTasksByStatus(PENDING).size(), is(0));
  }

  @Test
  public void testCronJobLifeCycle() throws Exception {
    expectRestore();
    expectPersists(5);

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

    changeStatus(queryByOwner(OWNER_A), STARTING);
    assertTaskCount(10);
    changeStatus(queryByOwner(OWNER_A), RUNNING);
    assertTaskCount(10);
    changeStatus(queryByOwner(OWNER_A), FINISHED);
  }

  @Test
  public void testCronNoSuicide() throws Exception {
    expectRestore();
    expectPersists(4);

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

    // Simulate a triggering of the cron job.
    cron.cronTriggered(job);
    assertTaskCount(10);

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
    expectRestore();
    expectPersists(2);

    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1);
    scheduler.createJob(job);
    assertTaskCount(1);

    Set<TaskState> tasks = scheduler.getTasks(queryJob(OWNER_A, JOB_A));
    assertThat(tasks.size(), is(1));

    String taskId = Iterables.get(tasks, 0).task.getAssignedTask().getTaskId();

    scheduler.killTasks(Query.byId(taskId));
    assertTaskCount(0);
  }

  @Test
  public void testKillRunningTask() throws Exception {
    expectRestore();
    expectPersists(4);

    expectKillTask(1);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1));
    String taskId = getOnlyTask(queryByOwner(OWNER_A)).task.getAssignedTask().getTaskId();
    changeStatus(query(taskId), STARTING);
    changeStatus(query(taskId), RUNNING);
    scheduler.killTasks(query(taskId));
    assertThat(getTask(taskId).task.getStatus(), is(KILLED_BY_CLIENT));
    assertThat(getTasks(queryByOwner(OWNER_A)).size(), is(1));
  }

  @Test
  public void testKillCronTask() throws Exception {
    expectRestore();
    expectPersists(2);

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
    expectRestore();
    expectPersists(3);

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

    Query pendingQuery = Query.byStatus(PENDING);
    changeStatus(pendingQuery, LOST);
    assertThat(getOnlyTask(pendingQuery).task.getStatus(), is(PENDING));
    assertTaskCount(2);
    assertThat(scheduler.getTasks(pendingQuery).size(), is(1));

    changeStatus(pendingQuery, LOST);
    assertThat(getOnlyTask(pendingQuery).task.getStatus(), is(PENDING));
    assertTaskCount(3);
    assertThat(scheduler.getTasks(pendingQuery).size(), is(1));
  }

  @Test
  public void testKillJob() throws Exception {
    expectRestore();
    expectPersists(2);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 10));
    assertTaskCount(10);

    scheduler.killTasks(queryJob(OWNER_A, JOB_A));
    assertTaskCount(0);
  }

  @Test
  public void testKillJob2() throws Exception {
    expectRestore();
    expectPersists(3);

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

  @Test
  public void testResourceUpdate() throws Exception {
    expectRestore();
    expectPersists(3);
    expectOffer(true);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1));
    SlaveOffer slaveOffer = createSlaveOffer(SLAVE_ID, SLAVE_HOST_1, 4, 4096);
    scheduler.offer(slaveOffer);

    String taskId = getOnlyTask(queryByOwner(OWNER_A)).task.getAssignedTask().getTaskId();

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
    expectRestore();
    expectPersists(6);
    expectOffer(true);
    expectOffer(true);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 2));
    SlaveOffer slaveOffer = createSlaveOffer(SLAVE_ID, SLAVE_HOST_1, 4, 4096);
    scheduler.offer(slaveOffer);
    scheduler.offer(slaveOffer);

    String taskId1 = Iterables.get(getTasks(queryByOwner(OWNER_A)), 0)
        .task.getAssignedTask().getTaskId();
    String taskId2 = Iterables.get(getTasks(queryByOwner(OWNER_A)), 1)
        .task.getAssignedTask().getTaskId();

    changeStatus(taskId1, RUNNING);
    changeStatus(taskId2, RUNNING);

    // Simulate state update from the executor telling the scheduler that the task is dead.
    // This can happen if the entire cluster goes down - the scheduler has persisted state
    // listing the task as running, and the executor reads the task state in and marks it as KILLED.
    RegisteredTaskUpdate update = new RegisteredTaskUpdate()
        .setSlaveHost(SLAVE_HOST_1);
    update.addToTaskInfos(new LiveTaskInfo().setTaskId(taskId1).setStatus(KILLED));
    update.addToTaskInfos(new LiveTaskInfo().setTaskId(taskId2).setStatus(FINISHED));
    scheduler.updateRegisteredTasks(update);

    // The expected outcome is that one task is rescheduled, and the old task is moved into the
    // KILLED state.  The FINISHED task's state is updated on the scheduler.
    assertTaskCount(3);
    assertThat(getOnlyTask(Query.byId(taskId1)).task.getStatus(), is(KILLED));
    assertThat(getOnlyTask(Query.byId(taskId2)).task.getStatus(), is(FINISHED));

    TaskState rescheduled = Iterables.getOnlyElement(getTasksByStatus(PENDING));
    assertThat(rescheduled.task.getAncestorId(), is(taskId1));
  }

  @Test
  public void testSlaveCannotModifyTasksForOtherSlave() throws Exception {
    expectRestore();
    expectPersists(6);
    expectOffer(true);
    expectOffer(true);

    control.replay();
    buildScheduler();

    SlaveOffer slaveOffer1 = createSlaveOffer(SLAVE_ID, SLAVE_HOST_1, 4, 4096);
    SlaveOffer slaveOffer2 = createSlaveOffer(SLAVE_ID, SLAVE_HOST_2, 4, 4096);

    // Offer resources for the scheduler to accept.
    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1));
    scheduler.offer(slaveOffer1);

    scheduler.createJob(makeJob(OWNER_B, JOB_B, DEFAULT_TASK, 1));
    scheduler.offer(slaveOffer2);

    String taskIdA = Iterables.get(getTasksOwnedBy(OWNER_A), 0).task.getAssignedTask().getTaskId();
    String taskIdB = Iterables.get(getTasksOwnedBy(OWNER_B), 0).task.getAssignedTask().getTaskId();

    changeStatus(taskIdA, RUNNING);
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
    expectRestore();
    expectPersists(12);
    expectOffer(true);
    expectOffer(true);
    expectOffer(true);
    expectOffer(true);

    control.replay();
    buildScheduler();

    SlaveOffer slaveOffer1 = createSlaveOffer(SLAVE_ID, SLAVE_HOST_1, 4, 4096);
    SlaveOffer slaveOffer2 = createSlaveOffer(SLAVE_ID, SLAVE_HOST_2, 4, 4096);

    // Offer resources for the scheduler to accept.
    TwitterTaskInfo daemonTask = new TwitterTaskInfo(DEFAULT_TASK);
    daemonTask.putToConfiguration("daemon", "true");

    scheduler.createJob(makeJob(OWNER_A, JOB_A, daemonTask, 2));
    scheduler.offer(slaveOffer1);
    scheduler.offer(slaveOffer1);

    scheduler.createJob(makeJob(OWNER_B, JOB_B, DEFAULT_TASK, 2));
    scheduler.offer(slaveOffer2);
    scheduler.offer(slaveOffer2);

    String taskIdA = Iterables.get(getTasksOwnedBy(OWNER_A), 0).task.getAssignedTask().getTaskId();
    String taskIdB = Iterables.get(getTasksOwnedBy(OWNER_A), 1).task.getAssignedTask().getTaskId();
    String taskIdC = Iterables.get(getTasksOwnedBy(OWNER_B), 0).task.getAssignedTask().getTaskId();
    String taskIdD = Iterables.get(getTasksOwnedBy(OWNER_B), 1).task.getAssignedTask().getTaskId();

    changeStatus(taskIdA, RUNNING);
    changeStatus(taskIdB, FINISHED);
    assertThat(getTasks(new Query(new TaskQuery().setOwner(OWNER_A).setJobName(JOB_A)
            .setStatuses(EnumSet.of(PENDING)))).size(),
        is(1));

    changeStatus(taskIdC, RUNNING);
    changeStatus(taskIdD, FAILED);

    Function<TaskState, String> getAncestorId = new Function<TaskState, String>() {
      @Override public String apply(TaskState state) { return state.task.getAncestorId(); }
    };

    // Since job A is a daemon, its missing RUNNING task should be rescheduled.
    scheduler.updateRegisteredTasks(new RegisteredTaskUpdate().setSlaveHost(SLAVE_HOST_1)
        .setTaskInfos(Arrays.<LiveTaskInfo>asList()));
    Set<TaskState> rescheduledTasks = getTasks(new Query(new TaskQuery()
        .setOwner(OWNER_A).setJobName(JOB_A).setStatuses(EnumSet.of(PENDING))));
    assertThat(rescheduledTasks.size(), is(2));
    Set<String> rescheduledTaskAncestors = Sets.newHashSet(Iterables.transform(rescheduledTasks,
        getAncestorId));
    assertThat(rescheduledTaskAncestors, is((Set<String>) Sets.newHashSet(taskIdA, taskIdB)));

    // Send an update from host 2 that does not include the FAILED task.
    scheduler.updateRegisteredTasks(new RegisteredTaskUpdate().setSlaveHost(SLAVE_HOST_2)
        .setTaskInfos(Arrays.<LiveTaskInfo>asList()));
    rescheduledTasks = getTasks(new Query(new TaskQuery()
        .setOwner(OWNER_B).setJobName(JOB_B).setStatuses(EnumSet.of(PENDING))));
    assertThat(rescheduledTasks.size(), is(1));
    rescheduledTaskAncestors = Sets.newHashSet(Iterables.transform(rescheduledTasks,
        getAncestorId));
    assertThat(rescheduledTaskAncestors, is((Set<String>) Sets.newHashSet(taskIdC)));

    // This task is not yet removed because we have not met the grace period.
    assertThat(Iterables.isEmpty(getTasks(taskIdD)), is(false));
  }

  @Test
  public void testUpdateJob() throws Exception {
    expectRestore();
    expectPersists(2);
    TwitterTaskInfo task = new TwitterTaskInfo(DEFAULT_TASK);
    task.putToConfiguration("start_command", "echo 'hello'");
    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 1);

    TwitterTaskInfo updatedTask = new TwitterTaskInfo(DEFAULT_TASK);
    updatedTask.putToConfiguration("start_command", "echo 'hi'");
    JobConfiguration updatedJob = makeJob(OWNER_A, JOB_A, updatedTask, 1);

    String updaterHdfsPath = "/fake/updater/hdfs/path";
    String updaterStartCommand = "/bin/do_the_update --token=";
    Capture<String> tokenCapture = new Capture<String>();
    updateTaskBuilder.apply(capture(tokenCapture));
    expectLastCall().andReturn(new TwitterTaskInfo()
        .setShardId(0)
        .setHdfsPath(updaterHdfsPath).setStartCommand(updaterStartCommand));

    control.replay();
    buildScheduler();

    scheduler.createJob(job);

    final String taskId = TaskState.id(getOnlyTask(queryByOwner(OWNER_A)));

    assertThat(scheduler.updateJob(updatedJob), is(JobUpdateResult.UPDATER_LAUNCHED));
    assertTaskCount(2);
    TaskState updaterTask = getOnlyTask(new Query(new TaskQuery(), new Predicate<ScheduledTask>() {
      @Override public boolean apply(ScheduledTask task) {
        return !Tasks.id(task).equals(taskId);
      }
    }));
    assertThat(TaskState.jobKey(updaterTask), is(Tasks.jobKey(OWNER_A, JOB_A + ".updater")));
    TwitterTaskInfo jobUpdater = TaskState.STATE_TO_INFO.apply(updaterTask);
    assertThat(jobUpdater.getHdfsPath(), is(updaterHdfsPath));
    String updateToken = tokenCapture.getValue();
    assertThat(Iterables.getOnlyElement(scheduler.updatesInProgress.keySet()), is(updateToken));
    assertThat(jobUpdater.getStartCommand(), is(updaterStartCommand));
  }

  @Test
  public void testUpdateJobUnchanged() throws Exception {
    expectRestore();
    expectPersists(1);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1));

    assertThat(scheduler.updateJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1)),
        is(JobUpdateResult.JOB_UNCHANGED));
  }

  @Test(expected = ScheduleException.class)
  public void testUpdateNonExistentJob() throws Exception {
    expectRestore();
    expectPersists(1);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1));
    scheduler.updateJob(makeJob(OWNER_B, JOB_A, DEFAULT_TASK, 1));
  }

  @Test
  public void testUpdateJobChangePriority() throws Exception {
    expectRestore();
    expectPersists(3);

    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1);
    scheduler.createJob(job);

    String taskId = getOnlyTask(queryByOwner(OWNER_A)).task.getAssignedTask().getTaskId();

    changeStatus(taskId, RUNNING);

    TwitterTaskInfo updatedTask = new TwitterTaskInfo(DEFAULT_TASK)
        .setShardId(0);
    updatedTask.putToConfiguration("priority", "100");
    assertThat(scheduler.updateJob(makeJob(OWNER_A, JOB_A, updatedTask, 1)),
        is(JobUpdateResult.COMPLETED));

    ScheduledTask task = getTask(taskId).task;
    assertThat(task.getStatus(), is(RUNNING));

    // Need to mimick the configuration parsing.
    TwitterTaskInfo expectedResult = new TwitterTaskInfo(updatedTask);
    ConfigurationManager.populateFields(job, expectedResult);
    assertThat(task.getAssignedTask().getTask(), is(expectedResult));
  }

  private Query byPriority(final int priority) {
    return new Query(new TaskQuery(), new Predicate<ScheduledTask>() {
      @Override public boolean apply(ScheduledTask task) {
        return task.getAssignedTask().getTask().getPriority() == priority;
      }
    });
  }

  @Test
  public void testUpdateJobRemoveTasks() throws Exception {
    expectRestore();
    expectPersists(11);
    expectKillTask(2);

    control.replay();
    buildScheduler();

    scheduler.registered(FRAMEWORK_ID);

    TwitterTaskInfo pending1 = new TwitterTaskInfo(DEFAULT_TASK);
    pending1.putToConfiguration("priority", "1");
    TwitterTaskInfo starting1 = new TwitterTaskInfo(DEFAULT_TASK);
    starting1.putToConfiguration("priority", "2");
    TwitterTaskInfo running1 = new TwitterTaskInfo(DEFAULT_TASK);
    running1.putToConfiguration("priority", "3");
    TwitterTaskInfo finished1 = new TwitterTaskInfo(DEFAULT_TASK);
    finished1.putToConfiguration("priority", "4");
    TwitterTaskInfo killed1 = new TwitterTaskInfo(DEFAULT_TASK);
    killed1.putToConfiguration("priority", "5");
    TwitterTaskInfo pending2 = new TwitterTaskInfo(DEFAULT_TASK);
    pending2.putToConfiguration("priority", "6");
    TwitterTaskInfo starting2 = new TwitterTaskInfo(DEFAULT_TASK);
    starting2.putToConfiguration("priority", "7");
    TwitterTaskInfo running2 = new TwitterTaskInfo(DEFAULT_TASK);
    running2.putToConfiguration("priority", "8");
    TwitterTaskInfo finished2 = new TwitterTaskInfo(DEFAULT_TASK);
    finished2.putToConfiguration("priority", "9");
    TwitterTaskInfo killed2 = new TwitterTaskInfo(DEFAULT_TASK);
    killed2.putToConfiguration("priority", "10");

    JobConfiguration job = makeJob(OWNER_A, JOB_A,
        Arrays.asList(pending1, starting1, running1, finished1, killed1,
            pending2, starting2, running2, finished2, killed2));
    scheduler.createJob(job);

    String pendingId1 = TaskState.STATE_TO_ID.apply(getOnlyTask(byPriority(1)));
    String startingId1 = TaskState.STATE_TO_ID.apply(getOnlyTask(byPriority(2)));
    changeStatus(startingId1, STARTING);
    String runningId1 = TaskState.STATE_TO_ID.apply(getOnlyTask(byPriority(3)));
    changeStatus(runningId1, RUNNING);
    String finishedId1 = TaskState.STATE_TO_ID.apply(getOnlyTask(byPriority(4)));
    changeStatus(finishedId1, FINISHED);
    String killedId1 = TaskState.STATE_TO_ID.apply(getOnlyTask(byPriority(5)));
    scheduler.killTasks(Query.byId(killedId1));
    String pendingId2 = TaskState.STATE_TO_ID.apply(getOnlyTask(byPriority(6)));
    String startingId2 = TaskState.STATE_TO_ID.apply(getOnlyTask(byPriority(7)));
    changeStatus(startingId2, STARTING);
    String runningId2 = TaskState.STATE_TO_ID.apply(getOnlyTask(byPriority(8)));
    changeStatus(runningId2, RUNNING);
    String finishedId2 = TaskState.STATE_TO_ID.apply(getOnlyTask(byPriority(9)));
    changeStatus(finishedId2, FINISHED);
    String killedId2 = TaskState.STATE_TO_ID.apply(getOnlyTask(byPriority(10)));
    scheduler.killTasks(Query.byId(killedId2));

    assertThat(scheduler.updateJob(makeJob(OWNER_A, JOB_A,
        Arrays.asList(pending1, starting1, running1, finished1, killed1))),
        is(JobUpdateResult.COMPLETED));

    // Check that all the tasks ended up in the right states.
    assertThat(getTask(pendingId1).task.getStatus(), is(PENDING));
    assertThat(getTask(startingId1).task.getStatus(), is(STARTING));
    assertThat(getTask(runningId1).task.getStatus(), is(RUNNING));
    assertThat(getTask(finishedId1).task.getStatus(), is(FINISHED));
    assertThat(getTasks(Query.byId(killedId1)).isEmpty(), is(true));
    assertThat(getTasks(Query.byId(pendingId2)).isEmpty(), is(true));
    assertThat(getTask(startingId2).task.getStatus(), is(KILLED_BY_CLIENT));
    assertThat(getTask(runningId2).task.getStatus(), is(KILLED_BY_CLIENT));
    assertThat(getTask(finishedId2).task.getStatus(), is(FINISHED));
    assertThat(getTasks(Query.byId(killedId2)).isEmpty(), is(true));

    // We have 5 active tasks: pending1, starting1, running1, as well as finished1 and killed1
    // which were reincarnated by the update.
    assertThat(scheduler.getTasks(Query.activeQuery(job)).size(), is(5));
  }

  @Test
  public void tetUpdateJobAddTasks() throws Exception {
    expectRestore();
    expectPersists(6);

    control.replay();
    buildScheduler();

    TwitterTaskInfo pending1 = new TwitterTaskInfo(DEFAULT_TASK);
    pending1.putToConfiguration("priority", "1");
    TwitterTaskInfo starting1 = new TwitterTaskInfo(DEFAULT_TASK);
    starting1.putToConfiguration("priority", "2");
    TwitterTaskInfo running1 = new TwitterTaskInfo(DEFAULT_TASK);
    running1.putToConfiguration("priority", "3");
    TwitterTaskInfo finished1 = new TwitterTaskInfo(DEFAULT_TASK);
    finished1.putToConfiguration("priority", "4");
    TwitterTaskInfo killed1 = new TwitterTaskInfo(DEFAULT_TASK);
    killed1.putToConfiguration("priority", "5");
    TwitterTaskInfo pending2 = new TwitterTaskInfo(DEFAULT_TASK);
    pending2.putToConfiguration("priority", "6");

    List<TwitterTaskInfo> tasks = Arrays.asList(pending1, starting1, running1, finished1, killed1);
    JobConfiguration job = makeJob(OWNER_A, JOB_A, tasks);
    scheduler.createJob(job);

    String pendingId1 = TaskState.STATE_TO_ID.apply(getOnlyTask(byPriority(1)));
    String startingId1 = TaskState.STATE_TO_ID.apply(getOnlyTask(byPriority(2)));
    changeStatus(startingId1, STARTING);
    String runningId1 = TaskState.STATE_TO_ID.apply(getOnlyTask(byPriority(3)));
    changeStatus(runningId1, RUNNING);
    String finishedId1 = TaskState.STATE_TO_ID.apply(getOnlyTask(byPriority(4)));
    changeStatus(finishedId1, FINISHED);
    String killedId1 = TaskState.STATE_TO_ID.apply(getOnlyTask(byPriority(5)));
    scheduler.killTasks(Query.byId(killedId1));

    List<TwitterTaskInfo> newTasks = Lists.newArrayList(pending2, pending2, pending2);
    newTasks.addAll(tasks);
    assertThat(scheduler.updateJob(makeJob(OWNER_A, JOB_A, newTasks)),
        is(JobUpdateResult.COMPLETED));

    // Check that all the tasks ended up in the right states.
    assertThat(getTask(pendingId1).task.getStatus(), is(PENDING));
    assertThat(getTask(startingId1).task.getStatus(), is(STARTING));
    assertThat(getTask(runningId1).task.getStatus(), is(RUNNING));
    assertThat(getTask(finishedId1).task.getStatus(), is(FINISHED));
    assertThat(getTasks(Query.byId(killedId1)).isEmpty(), is(true));

    // We have 5 active tasks: pending1, starting1, running1, as well as the newly-pending tasks
    assertThat(scheduler.getTasks(Query.activeQuery(job)).size(), is(8));
  }

  @Test
  public void testUpdateCronJob() throws Exception {
    expectRestore();
    expectPersists(2);

    control.replay();
    buildScheduler();

    String oldCronSchedule = "1 1 1 1 1";
    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1).setCronSchedule(oldCronSchedule));

    JobConfiguration originalJob = Iterables.getOnlyElement(cron.getJobs());

    String newCronSchedule = "* * * * 1";
    assertThat(scheduler.updateJob(
        makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1).setCronSchedule(newCronSchedule)),
        is(JobUpdateResult.COMPLETED));

    JobConfiguration updatedJob = Iterables.getOnlyElement(cron.getJobs());
    assertThat(updatedJob.getCronSchedule(), is(newCronSchedule));
    assertThat(updatedJob.setCronSchedule(oldCronSchedule), is(originalJob));
  }

  @Test
  public void testUpdateCronJobUnchanged() throws Exception {
    expectRestore();
    expectPersists(1);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1).setCronSchedule("1 1 1 1 1"));

    JobConfiguration original = Iterables.getOnlyElement(cron.getJobs());

    assertThat(scheduler.updateJob(makeJob(OWNER_A, JOB_A, DEFAULT_TASK, 1)
        .setCronSchedule("1 1 1 1 1")), is(JobUpdateResult.JOB_UNCHANGED));

    JobConfiguration updatedJob = Iterables.getOnlyElement(cron.getJobs());
    assertThat(updatedJob, is(original));
  }

  @Test(expected = UpdateException.class)
  public void testUpdateShardsRejectsBadToken() throws Exception {
    expectRestore();

    control.replay();
    buildScheduler();
    scheduler.updateShards("asdf", ImmutableSet.of(1, 2, 3), true);
  }

  @Test(expected = UpdateException.class)
  public void testUpdateShardsPreventsUpdateConflict() throws Exception {
    expectRestore();
    expectPersists(1);

    control.replay();
    buildScheduler();

    TwitterTaskInfo task = new TwitterTaskInfo(DEFAULT_TASK);
    task.putToConfiguration("start_command", "echo 'hello'");
    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 1);

    TwitterTaskInfo updatedTask = new TwitterTaskInfo(DEFAULT_TASK);
    updatedTask.putToConfiguration("start_command", "echo 'hi'");
    JobConfiguration updatedJob = makeJob(OWNER_A, JOB_A, updatedTask, 1);

    Map<Integer, TwitterTaskInfo> updateFrom = Tasks.mapInfoByShardId(job.getTaskConfigs());
    Map<Integer, TwitterTaskInfo> updateTo = Tasks.mapInfoByShardId(updatedJob.getTaskConfigs());

    scheduler.createJob(job);

    scheduler.registerUpdate(new UpdateConfig(), updateFrom, updateTo);
    scheduler.registerUpdate(new UpdateConfig(), updateFrom, updateTo);
  }

  @Test
  public void testFinishUpdate() throws Exception {
    expectRestore();

    control.replay();
    buildScheduler();

    TwitterTaskInfo task = new TwitterTaskInfo(DEFAULT_TASK);
    task.putToConfiguration("start_command", "echo 'hello'");
    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 1);

    String newCommand = "echo 'hi'";
    TwitterTaskInfo updatedTask = new TwitterTaskInfo(DEFAULT_TASK);
    updatedTask.putToConfiguration("start_command", newCommand);
    JobConfiguration updatedJob = ConfigurationManager.validateAndPopulate(
        makeJob(OWNER_A, JOB_A, updatedTask, 10));

    Map<Integer, TwitterTaskInfo> updateFrom = Tasks.mapInfoByShardId(
        ConfigurationManager.validateAndPopulate(job).getTaskConfigs());
    Map<Integer, TwitterTaskInfo> updateTo = Tasks.mapInfoByShardId(updatedJob.getTaskConfigs());

    scheduler.registerUpdate(new UpdateConfig(), updateFrom, updateTo);
    scheduler.updateFinished(OWNER_A, JOB_A);

    // Register the update again.  If the previous update was not removed, this will fail.
    scheduler.registerUpdate(new UpdateConfig(), updateFrom, updateTo);
  }

  @Test(expected = UpdateException.class)
  public void testFinishUnrecognizedUpdate() throws Exception {
    expectRestore();

    control.replay();
    buildScheduler();

    scheduler.updateFinished(OWNER_A, JOB_A);
  }

  @Test
  public void testRollingUpdate() throws Exception {
    expectRestore();
    expectPersists(19);
    expectAcceptedOffers(6);
    expectKillTask(3);

    control.replay();
    buildScheduler();

    TwitterTaskInfo task = new TwitterTaskInfo(DEFAULT_TASK);
    task.putToConfiguration("start_command", "echo 'hello'");
    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 10);

    String newCommand = "echo 'hi'";
    TwitterTaskInfo updatedTask = new TwitterTaskInfo(DEFAULT_TASK);
    updatedTask.putToConfiguration("start_command", newCommand);
    JobConfiguration updatedJob = ConfigurationManager.validateAndPopulate(
        makeJob(OWNER_A, JOB_A, updatedTask, 10));

    scheduler.createJob(job);

    Map<Integer, TwitterTaskInfo> updateFrom = Tasks.mapInfoByShardId(
        ConfigurationManager.validateAndPopulate(job).getTaskConfigs());
    Map<Integer, TwitterTaskInfo> updateTo = Tasks.mapInfoByShardId(updatedJob.getTaskConfigs());

    SlaveOffer slaveOffer = createSlaveOffer(SLAVE_ID, SLAVE_HOST_1, 4, 4096);

    // Allow several of the tasks to start.
    assertNotNull(scheduler.offer(slaveOffer));
    assertNotNull(scheduler.offer(slaveOffer));
    assertNotNull(scheduler.offer(slaveOffer));
    assertNotNull(scheduler.offer(slaveOffer));
    assertNotNull(scheduler.offer(slaveOffer));
    assertNotNull(scheduler.offer(slaveOffer));

    Set<TaskState> startingTasks = getTasks(Query.byStatus(STARTING));

    // Move the tasks between states.
    changeStatus(TaskState.STATE_TO_ID.apply(Iterables.get(startingTasks, 0)), RUNNING);
    changeStatus(TaskState.STATE_TO_ID.apply(Iterables.get(startingTasks, 0)), FINISHED);
    changeStatus(TaskState.STATE_TO_ID.apply(Iterables.get(startingTasks, 1)), RUNNING);
    changeStatus(TaskState.STATE_TO_ID.apply(Iterables.get(startingTasks, 2)), RUNNING);
    changeStatus(TaskState.STATE_TO_ID.apply(Iterables.get(startingTasks, 2)), FAILED);
    // This one will be automatically rescheduled.
    changeStatus(TaskState.STATE_TO_ID.apply(Iterables.get(startingTasks, 3)), KILLED);
    changeStatus(TaskState.STATE_TO_ID.apply(Iterables.get(startingTasks, 4)), RUNNING);

    String token = scheduler.registerUpdate(new UpdateConfig(), updateFrom, updateTo);

    expectRestarted(scheduler.updateShards(token, ImmutableSet.of(0, 1), false), newCommand);
    expectRestarted(scheduler.updateShards(token, ImmutableSet.of(2, 3), false), newCommand);
    expectRestarted(scheduler.updateShards(token, ImmutableSet.of(4, 5), false), newCommand);
    expectRestarted(scheduler.updateShards(token, ImmutableSet.of(6, 7), false), newCommand);
    expectRestarted(scheduler.updateShards(token, ImmutableSet.of(8, 9), false), newCommand);

    scheduler.updateFinished(token);

    assertTaskCount(16);  // 10 pending, 3 killed during update, 3 otherwise dead.

    assertThat(getTasksByStatus(PENDING).size(), is(10));
    // Only 2 killed tasks because the rest were pending, or dead.
    assertThat(getTasksByStatus(KILLED_BY_CLIENT).size(), is(3));
  }

  @Test
  public void testRollingUpdateRollback() throws Exception {
    expectRestore();
    expectPersists(29);
    expectAcceptedOffers(10);
    expectKillTask(3);

    control.replay();
    buildScheduler();

    String oldCommand = "echo 'hello'";
    TwitterTaskInfo task = new TwitterTaskInfo(DEFAULT_TASK);
    task.putToConfiguration("start_command", oldCommand);
    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 10);

    String newCommand = "echo 'hi'";
    TwitterTaskInfo updatedTask = new TwitterTaskInfo(DEFAULT_TASK);
    updatedTask.putToConfiguration("start_command", newCommand);
    JobConfiguration updatedJob = ConfigurationManager.validateAndPopulate(
        makeJob(OWNER_A, JOB_A, updatedTask, 10));

    scheduler.createJob(job);

    Map<Integer, TwitterTaskInfo> updateFrom = Tasks.mapInfoByShardId(
        ConfigurationManager.validateAndPopulate(job).getTaskConfigs());
    Map<Integer, TwitterTaskInfo> updateTo = Tasks.mapInfoByShardId(updatedJob.getTaskConfigs());

    SlaveOffer slaveOffer = createSlaveOffer(SLAVE_ID, SLAVE_HOST_1, 4, 4096);
    assertNotNull(scheduler.offer(slaveOffer));
    assertNotNull(scheduler.offer(slaveOffer));
    assertNotNull(scheduler.offer(slaveOffer));
    assertNotNull(scheduler.offer(slaveOffer));
    assertNotNull(scheduler.offer(slaveOffer));
    assertNotNull(scheduler.offer(slaveOffer));
    assertNotNull(scheduler.offer(slaveOffer));
    assertNotNull(scheduler.offer(slaveOffer));
    assertNotNull(scheduler.offer(slaveOffer));
    assertNotNull(scheduler.offer(slaveOffer));

    Map<Integer, TaskState> startingShards = TaskState.mapStateByShardId(
        getTasks(Query.byStatus(STARTING)));

    // Move the tasks between states.
    changeStatus(TaskState.STATE_TO_ID.apply(startingShards.get(0)), RUNNING);
    changeStatus(TaskState.STATE_TO_ID.apply(startingShards.get(0)), FINISHED);
    changeStatus(TaskState.STATE_TO_ID.apply(startingShards.get(1)), RUNNING);
    changeStatus(TaskState.STATE_TO_ID.apply(startingShards.get(2)), RUNNING);
    changeStatus(TaskState.STATE_TO_ID.apply(startingShards.get(2)), FAILED);
    changeStatus(TaskState.STATE_TO_ID.apply(startingShards.get(3)), FINISHED);
    changeStatus(TaskState.STATE_TO_ID.apply(startingShards.get(4)), RUNNING);
    changeStatus(TaskState.STATE_TO_ID.apply(startingShards.get(5)), RUNNING);
    changeStatus(TaskState.STATE_TO_ID.apply(startingShards.get(6)), RUNNING);
    changeStatus(TaskState.STATE_TO_ID.apply(startingShards.get(7)), RUNNING);
    changeStatus(TaskState.STATE_TO_ID.apply(startingShards.get(8)), RUNNING);
    changeStatus(TaskState.STATE_TO_ID.apply(startingShards.get(9)), RUNNING);

    String token = scheduler.registerUpdate(new UpdateConfig(), updateFrom, updateTo);

    // We will roll back before these tasks are updated.
    Set<String> untouchedShardTaskIds = ImmutableSet.copyOf(Iterables.transform(
        getTasks(new Query(new TaskQuery().setShardIds(ImmutableSet.of(6, 7, 8, 9)))),
        TaskState.STATE_TO_ID));

    expectRestarted(scheduler.updateShards(token, ImmutableSet.of(0, 1), false), newCommand);
    expectRestarted(scheduler.updateShards(token, ImmutableSet.of(2, 3), false), newCommand);
    expectRestarted(scheduler.updateShards(token, ImmutableSet.of(4, 5), false), newCommand);

    assertThat(getActiveShard(0).task.getStatus(), is(PENDING));
    assertThat(getActiveShard(1).task.getStatus(), is(PENDING));
    assertThat(getActiveShard(2).task.getStatus(), is(PENDING));
    assertThat(getActiveShard(3).task.getStatus(), is(PENDING));
    assertThat(getActiveShard(4).task.getStatus(), is(PENDING));
    assertThat(getActiveShard(5).task.getStatus(), is(PENDING));

    // Rollback.
    expectRestarted(scheduler.updateShards(token, ImmutableSet.of(0, 1), true), oldCommand);
    expectRestarted(scheduler.updateShards(token, ImmutableSet.of(2, 3), true), oldCommand);
    expectRestarted(scheduler.updateShards(token, ImmutableSet.of(4, 5), true), oldCommand);

    scheduler.updateFinished(token);

    Set<TaskState> untouchedTasks = getTasks(Query.byId(untouchedShardTaskIds));
    assertThat(untouchedTasks.size(), is(untouchedShardTaskIds.size()));
    for (TaskState state : untouchedTasks) {
      assertThat(state.task.getStatus(), is(RUNNING));
    }

    assertThat(getTasks(Query.activeQuery(OWNER_A, JOB_A)).size(), is(10));
  }

  @Test
  public void testRollingUpdateAddingShards() throws Exception {
    expectRestore();
    expectPersists(7);
    expectAcceptedOffers(2);
    expectKillTask(2);

    control.replay();
    buildScheduler();

    String oldCommand = "echo 'hello'";
    TwitterTaskInfo task = new TwitterTaskInfo(DEFAULT_TASK);
    task.putToConfiguration("start_command", oldCommand);
    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 2);

    String newCommand = "echo 'hi'";
    TwitterTaskInfo updatedTask = new TwitterTaskInfo(DEFAULT_TASK);
    updatedTask.putToConfiguration("start_command", newCommand);
    JobConfiguration updatedJob = ConfigurationManager.validateAndPopulate(
        makeJob(OWNER_A, JOB_A, updatedTask, 4));

    scheduler.createJob(job);

    Map<Integer, TwitterTaskInfo> updateFrom = Tasks.mapInfoByShardId(
        ConfigurationManager.validateAndPopulate(job).getTaskConfigs());
    Map<Integer, TwitterTaskInfo> updateTo = Tasks.mapInfoByShardId(updatedJob.getTaskConfigs());

    SlaveOffer slaveOffer = createSlaveOffer(SLAVE_ID, SLAVE_HOST_1, 4, 4096);
    assertNotNull(scheduler.offer(slaveOffer));
    assertNotNull(scheduler.offer(slaveOffer));

    Map<Integer, TaskState> startingShards = TaskState.mapStateByShardId(
        getTasks(Query.byStatus(STARTING)));

    changeStatus(TaskState.STATE_TO_ID.apply(startingShards.get(0)), RUNNING);
    changeStatus(TaskState.STATE_TO_ID.apply(startingShards.get(1)), RUNNING);

    String token = scheduler.registerUpdate(new UpdateConfig(), updateFrom, updateTo);

    expectRestarted(scheduler.updateShards(token, ImmutableSet.of(0, 1), false), newCommand);
    expectRestarted(scheduler.updateShards(token, ImmutableSet.of(2, 3), false), newCommand);

    assertThat(getActiveShard(0).task.getStatus(), is(PENDING));
    assertThat(getActiveShard(1).task.getStatus(), is(PENDING));
    assertThat(getActiveShard(2).task.getStatus(), is(PENDING));
    assertThat(getActiveShard(3).task.getStatus(), is(PENDING));

    scheduler.updateFinished(token);

    assertThat(getTasks(Query.activeQuery(OWNER_A, JOB_A)).size(), is(4));
  }

  @Test
  public void testRollingUpdateAddingShardsRollback() throws Exception {
    expectRestore();
    expectPersists(11);
    expectAcceptedOffers(4);
    expectKillTask(4);

    control.replay();
    buildScheduler();

    String oldCommand = "echo 'hello'";
    TwitterTaskInfo task = new TwitterTaskInfo(DEFAULT_TASK);
    task.putToConfiguration("start_command", oldCommand);
    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 2);

    String newCommand = "echo 'hi'";
    TwitterTaskInfo updatedTask = new TwitterTaskInfo(DEFAULT_TASK);
    updatedTask.putToConfiguration("start_command", newCommand);
    JobConfiguration updatedJob = ConfigurationManager.validateAndPopulate(
        makeJob(OWNER_A, JOB_A, updatedTask, 4));

    scheduler.createJob(job);

    Map<Integer, TwitterTaskInfo> updateFrom = Tasks.mapInfoByShardId(
        ConfigurationManager.validateAndPopulate(job).getTaskConfigs());
    Map<Integer, TwitterTaskInfo> updateTo = Tasks.mapInfoByShardId(updatedJob.getTaskConfigs());

    SlaveOffer slaveOffer = createSlaveOffer(SLAVE_ID, SLAVE_HOST_1, 4, 4096);
    assertNotNull(scheduler.offer(slaveOffer));
    assertNotNull(scheduler.offer(slaveOffer));

    Map<Integer, TaskState> startingShards = TaskState.mapStateByShardId(
        getTasks(Query.byStatus(STARTING)));

    changeStatus(TaskState.STATE_TO_ID.apply(startingShards.get(0)), RUNNING);
    changeStatus(TaskState.STATE_TO_ID.apply(startingShards.get(1)), RUNNING);

    String token = scheduler.registerUpdate(new UpdateConfig(), updateFrom, updateTo);

    expectRestarted(scheduler.updateShards(token, ImmutableSet.of(2, 3), false), newCommand);

    assertNotNull(scheduler.offer(slaveOffer));
    assertNotNull(scheduler.offer(slaveOffer));

    expectRestarted(scheduler.updateShards(token, ImmutableSet.of(0, 1), false), newCommand);
    expectRestarted(scheduler.updateShards(token, ImmutableSet.of(0, 1), true), oldCommand);
    // It is the responsibility of the updater to remove tasks.
    scheduler.killTasks(new Query(new TaskQuery().setShardIds(ImmutableSet.of(2, 3)),
        Tasks.ACTIVE_FILTER));

    assertThat(getActiveShard(0).task.getStatus(), is(PENDING));
    assertThat(getActiveShard(1).task.getStatus(), is(PENDING));

    scheduler.updateFinished(token);

    assertThat(getTasks(Query.activeQuery(OWNER_A, JOB_A)).size(), is(2));
  }

  @Test
  public void testRollingUpdateRemovingShards() throws Exception {
    expectRestore();
    expectPersists(11);
    expectAcceptedOffers(4);
    expectKillTask(4);

    control.replay();
    buildScheduler();

    String oldCommand = "echo 'hello'";
    TwitterTaskInfo task = new TwitterTaskInfo(DEFAULT_TASK);
    task.putToConfiguration("start_command", oldCommand);
    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 4);

    String newCommand = "echo 'hi'";
    TwitterTaskInfo updatedTask = new TwitterTaskInfo(DEFAULT_TASK);
    updatedTask.putToConfiguration("start_command", newCommand);
    JobConfiguration updatedJob = ConfigurationManager.validateAndPopulate(
        makeJob(OWNER_A, JOB_A, updatedTask, 2));

    scheduler.createJob(job);

    Map<Integer, TwitterTaskInfo> updateFrom = Tasks.mapInfoByShardId(
        ConfigurationManager.validateAndPopulate(job).getTaskConfigs());
    Map<Integer, TwitterTaskInfo> updateTo = Tasks.mapInfoByShardId(updatedJob.getTaskConfigs());

    SlaveOffer slaveOffer = createSlaveOffer(SLAVE_ID, SLAVE_HOST_1, 4, 4096);
    assertNotNull(scheduler.offer(slaveOffer));
    assertNotNull(scheduler.offer(slaveOffer));
    assertNotNull(scheduler.offer(slaveOffer));
    assertNotNull(scheduler.offer(slaveOffer));

    Map<Integer, TaskState> startingShards = TaskState.mapStateByShardId(
        getTasks(Query.byStatus(STARTING)));

    changeStatus(TaskState.STATE_TO_ID.apply(startingShards.get(0)), RUNNING);
    changeStatus(TaskState.STATE_TO_ID.apply(startingShards.get(1)), RUNNING);
    changeStatus(TaskState.STATE_TO_ID.apply(startingShards.get(2)), RUNNING);
    changeStatus(TaskState.STATE_TO_ID.apply(startingShards.get(3)), RUNNING);

    String token = scheduler.registerUpdate(new UpdateConfig(), updateFrom, updateTo);

    expectRestarted(scheduler.updateShards(token, ImmutableSet.of(0, 1), false), newCommand);
    // It is the responsibility of the updater to remove tasks.
    scheduler.killTasks(new Query(new TaskQuery().setShardIds(ImmutableSet.of(2, 3)),
        Tasks.ACTIVE_FILTER));

    assertThat(getActiveShard(0).task.getStatus(), is(PENDING));
    assertThat(getActiveShard(1).task.getStatus(), is(PENDING));

    scheduler.updateFinished(token);

    assertThat(getTasks(Query.activeQuery(OWNER_A, JOB_A)).size(), is(2));
  }

  @Test
  public void testRollingUpdateRemovingShardsRollback() throws Exception {
    expectRestore();
    expectPersists(11);
    expectAcceptedOffers(4);
    expectKillTask(4);

    control.replay();
    buildScheduler();

    String oldCommand = "echo 'hello'";
    TwitterTaskInfo task = new TwitterTaskInfo(DEFAULT_TASK);
    task.putToConfiguration("start_command", oldCommand);
    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 4);

    String newCommand = "echo 'hi'";
    TwitterTaskInfo updatedTask = new TwitterTaskInfo(DEFAULT_TASK);
    updatedTask.putToConfiguration("start_command", newCommand);
    JobConfiguration updatedJob = ConfigurationManager.validateAndPopulate(
        makeJob(OWNER_A, JOB_A, updatedTask, 2));

    scheduler.createJob(job);

    Map<Integer, TwitterTaskInfo> updateFrom = Tasks.mapInfoByShardId(
        ConfigurationManager.validateAndPopulate(job).getTaskConfigs());
    Map<Integer, TwitterTaskInfo> updateTo = Tasks.mapInfoByShardId(updatedJob.getTaskConfigs());

    SlaveOffer slaveOffer = createSlaveOffer(SLAVE_ID, SLAVE_HOST_1, 4, 4096);
    assertNotNull(scheduler.offer(slaveOffer));
    assertNotNull(scheduler.offer(slaveOffer));
    assertNotNull(scheduler.offer(slaveOffer));
    assertNotNull(scheduler.offer(slaveOffer));

    Map<Integer, TaskState> startingShards = TaskState.mapStateByShardId(
        getTasks(Query.byStatus(STARTING)));

    changeStatus(TaskState.STATE_TO_ID.apply(startingShards.get(0)), RUNNING);
    changeStatus(TaskState.STATE_TO_ID.apply(startingShards.get(1)), RUNNING);

    String token = scheduler.registerUpdate(new UpdateConfig(), updateFrom, updateTo);

    scheduler.killTasks(new Query(new TaskQuery().setShardIds(ImmutableSet.of(2, 3)),
        Tasks.ACTIVE_FILTER));
    expectRestarted(scheduler.updateShards(token, ImmutableSet.of(0, 1), false), newCommand);
    expectRestarted(scheduler.updateShards(token, ImmutableSet.of(0, 1), true), oldCommand);
    expectRestarted(scheduler.updateShards(token, ImmutableSet.of(2, 3), true), oldCommand);

    assertThat(getActiveShard(0).task.getStatus(), is(PENDING));
    assertThat(getActiveShard(1).task.getStatus(), is(PENDING));
    assertThat(getActiveShard(2).task.getStatus(), is(PENDING));
    assertThat(getActiveShard(3).task.getStatus(), is(PENDING));

    scheduler.updateFinished(token);

    assertThat(getTasks(Query.activeQuery(OWNER_A, JOB_A)).size(), is(4));
  }

  private void expectKillTask(int numTasks) {
    killTask.execute((String) anyObject());
    expectLastCall().times(numTasks);
  }

  private static final Function<TaskState, String> GET_START_COMMAND =
      new Function<TaskState, String>() {
        @Override public String apply(TaskState state) {
          return state.task.getAssignedTask().getTask().getStartCommand();
        }
      };

  private void expectRestarted(Set<String> taskIds, String expectedCommand) {
    Set<TaskState> tasks = getTasks(Query.byId(taskIds));
    Set<String> startCommands = ImmutableSet.copyOf(Iterables.transform(tasks, GET_START_COMMAND));
    Set<String> expectedCommands = ImmutableSet.of(expectedCommand);
    assertThat(startCommands, is(expectedCommands));
    for (TaskState task : tasks) {
      assertThat(task.task.getStatus(), is(PENDING));
    }
  }

  private void assertTaskCount(int numTasks) {
    assertThat(scheduler.getTasks(Query.GET_ALL).size(), is(numTasks));
  }

  private static JobConfiguration makeJob(String owner, String jobName, TwitterTaskInfo task,
      int numTasks) {
    List<TwitterTaskInfo> tasks = Lists.newArrayList();
    for (int i = 0; i < numTasks; i++) {
      tasks.add(new TwitterTaskInfo(task).setOwner(owner).setJobName(jobName));
    }
    return makeJob(owner, jobName, tasks);
  }

  private static JobConfiguration makeJob(String owner, String jobName,
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

  private TaskState getActiveShard(int shardId) {
    return getOnlyTask(new Query(new TaskQuery()
        .setShardIds(ImmutableSet.of(shardId)), Tasks.ACTIVE_FILTER));
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

  private Set<TaskState> getTasksOwnedBy(String owner) {
    return scheduler.getTasks(query(owner, null, null));
  }

  private Query query(Iterable<String> taskIds) {
    return query(null, null, taskIds);
  }

  private Query query(String... taskIds) {
    return query(null, null, ImmutableList.copyOf(taskIds));
  }

  private Query queryByOwner(String owner) {
    return query(owner, null, null);
  }

  private Query queryJob(String owner, String jobName) {
    return query(owner, jobName, null);
  }

  private Query query(String owner, String jobName, Iterable<String> taskIds) {
    TaskQuery query = new TaskQuery();
    if (owner != null) query.setOwner(owner);
    if (jobName != null) query.setJobName(jobName);
    if (taskIds!= null) query.setTaskIds(Sets.newHashSet(taskIds));

    return new Query(query);
  }

  public void changeStatus(Query query, ScheduleStatus status) {
    scheduler.setTaskStatus(query, status, null);
  }

  public void changeStatus(String taskId, ScheduleStatus status) {
    scheduler.setTaskStatus(query(Arrays.asList(taskId)), status, null);
  }

  private void expectAcceptedOffers(int count) {
    for (int i = 0; i < count; i++) expectOffer(true);
  }

  private void expectOffer(boolean passFilter) {
    expect(schedulingFilter.makeFilter((TwitterTaskInfo) anyObject(), (String) anyObject()))
        .andReturn(passFilter ? Predicates.<ScheduledTask>alwaysTrue()
        : Predicates.<ScheduledTask>alwaysFalse());
  }
}

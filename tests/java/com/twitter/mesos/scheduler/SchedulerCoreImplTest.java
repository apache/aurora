package com.twitter.mesos.scheduler;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.twitter.common.base.Closure;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.CronCollisionPolicy;
import com.twitter.mesos.gen.ExecutorStatus;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.SchedulerState;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TrackedTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.persistence.NoPersistence;
import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.Callable;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Unit test for the SchedulerCore.
 *
 * TODO(wfarner): Revamp this class to make use of mocks.
 *
 * TODO(wfarner): Test all the different cases for setTaskStaus:
 *    - Killed tasks get removed.
 *    - Failed tasks have failed count incremented.
 *    - Tasks above maxTaskFailures have _all_ tasks in the job removed.
 *    - Daemon tasks are rescheduled.
 *
 * @author wfarner
 */
public class SchedulerCoreImplTest extends EasyMockTest {

  private WorkQueue workQueue;
  private Driver driver;
  private SchedulerCore scheduler;
  private CronJobManager cron;

  private static final String JOB_NAME_A = "Test_Job_A";
  private static final String JOB_OWNER_A = "Test_Owner_A";
  private static final TwitterTaskInfo TASK_A = defaultTask();

  private static final String JOB_NAME_B = "Test_Job_B";
  private static final String JOB_OWNER_B = "Test_Owner_B";

  private static final String SLAVE_ID = "SlaveId";
  private static final String SLAVE_HOST = "SlaveHost";

  @Before
  public void setUp() {
    workQueue = createMock(WorkQueue.class);
    driver = createMock(Driver.class);

    cron = new CronJobManager();
    ImmediateJobManager immediateJobManager = new ImmediateJobManager();
    scheduler = new SchedulerCoreImpl(cron, immediateJobManager,
        new NoPersistence<SchedulerState>(), new ExecutorTracker() {
          @Override public void start(Closure<String> restartCallback) {
            // No op.
          }
          @Override public void addStatus(ExecutorStatus status) {
            // No-op.
          }
        }, workQueue);
    cron.schedulerCore = scheduler;
    immediateJobManager.schedulerCore = scheduler;
  }

  @Test
  public void testCreateJob() throws Exception {
    control.replay();

    int numTasks = 10;
    JobConfiguration job = makeJob(JOB_OWNER_A, JOB_NAME_A, TASK_A, numTasks);
    scheduler.createJob(job);
    assertTaskCount(numTasks);

    Iterable<TrackedTask> tasks = scheduler.getTasks(
        new TaskQuery().setOwner(JOB_OWNER_A).setJobName(JOB_NAME_A));
    assertThat(Iterables.size(tasks), is(numTasks));
    for (TrackedTask task : tasks) {
      assertThat(task.getStatus(), is(ScheduleStatus.PENDING));
      assertThat(task.isSetTaskId(), is(true));
      assertThat(task.isSetSlaveId(), is(false));
      assertThat(task.getTask(), is(ConfigurationManager.populateFields(job, TASK_A)));
    }
  }

  @Test
  public void testCreateDuplicateJob() throws Exception {
    control.replay();

    scheduler.createJob(makeJob(JOB_OWNER_A, JOB_NAME_A, TASK_A, 1));
    assertTaskCount(1);

    try {
      scheduler.createJob(makeJob(JOB_OWNER_A, JOB_NAME_A, TASK_A, 1));
      fail();
    } catch (ScheduleException e) {
      // Expected
    }

    assertTaskCount(1);
  }

  @Test
  public void testCreateDuplicateCronJob() throws Exception {
    control.replay();

    // Cron jobs are scheduled on a delay, so this job's tasks will not be scheduled immediately,
    // but duplicate jobs should still be rejected.
    scheduler.createJob(makeJob(JOB_OWNER_A, JOB_NAME_A, TASK_A, 1)
        .setCronSchedule("* * * * *"));
    assertTaskCount(0);

    try {
      scheduler.createJob(makeJob(JOB_OWNER_A, JOB_NAME_A, TASK_A, 1));
      fail();
    } catch (ScheduleException e) {
      // Expected
    }

    assertTaskCount(0);
  }

  @Test
  public void testJobLifeCycle() throws Exception {
    control.replay();

    int numTasks = 10;
    scheduler.createJob(makeJob(JOB_OWNER_A, JOB_NAME_A, TASK_A, numTasks));

    assertTaskCount(numTasks);

    /**
     * TODO(wfarner): Complete this once constructing a SlaveOffer object doesn't require swig.
    TaskDescription desc = scheduler.offer(
        makeOffer(SLAVE_ID, 1, ONE_GB.as(Data.BYTES)));
    assertThat(desc, is(not(null)));
    assertThat(desc.getSlaveId(), is(SLAVE_ID));

    TwitterTaskInfo taskInfo = new TwitterTaskInfo();
    new TDeserializer().deserialize(taskInfo, desc.getArg());
    assertThat(taskInfo, is(taskObj));
     */

    // TODO(wfarner): Complete.
  }

  @Test
  public void testRestartTask() throws Exception {
    Capture<Callable<Boolean>> workCapture = new Capture<Callable<Boolean>>();
    workQueue.doWork(capture(workCapture));

    expect(driver.killTask(1)).andReturn(0);

    control.replay();

    scheduler.registered(driver, "");
    scheduler.createJob(makeJob(JOB_OWNER_A, JOB_NAME_A, TASK_A, 1));
    scheduler.setTaskStatus(new TaskQuery().setOwner(JOB_OWNER_A), ScheduleStatus.STARTING);
    scheduler.setTaskStatus(new TaskQuery().setOwner(JOB_OWNER_A), ScheduleStatus.RUNNING);

    int taskId = Iterables.getOnlyElement(Iterables.transform(
        scheduler.getTasks(new TaskQuery().setOwner(JOB_OWNER_A)), Tasks.GET_TASK_ID));

    Set<Integer> restartRequest = Sets.newHashSet(taskId);
    Set<Integer> restarted = scheduler.restartTasks(restartRequest);

    assertThat(restarted, is(restartRequest));

    workCapture.getValue().call();

    // Mimick the master notifying the scheduler of a task state change.
    scheduler.setTaskStatus(new TaskQuery().setTaskIds(restartRequest), ScheduleStatus.KILLED);

    assertThat(Iterables.size(scheduler.getTasks(new TaskQuery().setTaskIds(restartRequest)
        .setStatuses(Sets.newHashSet(ScheduleStatus.KILLED_BY_CLIENT)))),
        is(1));

    assertThat(Iterables.size(scheduler.getTasks(new TaskQuery()
        .setStatuses(Sets.newHashSet(ScheduleStatus.PENDING)))),
        is(1));
  }

  @Test
  public void testRestartUnknownTask() throws Exception {
    control.replay();

    scheduler.createJob(makeJob(JOB_OWNER_A, JOB_NAME_A, TASK_A, 1));
    scheduler.setTaskStatus(new TaskQuery().setOwner(JOB_OWNER_A), ScheduleStatus.STARTING);
    scheduler.setTaskStatus(new TaskQuery().setOwner(JOB_OWNER_A), ScheduleStatus.RUNNING);

    int taskId = Iterables.getOnlyElement(Iterables.transform(
        scheduler.getTasks(new TaskQuery().setOwner(JOB_OWNER_A)), Tasks.GET_TASK_ID));

    Set<Integer> restartRequest = Sets.newHashSet(taskId + 1);
    Set<Integer> restarted = scheduler.restartTasks(restartRequest);

    assertThat(restarted.isEmpty(), is(true));
  }

  @Test
  public void testRestartInactiveTask() throws Exception {
    control.replay();

    scheduler.createJob(makeJob(JOB_OWNER_A, JOB_NAME_A, TASK_A, 1));
    scheduler.setTaskStatus(new TaskQuery().setOwner(JOB_OWNER_A), ScheduleStatus.STARTING);
    scheduler.setTaskStatus(new TaskQuery().setOwner(JOB_OWNER_A), ScheduleStatus.RUNNING);
    scheduler.setTaskStatus(new TaskQuery().setOwner(JOB_OWNER_A), ScheduleStatus.FINISHED);

    int taskId = Iterables.getOnlyElement(Iterables.transform(
        scheduler.getTasks(new TaskQuery().setOwner(JOB_OWNER_A)), Tasks.GET_TASK_ID));

    Set<Integer> restartRequest = Sets.newHashSet(taskId);
    Set<Integer> restarted = scheduler.restartTasks(restartRequest);

    assertThat(restarted.isEmpty(), is(true));
  }

  @Test
  public void testRestartMixedTasks() throws Exception {
    Capture<Callable<Boolean>> workCapture = new Capture<Callable<Boolean>>();
    workQueue.doWork(capture(workCapture));

    expect(driver.killTask(1)).andReturn(0);

    control.replay();

    scheduler.registered(driver, "");
    scheduler.createJob(makeJob(JOB_OWNER_A, JOB_NAME_A, TASK_A, 1));
    scheduler.createJob(makeJob(JOB_OWNER_B, JOB_NAME_B, TASK_A, 1));
    scheduler.setTaskStatus(new TaskQuery().setOwner(JOB_OWNER_A), ScheduleStatus.STARTING);
    scheduler.setTaskStatus(new TaskQuery().setOwner(JOB_OWNER_A), ScheduleStatus.RUNNING);

    scheduler.setTaskStatus(new TaskQuery().setOwner(JOB_OWNER_B), ScheduleStatus.STARTING);
    scheduler.setTaskStatus(new TaskQuery().setOwner(JOB_OWNER_B), ScheduleStatus.RUNNING);
    scheduler.setTaskStatus(new TaskQuery().setOwner(JOB_OWNER_B), ScheduleStatus.FINISHED);

    int activeTaskId = Iterables.getOnlyElement(
        scheduler.getTasks(new TaskQuery().setOwner(JOB_OWNER_A))).getTaskId();
    int inactiveTaskId = Iterables.getOnlyElement(
        scheduler.getTasks(new TaskQuery().setOwner(JOB_OWNER_B))).getTaskId();

    Set<Integer> restartRequest = Sets.newHashSet(activeTaskId, inactiveTaskId, 100000);
    Set<Integer> restarted = scheduler.restartTasks(restartRequest);

    Set<Integer> expectedRestart = Sets.newHashSet(activeTaskId);

    assertThat(restarted, is(expectedRestart));

    workCapture.getValue().call();

    // Mimick the master notifying the scheduler of a task state change.
    scheduler.setTaskStatus(new TaskQuery().setTaskIds(expectedRestart), ScheduleStatus.KILLED);

    assertThat(Iterables.size(scheduler.getTasks(new TaskQuery().setTaskIds(expectedRestart)
        .setStatuses(Sets.newHashSet(ScheduleStatus.KILLED_BY_CLIENT)))),
        is(1));

    assertThat(Iterables.size(scheduler.getTasks(new TaskQuery().setOwner(JOB_OWNER_A)
        .setStatuses(Sets.newHashSet(ScheduleStatus.PENDING)))),
        is(1));
  }

  @Test
  public void testDaemonTasksRescheduled() throws Exception {
    control.replay();

    // Schedule 5 daemon and 5 non-daemon tasks.
    scheduler.createJob(makeJob(JOB_OWNER_A, JOB_NAME_A, TASK_A, 5));
    TwitterTaskInfo task = new TwitterTaskInfo(TASK_A);
    task.putToConfiguration("daemon", "true");
    scheduler.createJob(makeJob(JOB_OWNER_A, JOB_NAME_A + "daemon", task, 5));

    assertThat(Iterables.size(scheduler.getTasks(
        new TaskQuery().setStatuses(Sets.newHashSet(ScheduleStatus.PENDING)))),
        is(10));

    scheduler.setTaskStatus(new TaskQuery().setOwner(JOB_OWNER_A), ScheduleStatus.STARTING);
    assertThat(Iterables.size(scheduler.getTasks(
        new TaskQuery().setStatuses(Sets.newHashSet(ScheduleStatus.STARTING)))),
        is(10));

    scheduler.setTaskStatus(new TaskQuery().setOwner(JOB_OWNER_A), ScheduleStatus.RUNNING);
    assertThat(Iterables.size(scheduler.getTasks(
        new TaskQuery().setStatuses(Sets.newHashSet(ScheduleStatus.RUNNING)))),
        is(10));

    // Daemon tasks will move back into PENDING state after finishing.
    scheduler.setTaskStatus(new TaskQuery().setOwner(JOB_OWNER_A), ScheduleStatus.FINISHED);
    assertThat(Iterables.size(scheduler.getTasks(
        new TaskQuery().setStatuses(Sets.newHashSet(ScheduleStatus.PENDING)))),
        is(5));
    assertThat(Iterables.size(scheduler.getTasks(new TaskQuery().setStatuses(
        Sets.newHashSet(ScheduleStatus.PENDING)))), is(5));
    assertThat(Iterables.size(scheduler.getTasks(new TaskQuery().setStatuses(
        Sets.newHashSet(ScheduleStatus.FINISHED)))), is(10));
  }

  @Test
  public void testFailedTaskIncrementsFailureCount() throws Exception {
    control.replay();

    int maxFailures = 5;
    TwitterTaskInfo task = new TwitterTaskInfo(TASK_A);
    task.putToConfiguration("max_task_failures", String.valueOf(maxFailures));
    JobConfiguration job = makeJob(JOB_OWNER_A, JOB_NAME_A, task, 1);
    scheduler.createJob(job);
    assertTaskCount(1);

    Iterable<TrackedTask> tasks = scheduler.getTasks(
        new TaskQuery().setOwner(JOB_OWNER_A).setJobName(JOB_NAME_A));
    assertThat(Iterables.size(tasks), is(1));

    TaskQuery emptyQuery = new TaskQuery();

    for (int i = 0; i < maxFailures - 1; i++) {
      TaskQuery taskQuery = new TaskQuery().setTaskIds(Sets.newHashSet(i + 1));
      scheduler.setTaskStatus(taskQuery, ScheduleStatus.RUNNING);
      assertThat(Iterables.get(scheduler.getTasks(taskQuery), 0).getFailureCount(), is(i));
      scheduler.setTaskStatus(taskQuery, ScheduleStatus.FAILED);

      assertTaskCount(i + 2);

      TrackedTask rescheduled = Iterables.getOnlyElement(scheduler.getTasks(
          emptyQuery.setStatuses(Sets.newHashSet(ScheduleStatus.PENDING))));
      assertThat(rescheduled.getFailureCount(), is(i + 1));
    }

    scheduler.setTaskStatus(new TaskQuery().setStatuses(Sets.newHashSet(ScheduleStatus.PENDING)),
        ScheduleStatus.FAILED);
    assertThat(Iterables.size(scheduler.getTasks(new TaskQuery().setStatuses(
        Sets.newHashSet(ScheduleStatus.FAILED)))), is(maxFailures));
    assertThat(Iterables.size(scheduler.getTasks(new TaskQuery().setStatuses(
        Sets.newHashSet(ScheduleStatus.PENDING)))), is(0));
  }

  @Test
  public void testCronJobLifeCycle() {
    control.replay();

    // TODO(wfarner): Figure out how to test the lifecycle of a cron job.
  }

  @Test
  public void testCronNoSuicide() throws Exception {
    control.replay();

    JobConfiguration job = makeJob(JOB_OWNER_A, JOB_NAME_A, TASK_A, 10);
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
    assertThat(cron.hasJob(JOB_OWNER_A, JOB_NAME_A), is(true));

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
    assertThat(cron.hasJob(JOB_OWNER_A, JOB_NAME_A), is(true));
  }

  @Test
  public void testKillTask() throws Exception {
    control.replay();

    JobConfiguration job = makeJob(JOB_OWNER_A, JOB_NAME_A, TASK_A, 1);
    scheduler.createJob(job);
    assertTaskCount(1);

    Iterable<TrackedTask> tasks = scheduler.getTasks(
        new TaskQuery().setOwner(JOB_OWNER_A).setJobName(JOB_NAME_A));
    assertThat(Iterables.size(tasks), is(1));

    int taskId = Iterables.get(tasks, 0).getTaskId();

    scheduler.killTasks(new TaskQuery().setTaskIds(Sets.newHashSet(taskId)));
    assertTaskCount(0);
  }

  @Test
  public void testKillCronTask() throws Exception {
    control.replay();

    JobConfiguration job = makeJob(JOB_OWNER_A, JOB_NAME_A, TASK_A, 1);
    job.setCronSchedule("1 1 1 1 1");
    scheduler.createJob(job);

    // This will fail if the cron task could not be found.
    scheduler.killTasks(new TaskQuery().setOwner(JOB_OWNER_A).setJobName(JOB_NAME_A));
  }

  @Test
  public void testLostTaskRescheduled() throws Exception {
    control.replay();

    int maxFailures = 5;
    TwitterTaskInfo task = new TwitterTaskInfo(TASK_A);
    task.putToConfiguration("max_task_failures", String.valueOf(maxFailures));
    JobConfiguration job = makeJob(JOB_OWNER_A, JOB_NAME_A, task, 1);
    scheduler.createJob(job);
    assertTaskCount(1);

    Iterable<TrackedTask> tasks = scheduler.getTasks(
        new TaskQuery().setOwner(JOB_OWNER_A).setJobName(JOB_NAME_A));
    assertThat(Iterables.size(tasks), is(1));

    TaskQuery pendingQuery = new TaskQuery().setStatuses(Sets.newHashSet(ScheduleStatus.PENDING));
    scheduler.setTaskStatus(pendingQuery, ScheduleStatus.LOST);
    assertThat(Iterables.getOnlyElement(scheduler.getTasks(pendingQuery)).getStatus(),
        is(ScheduleStatus.PENDING));
    assertTaskCount(2);
    assertThat(Iterables.size(scheduler.getTasks(pendingQuery)), is(1));

    scheduler.setTaskStatus(pendingQuery, ScheduleStatus.LOST);
    assertThat(Iterables.getOnlyElement(scheduler.getTasks(pendingQuery)).getStatus(),
        is(ScheduleStatus.PENDING));
    assertTaskCount(3);
    assertThat(Iterables.size(scheduler.getTasks(pendingQuery)), is(1));
  }

  @Test
  public void testKillJob() throws Exception {
    control.replay();

    scheduler.createJob(makeJob(JOB_OWNER_A, JOB_NAME_A, TASK_A, 10));
    assertTaskCount(10);

    scheduler.killTasks(new TaskQuery().setOwner(JOB_OWNER_A).setJobName(JOB_NAME_A));
    assertTaskCount(0);
  }

  @Test
  public void testKillJob2() throws Exception {
    control.replay();

    scheduler.createJob(makeJob(JOB_OWNER_A, JOB_NAME_A, TASK_A, 10));
    assertTaskCount(10);

    scheduler.createJob(makeJob(JOB_OWNER_A, JOB_NAME_A + "2", TASK_A, 10));
    assertTaskCount(20);

    scheduler.killTasks(new TaskQuery().setOwner(JOB_OWNER_A).setJobName(JOB_NAME_A + "2"));
    assertTaskCount(10);

    for (TrackedTask task : scheduler.getTasks(new TaskQuery())) {
      assertThat(task.getJobName(), is(JOB_NAME_A));
    }
  }

  private void assertTaskCount(int numTasks) {
    assertThat(Iterables.size(scheduler.getTasks(new TaskQuery())), is(numTasks));
  }

  private static JobConfiguration makeJob(String owner, String jobName, TwitterTaskInfo task,
      int numTasks) {
    JobConfiguration job = new JobConfiguration();
    job.setOwner(owner)
        .setName(jobName);
    for (int i = 0; i < numTasks; i++) {
      job.addToTaskConfigs(new TwitterTaskInfo(task));
    }

    return job;
  }

  private static TwitterTaskInfo defaultTask() {
    return new TwitterTaskInfo().setConfiguration(ImmutableMap.<String, String>builder()
        .put("start_command", "date")
        .put("cpus", "1.0")
        .put("ram_mb", "1024")
        .put("hdfs_path", "/fake/path")
        .build());
  }
}

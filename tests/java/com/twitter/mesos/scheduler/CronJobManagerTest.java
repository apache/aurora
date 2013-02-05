package com.twitter.mesos.scheduler;

import java.util.concurrent.Executor;

import com.google.common.collect.ImmutableSet;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.CronCollisionPolicy;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.CronJobManager.CronScheduler;
import com.twitter.mesos.scheduler.storage.testing.StorageTestUtil;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.fail;

public class CronJobManagerTest extends EasyMockTest {

  private static final String OWNER = "owner";
  private static final String JOB_NAME = "jobName";

  private SchedulerCore scheduler;
  private Executor delayExecutor;
  private Capture<Runnable> delayLaunchCapture;
  private StorageTestUtil storageUtil;

  private CronScheduler cronScheduler;
  private CronJobManager cron;
  private JobConfiguration job;

  @Before
  public void setUp() throws Exception {
    scheduler = createMock(SchedulerCore.class);
    delayExecutor = createMock(Executor.class);
    delayLaunchCapture = createCapture();
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectTransactions();
    cronScheduler = createMock(CronScheduler.class);
    cron = new CronJobManager(storageUtil.storage, cronScheduler, delayExecutor);
    cron.schedulerCore = scheduler;
    job = makeJob();
  }

  private void expectJobAccepted(JobConfiguration savedJob) {
    storageUtil.jobStore.saveAcceptedJob(CronJobManager.MANAGER_KEY, savedJob);
    expect(cronScheduler.schedule(eq(savedJob.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");
  }

  private void expectJobAccepted() {
    expectJobAccepted(job);
  }

  private IExpectationSetters<?> expectJobFetch() {
    return expect(storageUtil.jobStore.fetchJob(CronJobManager.MANAGER_KEY, Tasks.jobKey(job)))
        .andReturn(job);
  }

  private IExpectationSetters<?> expectActiveTaskFetch(ScheduledTask... activeTasks) {
    TaskQuery query = Query.activeQuery(job.getOwner().getRole(), job.getName());
    return storageUtil.expectTaskFetch(query, activeTasks);
  }

  @Test
  public void testStart() throws Exception {
    expectJobAccepted();
    expectJobFetch();
    expectActiveTaskFetch();

    // Job is executed immediately since there are no existing tasks to kill.
    scheduler.runJob(job);

    control.replay();

    cron.receiveJob(job);
    cron.startJobNow(job.getOwner().getRole(), job.getName());
  }

  @Test
  public void testDelayedStart() throws Exception {
    expectJobAccepted();
    expectJobFetch();

    // Query to test if live tasks exist for the job.
    expectActiveTaskFetch(new ScheduledTask());

    // Live tasks exist, so the cron manager must delay the cron launch.
    delayExecutor.execute(capture(delayLaunchCapture));

    // The cron manager will then try to initiate the kill.
    scheduler.killTasks((TaskQuery) anyObject(), eq(CronJobManager.CRON_USER));

    // Immediate query and delayed query.
    expectActiveTaskFetch(new ScheduledTask()).times(2);

    // Simulate the live task disappearing.
    expectActiveTaskFetch();

    scheduler.runJob(job);

    control.replay();

    cron.receiveJob(job);
    cron.startJobNow(job.getOwner().getRole(), job.getName());
    delayLaunchCapture.getValue().run();
  }

  @Test
  public void testDelayedStartResets() throws Exception {
    expectJobAccepted();
    expectJobFetch();

    // Query to test if live tasks exist for the job.
    expectActiveTaskFetch(new ScheduledTask());

    // Live tasks exist, so the cron manager must delay the cron launch.
    delayExecutor.execute(capture(delayLaunchCapture));

    // The cron manager will then try to initiate the kill.
    scheduler.killTasks((TaskQuery) anyObject(), eq(CronJobManager.CRON_USER));

    // Immediate query and delayed query.
    expectActiveTaskFetch(new ScheduledTask()).times(2);

    // Simulate the live task disappearing.
    expectActiveTaskFetch();

    // Round two.
    expectJobFetch();
    expectActiveTaskFetch(new ScheduledTask());
    delayExecutor.execute(capture(delayLaunchCapture));
    scheduler.killTasks((TaskQuery) anyObject(), eq(CronJobManager.CRON_USER));
    expectActiveTaskFetch(new ScheduledTask()).times(2);
    expectActiveTaskFetch();

    scheduler.runJob(job);
    expectLastCall().times(2);

    control.replay();

    cron.receiveJob(job);
    cron.startJobNow(job.getOwner().getRole(), job.getName());
    delayLaunchCapture.getValue().run();

    // Start the job again.  Since the previous delayed start completed, this should repeat the
    // entire process.
    cron.startJobNow(job.getOwner().getRole(), job.getName());
    delayLaunchCapture.getValue().run();
  }

  @Test
  public void testDelayedStartMultiple() throws Exception {
    expectJobAccepted();
    expectJobFetch();

    // Query to test if live tasks exist for the job.
    expectActiveTaskFetch(new ScheduledTask()).times(3);

    // Live tasks exist, so the cron manager must delay the cron launch.
    delayExecutor.execute(capture(delayLaunchCapture));

    // The cron manager will then try to initiate the kill.
    expectJobFetch().times(2);
    scheduler.killTasks((TaskQuery) anyObject(), eq(CronJobManager.CRON_USER));
    expectLastCall().times(3);

    // Immediate queries and delayed query.
    expectActiveTaskFetch(new ScheduledTask()).times(4);

    // Simulate the live task disappearing.
    expectActiveTaskFetch();

    scheduler.runJob(job);

    control.replay();

    cron.receiveJob(job);

    // Attempt to trick the cron manager into launching multiple times, or launching multiple
    // pollers.
    cron.startJobNow(job.getOwner().getRole(), job.getName());
    cron.startJobNow(job.getOwner().getRole(), job.getName());
    cron.startJobNow(job.getOwner().getRole(), job.getName());
    delayLaunchCapture.getValue().run();
  }

  @Test
  public void testUpdate() throws Exception {
    JobConfiguration updated = makeJob().setCronSchedule("1 2 3 4 5");

    expectJobAccepted();
    cronScheduler.deschedule("key");
    expectJobAccepted(updated);

    control.replay();

    cron.receiveJob(job);
    cron.updateJob(updated);
  }

  @Test
  public void testRunOverlapNoShardCollision() throws Exception {
    TwitterTaskInfo task = new TwitterTaskInfo().setShardId(0);
    ScheduledTask scheduledTask = new ScheduledTask()
        .setStatus(ScheduleStatus.RUNNING)
        .setAssignedTask(new AssignedTask().setTask(task));

    job = makeJob()
        .setTaskConfigs(ImmutableSet.of(task))
        .setCronCollisionPolicy(CronCollisionPolicy.RUN_OVERLAP);
    expectJobAccepted();
    expectJobFetch();
    expectActiveTaskFetch(scheduledTask);

    JobConfiguration shardAdjusted = job.deepCopy()
        .setTaskConfigs(ImmutableSet.of(task.deepCopy().setShardId(1)));
    scheduler.runJob(shardAdjusted);

    control.replay();

    cron.receiveJob(job);
    cron.startJobNow(job.getOwner().getRole(), job.getName());
  }

  @Test
  public void testConsistentState() throws Exception {
    JobConfiguration updated = makeJob().setCronSchedule("1 2 3 4 5");

    expectJobAccepted();
    cronScheduler.deschedule("key");
    expectJobAccepted(updated);

    control.replay();

    cron.receiveJob(job);

    JobConfiguration failedUpdate = updated.deepCopy();
    failedUpdate.unsetCronSchedule();
    try {
      cron.updateJob(failedUpdate);
      fail();
    } catch (ScheduleException e) {
      // Expected.
    }

    cron.updateJob(updated);
  }

  private JobConfiguration makeJob() {
    return new JobConfiguration()
        .setOwner(new Identity().setRole(OWNER))
        .setName(JOB_NAME)
        .setCronSchedule("1 1 1 1 1");
  }
}

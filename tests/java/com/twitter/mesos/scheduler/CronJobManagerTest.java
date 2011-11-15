package com.twitter.mesos.scheduler;

import java.util.Set;
import java.util.concurrent.Executor;

import com.google.common.collect.ImmutableSet;

import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.testing.TearDownRegistry;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.scheduler.db.testing.DbStorageTestUtil;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.Work;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;

/**
 * @author William Farner
 */
public class CronJobManagerTest extends EasyMockTest {

  private static final String OWNER = "owner";
  private static final String JOB_NAME = "jobName";

  private SchedulerCore scheduler;
  private Executor delayExecutor;
  private Capture<Runnable> delayLaunchCapture;

  private CronJobManager cron;

  @Before
  public void setUp() throws Exception {
    scheduler = createMock(SchedulerCore.class);
    delayExecutor = createMock(Executor.class);
    delayLaunchCapture = createCapture();
    Storage storage = DbStorageTestUtil.setupStorage(this);
    storage.prepare();
    storage.start(Work.NOOP);
    cron = new CronJobManager(storage, new TearDownRegistry(this), delayExecutor);
    cron.schedulerCore = scheduler;
  }

  @Test
  public void testStart() throws Exception {
    expect(scheduler.getTasks((Query) anyObject())).andReturn(ImmutableSet.<ScheduledTask>of());

    JobConfiguration job = makeJob();
    // Job is executed immediately since there are no existing tasks to kill.
    scheduler.runJob(job);

    control.replay();

    cron.receiveJob(job);
    cron.startJobNow(Tasks.jobKey(job));
  }

  @Test
  public void testDelayedStart() throws Exception {
    Set<ScheduledTask> oneTask = ImmutableSet.of(new ScheduledTask());

    // Query to test if live tasks exist for the job.
    expect(scheduler.getTasks((Query) anyObject())).andReturn(oneTask);

    // Live tasks exist, so the cron manager must delay the cron launch.
    delayExecutor.execute(capture(delayLaunchCapture));

    // The cron manager will then try to initiate the kill.
    scheduler.killTasks((Query) anyObject(), eq(CronJobManager.CRON_USER));

    // Immediate query and delayed query.
    expect(scheduler.getTasks((Query) anyObject())).andReturn(oneTask).times(2);

    // Simulate the live task disappearing.
    expect(scheduler.getTasks((Query) anyObject())).andReturn(ImmutableSet.<ScheduledTask>of());

    JobConfiguration job = makeJob();

    scheduler.runJob(job);

    control.replay();

    cron.receiveJob(job);
    cron.startJobNow(Tasks.jobKey(job));
    delayLaunchCapture.getValue().run();
  }

  @Test
  public void testDelayedStartResets() throws Exception {
    Set<ScheduledTask> oneTask = ImmutableSet.of(new ScheduledTask());

    // Query to test if live tasks exist for the job.
    expect(scheduler.getTasks((Query) anyObject())).andReturn(oneTask);

    // Live tasks exist, so the cron manager must delay the cron launch.
    delayExecutor.execute(capture(delayLaunchCapture));

    // The cron manager will then try to initiate the kill.
    scheduler.killTasks((Query) anyObject(), eq(CronJobManager.CRON_USER));

    // Immediate query and delayed query.
    expect(scheduler.getTasks((Query) anyObject())).andReturn(oneTask).times(2);

    // Simulate the live task disappearing.
    expect(scheduler.getTasks((Query) anyObject())).andReturn(ImmutableSet.<ScheduledTask>of());

    // Round two.
    expect(scheduler.getTasks((Query) anyObject())).andReturn(oneTask);
    delayExecutor.execute(capture(delayLaunchCapture));
    scheduler.killTasks((Query) anyObject(), eq(CronJobManager.CRON_USER));
    expect(scheduler.getTasks((Query) anyObject())).andReturn(oneTask).times(2);
    expect(scheduler.getTasks((Query) anyObject())).andReturn(ImmutableSet.<ScheduledTask>of());

    JobConfiguration job = makeJob();

    scheduler.runJob(job);

    scheduler.runJob(job);

    control.replay();

    cron.receiveJob(job);
    cron.startJobNow(Tasks.jobKey(job));
    delayLaunchCapture.getValue().run();

    // Start the job again.  Since the previous delayed start completed, this should repeat the
    // entire process.
    cron.startJobNow(Tasks.jobKey(job));
    delayLaunchCapture.getValue().run();
  }

  @Test
  public void testDelayedStartMultiple() throws Exception {
    Set<ScheduledTask> oneTask = ImmutableSet.of(new ScheduledTask());

    // Query to test if live tasks exist for the job.
    expect(scheduler.getTasks((Query) anyObject())).andReturn(oneTask).times(3);

    // Live tasks exist, so the cron manager must delay the cron launch.
    delayExecutor.execute(capture(delayLaunchCapture));

    // The cron manager will then try to initiate the kill.
    scheduler.killTasks((Query) anyObject(), eq(CronJobManager.CRON_USER));
    expectLastCall().times(3);

    // Immediate queries and delayed query.
    expect(scheduler.getTasks((Query) anyObject())).andReturn(oneTask).times(4);

    // Simulate the live task disappearing.
    expect(scheduler.getTasks((Query) anyObject())).andReturn(ImmutableSet.<ScheduledTask>of());

    JobConfiguration job = makeJob();

    scheduler.runJob(job);

    control.replay();

    cron.receiveJob(job);

    // Attempt to trick the cron manager into launching multiple times, or launching multiple
    // pollers.
    cron.startJobNow(Tasks.jobKey(job));
    cron.startJobNow(Tasks.jobKey(job));
    cron.startJobNow(Tasks.jobKey(job));
    delayLaunchCapture.getValue().run();
  }

  private JobConfiguration makeJob() {
    return new JobConfiguration()
        .setOwner(new Identity().setRole(OWNER))
        .setName(JOB_NAME)
        .setCronSchedule("1 1 1 1 1");
  }
}

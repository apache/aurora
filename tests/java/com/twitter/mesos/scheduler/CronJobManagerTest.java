package com.twitter.mesos.scheduler;

import java.util.Set;
import java.util.concurrent.Executor;

import com.google.common.collect.ImmutableSet;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.scheduler.CronJobManager.CronScheduler;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;
import com.twitter.mesos.scheduler.storage.mem.MemStorage;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;

public class CronJobManagerTest extends EasyMockTest {

  private static final String OWNER = "owner";
  private static final String JOB_NAME = "jobName";

  private SchedulerCore scheduler;
  private Executor delayExecutor;
  private Capture<Runnable> delayLaunchCapture;

  private CronScheduler cronScheduler;
  private CronJobManager cron;

  @Before
  public void setUp() throws Exception {
    scheduler = createMock(SchedulerCore.class);
    delayExecutor = createMock(Executor.class);
    delayLaunchCapture = createCapture();
    Storage storage = new MemStorage();
    storage.prepare();
    storage.start(MutateWork.NOOP);
    cronScheduler = createMock(CronScheduler.class);
    cron = new CronJobManager(storage, cronScheduler, delayExecutor);
    cron.schedulerCore = scheduler;
  }

  @Test
  public void testStart() throws Exception {
    JobConfiguration job = makeJob();

    expect(scheduler.getTasks((TaskQuery) anyObject())).andReturn(ImmutableSet.<ScheduledTask>of());
    expect(cronScheduler.schedule(eq(job.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");


    // Job is executed immediately since there are no existing tasks to kill.
    scheduler.runJob(job);

    control.replay();

    cron.receiveJob(job);
    cron.startJobNow(Tasks.jobKey(job));
  }

  @Test
  public void testDelayedStart() throws Exception {
    JobConfiguration job = makeJob();
    Set<ScheduledTask> oneTask = ImmutableSet.of(new ScheduledTask());

    expect(cronScheduler.schedule(eq(job.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");

    // Query to test if live tasks exist for the job.
    expect(scheduler.getTasks((TaskQuery) anyObject())).andReturn(oneTask);

    // Live tasks exist, so the cron manager must delay the cron launch.
    delayExecutor.execute(capture(delayLaunchCapture));

    // The cron manager will then try to initiate the kill.
    scheduler.killTasks((TaskQuery) anyObject(), eq(CronJobManager.CRON_USER));

    // Immediate query and delayed query.
    expect(scheduler.getTasks((TaskQuery) anyObject())).andReturn(oneTask).times(2);

    // Simulate the live task disappearing.
    expect(scheduler.getTasks((TaskQuery) anyObject())).andReturn(ImmutableSet.<ScheduledTask>of());

    scheduler.runJob(job);

    control.replay();

    cron.receiveJob(job);
    cron.startJobNow(Tasks.jobKey(job));
    delayLaunchCapture.getValue().run();
  }

  @Test
  public void testDelayedStartResets() throws Exception {
    JobConfiguration job = makeJob();
    Set<ScheduledTask> oneTask = ImmutableSet.of(new ScheduledTask());

    expect(cronScheduler.schedule(eq(job.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");

    // Query to test if live tasks exist for the job.
    expect(scheduler.getTasks((TaskQuery) anyObject())).andReturn(oneTask);

    // Live tasks exist, so the cron manager must delay the cron launch.
    delayExecutor.execute(capture(delayLaunchCapture));

    // The cron manager will then try to initiate the kill.
    scheduler.killTasks((TaskQuery) anyObject(), eq(CronJobManager.CRON_USER));

    // Immediate query and delayed query.
    expect(scheduler.getTasks((TaskQuery) anyObject())).andReturn(oneTask).times(2);

    // Simulate the live task disappearing.
    expect(scheduler.getTasks((TaskQuery) anyObject())).andReturn(ImmutableSet.<ScheduledTask>of());

    // Round two.
    expect(scheduler.getTasks((TaskQuery) anyObject())).andReturn(oneTask);
    delayExecutor.execute(capture(delayLaunchCapture));
    scheduler.killTasks((TaskQuery) anyObject(), eq(CronJobManager.CRON_USER));
    expect(scheduler.getTasks((TaskQuery) anyObject())).andReturn(oneTask).times(2);
    expect(scheduler.getTasks((TaskQuery) anyObject())).andReturn(ImmutableSet.<ScheduledTask>of());

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
    JobConfiguration job = makeJob();
    Set<ScheduledTask> oneTask = ImmutableSet.of(new ScheduledTask());

    expect(cronScheduler.schedule(eq(job.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");

    // Query to test if live tasks exist for the job.
    expect(scheduler.getTasks((TaskQuery) anyObject())).andReturn(oneTask).times(3);

    // Live tasks exist, so the cron manager must delay the cron launch.
    delayExecutor.execute(capture(delayLaunchCapture));

    // The cron manager will then try to initiate the kill.
    scheduler.killTasks((TaskQuery) anyObject(), eq(CronJobManager.CRON_USER));
    expectLastCall().times(3);

    // Immediate queries and delayed query.
    expect(scheduler.getTasks((TaskQuery) anyObject())).andReturn(oneTask).times(4);

    // Simulate the live task disappearing.
    expect(scheduler.getTasks((TaskQuery) anyObject())).andReturn(ImmutableSet.<ScheduledTask>of());

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

  @Test
  public void testUpdate() throws Exception {
    JobConfiguration job = makeJob();
    JobConfiguration updated = makeJob().setCronSchedule("1 2 3 4 5");

    expect(cronScheduler.schedule(eq(job.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");
    cronScheduler.deschedule("key");
    expect(cronScheduler.schedule(eq(updated.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key2");

    control.replay();

    cron.receiveJob(job);
    cron.updateJob(updated);
  }

  private JobConfiguration makeJob() {
    return new JobConfiguration()
        .setOwner(new Identity().setRole(OWNER))
        .setName(JOB_NAME)
        .setCronSchedule("1 1 1 1 1");
  }
}

package com.twitter.mesos.scheduler;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.scheduler.db.testing.DbStorageTestUtil;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;

/**
 * @author William Farner
 */
public class CronJobManagerTest extends EasyMockTest {

  private static final String OWNER = "owner";
  private static final String JOB_NAME = "jobName";

  private SchedulerCore scheduler;

  private CronJobManager cron;

  @Before
  public void setUp() throws Exception {
    scheduler = createMock(SchedulerCore.class);
    Storage storage = DbStorageTestUtil.setupStorage(this);
    storage.prepare();
    storage.start(Work.NOOP);
    cron = new CronJobManager(storage);
    cron.schedulerCore = scheduler;
  }

  @Test
  public void testDelayedStart() throws Exception {
    Set<ScheduledTask> oneTask = ImmutableSet.of(new ScheduledTask());

    // Query to test if live tasks exist for the job.
    expect(scheduler.getTasks((Query) anyObject())).andReturn(oneTask);

    // The cron manager will then try to initiate the kill.
    scheduler.killTasks((Query) anyObject(), eq(CronJobManager.CRON_USER));

    // Immediate query and delayed query.
    expect(scheduler.getTasks((Query) anyObject())).andReturn(oneTask).times(2);

    // Simulate the live task disappearing.
    expect(scheduler.getTasks((Query) anyObject())).andReturn(ImmutableSet.<ScheduledTask>of());

    JobConfiguration job = makeJob();

    final CountDownLatch jobLaunched = new CountDownLatch(1);
    scheduler.runJob(job);
    expectLastCall().andAnswer(new IAnswer<Object>() {
      @Override public Object answer() {
        jobLaunched.countDown();
        return null;
      }
    });

    control.replay();

    cron.receiveJob(job);
    cron.startJobNow(Tasks.jobKey(job));

    jobLaunched.await(10, TimeUnit.SECONDS);
    assertEquals("Job not launched.", 0, jobLaunched.getCount());
  }

  @Test
  public void testDelayedStartResets() throws Exception {
    Set<ScheduledTask> oneTask = ImmutableSet.of(new ScheduledTask());

    // Query to test if live tasks exist for the job.
    expect(scheduler.getTasks((Query) anyObject())).andReturn(oneTask);

    // The cron manager will then try to initiate the kill.
    scheduler.killTasks((Query) anyObject(), eq(CronJobManager.CRON_USER));

    // Immediate query and delayed query.
    expect(scheduler.getTasks((Query) anyObject())).andReturn(oneTask).times(2);

    // Simulate the live task disappearing.
    expect(scheduler.getTasks((Query) anyObject())).andReturn(ImmutableSet.<ScheduledTask>of());

    // Round two.
    expect(scheduler.getTasks((Query) anyObject())).andReturn(oneTask);
    scheduler.killTasks((Query) anyObject(), eq(CronJobManager.CRON_USER));
    expect(scheduler.getTasks((Query) anyObject())).andReturn(oneTask).times(2);
    expect(scheduler.getTasks((Query) anyObject())).andReturn(ImmutableSet.<ScheduledTask>of());

    JobConfiguration job = makeJob();

    final CountDownLatch jobLaunched = new CountDownLatch(1);
    scheduler.runJob(job);
    expectLastCall().andAnswer(new IAnswer<Object>() {
      @Override public Object answer() {
        jobLaunched.countDown();
        return null;
      }
    });

    final CountDownLatch jobReLaunched = new CountDownLatch(1);
    scheduler.runJob(job);
    expectLastCall().andAnswer(new IAnswer<Object>() {
      @Override public Object answer() {
        jobReLaunched.countDown();
        return null;
      }
    });

    control.replay();

    cron.receiveJob(job);
    cron.startJobNow(Tasks.jobKey(job));

    jobLaunched.await(10, TimeUnit.SECONDS);
    assertEquals("Job not launched.", 0, jobLaunched.getCount());

    // Start the job again, which should reset the cycle.
    cron.startJobNow(Tasks.jobKey(job));

    jobReLaunched.await(10, TimeUnit.SECONDS);
    assertEquals("Job not relaunched.", 0, jobReLaunched.getCount());
  }

  @Test
  public void testDelayedStartMultiple() throws Exception {
    Set<ScheduledTask> oneTask = ImmutableSet.of(new ScheduledTask());

    // Query to test if live tasks exist for the job.
    expect(scheduler.getTasks((Query) anyObject())).andReturn(oneTask).times(3);

    // The cron manager will then try to initiate the kill.
    scheduler.killTasks((Query) anyObject(), eq(CronJobManager.CRON_USER));
    expectLastCall().times(2);

    // Immediate query and delayed query.
    expect(scheduler.getTasks((Query) anyObject())).andReturn(oneTask).times(2);

    // Simulate the live task disappearing.
    expect(scheduler.getTasks((Query) anyObject())).andReturn(ImmutableSet.<ScheduledTask>of());

    JobConfiguration job = makeJob();

    final CountDownLatch jobLaunched = new CountDownLatch(1);
    scheduler.runJob(job);
    expectLastCall().andAnswer(new IAnswer<Object>() {
      @Override public Object answer() {
        jobLaunched.countDown();
        return null;
      }
    });

    control.replay();

    cron.receiveJob(job);

    // Attempt to trick the cron manager into launching multiple times, or launching multiple
    // pollers.
    cron.startJobNow(Tasks.jobKey(job));
    cron.startJobNow(Tasks.jobKey(job));
    cron.startJobNow(Tasks.jobKey(job));

    jobLaunched.await(10, TimeUnit.SECONDS);
    assertEquals("Job not launched.", 0, jobLaunched.getCount());
  }

  private JobConfiguration makeJob() {
    return new JobConfiguration()
        .setOwner(new Identity().setRole(OWNER))
        .setName(JOB_NAME)
        .setCronSchedule("1 1 1 1 1");
  }
}

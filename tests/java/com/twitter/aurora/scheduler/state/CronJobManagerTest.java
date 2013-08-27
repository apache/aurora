package com.twitter.aurora.scheduler.state;

import java.util.concurrent.Executor;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.gen.AssignedTask;
import com.twitter.aurora.gen.Identity;
import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.ScheduleException;
import com.twitter.aurora.scheduler.configuration.ParsedConfiguration;
import com.twitter.aurora.scheduler.cron.CronScheduler;
import com.twitter.aurora.scheduler.storage.testing.StorageTestUtil;
import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.testing.easymock.EasyMockTest;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.fail;

import static com.twitter.aurora.gen.Constants.DEFAULT_ENVIRONMENT;
import static com.twitter.aurora.gen.CronCollisionPolicy.RUN_OVERLAP;

public class CronJobManagerTest extends EasyMockTest {

  private static final String OWNER = "owner";
  private static final String ENVIRONMENT = "staging11";
  private static final String JOB_NAME = "jobName";

  private SchedulerCore scheduler;
  private StateManagerImpl stateManager;
  private Executor delayExecutor;
  private Capture<Runnable> delayLaunchCapture;
  private StorageTestUtil storageUtil;

  private CronScheduler cronScheduler;
  private ShutdownRegistry shutdownRegistry;
  private CronJobManager cron;
  private JobConfiguration job;
  private ParsedConfiguration parsedConfiguration;

  @Before
  public void setUp() throws Exception {
    scheduler = createMock(SchedulerCore.class);
    stateManager = createMock(StateManagerImpl.class);
    delayExecutor = createMock(Executor.class);
    delayLaunchCapture = createCapture();
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    cronScheduler = createMock(CronScheduler.class);
    shutdownRegistry = createMock(ShutdownRegistry.class);
    cron = new CronJobManager(
        stateManager,
        storageUtil.storage,
        cronScheduler,
        shutdownRegistry,
        delayExecutor);
    cron.schedulerCore = scheduler;
    job = makeJob();
    parsedConfiguration = ParsedConfiguration.fromUnparsed(job);
  }

  private void expectJobAccepted(JobConfiguration savedJob) throws Exception {
    storageUtil.jobStore.saveAcceptedJob(CronJobManager.MANAGER_KEY, savedJob);
    expect(cronScheduler.isValidSchedule(savedJob.getCronSchedule())).andReturn(true);
    expect(cronScheduler.schedule(eq(savedJob.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("key");
  }

  private void expectJobAccepted() throws Exception {
    expectJobAccepted(parsedConfiguration.getJobConfig());
  }

  private IExpectationSetters<?> expectJobFetch() {
    return expect(storageUtil.jobStore.fetchJob(CronJobManager.MANAGER_KEY, job.getKey()))
        .andReturn(Optional.of(parsedConfiguration.getJobConfig()));
  }

  private IExpectationSetters<?> expectActiveTaskFetch(ScheduledTask... activeTasks) {
    return storageUtil.expectTaskFetch(Query.jobScoped(job.getKey()).active(), activeTasks);
  }

  @Test
  public void testStart() throws Exception {
    expectJobAccepted();
    expectJobFetch();
    expectActiveTaskFetch();

    // Job is executed immediately since there are no existing tasks to kill.
    stateManager.insertPendingTasks(parsedConfiguration.getTaskConfigs());

    control.replay();

    cron.receiveJob(parsedConfiguration);
    cron.startJobNow(job.getKey());
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
    scheduler.killTasks((Query.Builder) anyObject(), eq(CronJobManager.CRON_USER));

    // Immediate query and delayed query.
    expectActiveTaskFetch(new ScheduledTask()).times(2);

    // Simulate the live task disappearing.
    expectActiveTaskFetch();

    stateManager.insertPendingTasks(parsedConfiguration.getTaskConfigs());

    control.replay();

    cron.receiveJob(parsedConfiguration);
    cron.startJobNow(job.getKey());
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
    scheduler.killTasks((Query.Builder) anyObject(), eq(CronJobManager.CRON_USER));

    // Immediate query and delayed query.
    expectActiveTaskFetch(new ScheduledTask()).times(2);

    // Simulate the live task disappearing.
    expectActiveTaskFetch();

    // Round two.
    expectJobFetch();
    expectActiveTaskFetch(new ScheduledTask());
    delayExecutor.execute(capture(delayLaunchCapture));
    scheduler.killTasks((Query.Builder) anyObject(), eq(CronJobManager.CRON_USER));
    expectActiveTaskFetch(new ScheduledTask()).times(2);
    expectActiveTaskFetch();

    stateManager.insertPendingTasks(parsedConfiguration.getTaskConfigs());
    expectLastCall().times(2);

    control.replay();

    cron.receiveJob(parsedConfiguration);
    cron.startJobNow(job.getKey());
    delayLaunchCapture.getValue().run();

    // Start the job again.  Since the previous delayed start completed, this should repeat the
    // entire process.
    cron.startJobNow(job.getKey());
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
    scheduler.killTasks((Query.Builder) anyObject(), eq(CronJobManager.CRON_USER));
    expectLastCall().times(3);

    // Immediate queries and delayed query.
    expectActiveTaskFetch(new ScheduledTask()).times(4);

    // Simulate the live task disappearing.
    expectActiveTaskFetch();

    stateManager.insertPendingTasks(parsedConfiguration.getTaskConfigs());

    control.replay();

    cron.receiveJob(parsedConfiguration);

    // Attempt to trick the cron manager into launching multiple times, or launching multiple
    // pollers.
    cron.startJobNow(job.getKey());
    cron.startJobNow(job.getKey());
    cron.startJobNow(job.getKey());
    delayLaunchCapture.getValue().run();
  }

  @Test
  public void testUpdate() throws Exception {
    ParsedConfiguration updated = new ParsedConfiguration(job.setCronSchedule("1 2 3 4 5"));

    expectJobAccepted();
    cronScheduler.deschedule("key");
    expectJobAccepted(updated.getJobConfig());

    control.replay();

    cron.receiveJob(parsedConfiguration);
    cron.updateJob(updated);
  }

  @Test
  public void testRunOverlapNoShardCollision() throws Exception {
    ScheduledTask scheduledTask = new ScheduledTask()
        .setStatus(ScheduleStatus.RUNNING)
        .setAssignedTask(new AssignedTask().setTask(defaultTask()));
    parsedConfiguration =
        ParsedConfiguration.fromUnparsed(job.deepCopy().setCronCollisionPolicy(RUN_OVERLAP));
    expectJobAccepted();
    expectJobFetch();
    expectActiveTaskFetch(scheduledTask);

    stateManager.insertPendingTasks(ImmutableSet.of(
        parsedConfiguration.getJobConfig().getTaskConfig().deepCopy().setShardId(1)));

    control.replay();

    cron.receiveJob(parsedConfiguration);
    cron.startJobNow(job.getKey());
  }

  @Test
  public void testConsistentState() throws Exception {
    JobConfiguration updated = makeJob().setCronSchedule("1 2 3 4 5");

    expectJobAccepted();
    cronScheduler.deschedule("key");
    expectJobAccepted(updated);

    control.replay();

    cron.receiveJob(parsedConfiguration);

    JobConfiguration failedUpdate = updated.deepCopy();
    failedUpdate.unsetCronSchedule();
    try {
      cron.updateJob(new ParsedConfiguration(failedUpdate));
      fail();
    } catch (ScheduleException e) {
      // Expected.
    }

    cron.updateJob(new ParsedConfiguration(updated));
  }

  private JobConfiguration makeJob() {
    return new JobConfiguration()
        .setName(JOB_NAME)
        .setOwner(new Identity(OWNER, OWNER))
        .setKey(JobKeys.from(OWNER, ENVIRONMENT, JOB_NAME))
        .setCronSchedule("1 1 1 1 1")
        .setTaskConfig(defaultTask())
        .setShardCount(1);
  }

  private static TaskConfig defaultTask() {
    return new TaskConfig()
        .setContactEmail("testing@twitter.com")
        .setThermosConfig("data".getBytes())
        .setNumCpus(1)
        .setRamMb(1024)
        .setDiskMb(1024)
        .setEnvironment(DEFAULT_ENVIRONMENT);
  }
}

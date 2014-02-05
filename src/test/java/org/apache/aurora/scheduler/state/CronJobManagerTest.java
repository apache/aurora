/**
 * Copyright 2013 Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.state;

import java.util.concurrent.Executor;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.base.ExceptionalCommand;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.CronCollisionPolicy;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.ScheduleException;
import org.apache.aurora.scheduler.configuration.SanitizedConfiguration;
import org.apache.aurora.scheduler.cron.CronException;
import org.apache.aurora.scheduler.cron.CronScheduler;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent.SchedulerActive;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.apiConstants.DEFAULT_ENVIRONMENT;
import static org.apache.aurora.scheduler.state.CronJobManager.CRON_USER;
import static org.apache.aurora.scheduler.state.CronJobManager.MANAGER_KEY;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CronJobManagerTest extends EasyMockTest {

  private static final String OWNER = "owner";
  private static final String ENVIRONMENT = "staging11";
  private static final String JOB_NAME = "jobName";
  private static final String DEFAULT_JOB_KEY = "key";
  private static final IScheduledTask TASK = IScheduledTask.build(new ScheduledTask());

  private SchedulerCore scheduler;
  private StateManager stateManager;
  private Executor delayExecutor;
  private Capture<Runnable> delayLaunchCapture;
  private StorageTestUtil storageUtil;

  private CronScheduler cronScheduler;
  private ShutdownRegistry shutdownRegistry;
  private CronJobManager cron;
  private IJobConfiguration job;
  private SanitizedConfiguration sanitizedConfiguration;

  @Before
  public void setUp() throws Exception {
    scheduler = createMock(SchedulerCore.class);
    stateManager = createMock(StateManager.class);
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
    sanitizedConfiguration = SanitizedConfiguration.fromUnsanitized(job);
  }

  private void expectJobValidated(IJobConfiguration savedJob) {
    expect(cronScheduler.isValidSchedule(savedJob.getCronSchedule())).andReturn(true);
  }

  private void expectJobValidated() {
    expectJobValidated(sanitizedConfiguration.getJobConfig());
  }

  private Capture<Runnable> expectJobAccepted(IJobConfiguration savedJob) throws Exception {
    expectJobValidated(savedJob);
    storageUtil.jobStore.saveAcceptedJob(MANAGER_KEY, savedJob);
    Capture<Runnable> jobTriggerCapture = createCapture();
    expect(cronScheduler.schedule(eq(savedJob.getCronSchedule()), capture(jobTriggerCapture)))
        .andReturn(DEFAULT_JOB_KEY);
    return jobTriggerCapture;
  }

  private void expectJobAccepted() throws Exception {
    expectJobAccepted(sanitizedConfiguration.getJobConfig());
  }

  private void expectJobFetch() {
    expectJobValidated();
    expect(storageUtil.jobStore.fetchJob(MANAGER_KEY, job.getKey()))
        .andReturn(Optional.of(sanitizedConfiguration.getJobConfig()));
  }

  private IExpectationSetters<?> expectActiveTaskFetch(IScheduledTask... activeTasks) {
    return storageUtil.expectTaskFetch(Query.jobScoped(job.getKey()).active(), activeTasks);
  }

  @Test
  public void testPubsubWiring() throws Exception {
    expect(cronScheduler.startAsync()).andReturn(cronScheduler);
    cronScheduler.awaitRunning();
    shutdownRegistry.addAction(EasyMock.<ExceptionalCommand<?>>anyObject());
    expect(storageUtil.jobStore.fetchJobs(MANAGER_KEY))
        .andReturn(ImmutableList.<IJobConfiguration>of());

    control.replay();

    Injector injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        bind(StateManager.class).toInstance(stateManager);
        bind(Storage.class).toInstance(storageUtil.storage);
        bind(CronScheduler.class).toInstance(cronScheduler);
        bind(ShutdownRegistry.class).toInstance(shutdownRegistry);
        bind(SchedulerCore.class).toInstance(scheduler);
        PubsubTestUtil.installPubsub(binder());
        StateModule.bindCronJobManager(binder());
      }
    });
    cron = injector.getInstance(CronJobManager.class);
    EventSink eventSink = PubsubTestUtil.startPubsub(injector);
    eventSink.post(new SchedulerActive());
  }

  @Test
  public void testStart() throws Exception {
    expectJobAccepted();
    expectJobFetch();
    expectActiveTaskFetch();

    // Job is executed immediately since there are no existing tasks to kill.
    stateManager.insertPendingTasks(sanitizedConfiguration.getTaskConfigs());
    expect(cronScheduler.getSchedule(DEFAULT_JOB_KEY))
        .andReturn(Optional.of(job.getCronSchedule()))
        .times(2);

    control.replay();

    assertEquals(ImmutableMap.<IJobKey, String>of(), cron.getScheduledJobs());
    cron.receiveJob(sanitizedConfiguration);
    assertEquals(ImmutableMap.of(job.getKey(), job.getCronSchedule()), cron.getScheduledJobs());
    cron.startJobNow(job.getKey());
    assertEquals(ImmutableMap.of(job.getKey(), job.getCronSchedule()), cron.getScheduledJobs());
  }

  @Test
  public void testDeleteInconsistent() throws Exception {
    // Tests a case where a job exists in the storage, but is not registered with the cron system.

    expect(storageUtil.jobStore.fetchJob(MANAGER_KEY, job.getKey()))
        .andReturn(Optional.of(sanitizedConfiguration.getJobConfig()));

    control.replay();

    assertTrue(cron.deleteJob(job.getKey()));
  }

  @Test
  public void testDelayedStart() throws Exception {
    expectJobAccepted();
    expectJobFetch();

    // Query to test if live tasks exist for the job.
    expectActiveTaskFetch(TASK);

    // Live tasks exist, so the cron manager must delay the cron launch.
    delayExecutor.execute(capture(delayLaunchCapture));

    // The cron manager will then try to initiate the kill.
    scheduler.killTasks((Query.Builder) anyObject(), eq(CronJobManager.CRON_USER));

    // Immediate query and delayed query.
    expectActiveTaskFetch(TASK).times(2);

    // Simulate the live task disappearing.
    expectActiveTaskFetch();

    stateManager.insertPendingTasks(sanitizedConfiguration.getTaskConfigs());

    control.replay();

    cron.receiveJob(sanitizedConfiguration);
    cron.startJobNow(job.getKey());
    assertEquals(ImmutableSet.of(job.getKey()), cron.getPendingRuns());
    delayLaunchCapture.getValue().run();
    assertEquals(ImmutableSet.<IJobKey>of(), cron.getPendingRuns());
  }

  @Test
  public void testDelayedStartResets() throws Exception {
    expectJobAccepted();
    expectJobFetch();

    // Query to test if live tasks exist for the job.
    expectActiveTaskFetch(TASK);

    // Live tasks exist, so the cron manager must delay the cron launch.
    delayExecutor.execute(capture(delayLaunchCapture));

    // The cron manager will then try to initiate the kill.
    scheduler.killTasks((Query.Builder) anyObject(), eq(CronJobManager.CRON_USER));

    // Immediate query and delayed query.
    expectActiveTaskFetch(TASK).times(2);

    // Simulate the live task disappearing.
    expectActiveTaskFetch();

    // Round two.
    expectJobFetch();
    expectActiveTaskFetch(TASK);
    delayExecutor.execute(capture(delayLaunchCapture));
    scheduler.killTasks((Query.Builder) anyObject(), eq(CronJobManager.CRON_USER));
    expectActiveTaskFetch(TASK).times(2);
    expectActiveTaskFetch();

    stateManager.insertPendingTasks(sanitizedConfiguration.getTaskConfigs());
    expectLastCall().times(2);

    control.replay();

    cron.receiveJob(sanitizedConfiguration);
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
    expectActiveTaskFetch(TASK).times(3);

    // Live tasks exist, so the cron manager must delay the cron launch.
    delayExecutor.execute(capture(delayLaunchCapture));

    // The cron manager will then try to initiate the kill.
    expectJobFetch();
    expectJobFetch();
    scheduler.killTasks((Query.Builder) anyObject(), eq(CronJobManager.CRON_USER));
    expectLastCall().times(3);

    // Immediate queries and delayed query.
    expectActiveTaskFetch(TASK).times(4);

    // Simulate the live task disappearing.
    expectActiveTaskFetch();

    stateManager.insertPendingTasks(sanitizedConfiguration.getTaskConfigs());

    control.replay();

    cron.receiveJob(sanitizedConfiguration);

    // Attempt to trick the cron manager into launching multiple times, or launching multiple
    // pollers.
    cron.startJobNow(job.getKey());
    cron.startJobNow(job.getKey());
    cron.startJobNow(job.getKey());
    delayLaunchCapture.getValue().run();
  }

  @Test
  public void testUpdate() throws Exception {
    SanitizedConfiguration updated = new SanitizedConfiguration(
        IJobConfiguration.build(job.newBuilder().setCronSchedule("1 2 3 4 5")));

    expectJobAccepted();
    cronScheduler.deschedule(DEFAULT_JOB_KEY);
    expectJobAccepted(updated.getJobConfig());

    control.replay();

    cron.receiveJob(sanitizedConfiguration);
    cron.updateJob(updated);
  }

  @Test
  public void testConsistentState() throws Exception {
    IJobConfiguration updated =
        IJobConfiguration.build(makeJob().newBuilder().setCronSchedule("1 2 3 4 5"));

    expectJobAccepted();
    cronScheduler.deschedule(DEFAULT_JOB_KEY);
    expectJobAccepted(updated);

    control.replay();

    cron.receiveJob(sanitizedConfiguration);

    IJobConfiguration failedUpdate =
        IJobConfiguration.build(updated.newBuilder().setCronSchedule(null));
    try {
      cron.updateJob(new SanitizedConfiguration(failedUpdate));
      fail();
    } catch (ScheduleException e) {
      // Expected.
    }

    cron.updateJob(new SanitizedConfiguration(updated));
  }

  @Test
  public void testInvalidStoredJob() throws Exception {
    // Invalid jobs are left alone, but doesn't halt operation.

    expect(cronScheduler.startAsync()).andReturn(cronScheduler);
    cronScheduler.awaitRunning();
    shutdownRegistry.addAction(EasyMock.<ExceptionalCommand<?>>anyObject());

    IJobConfiguration jobA =
        IJobConfiguration.build(makeJob().newBuilder().setCronSchedule("1 2 3 4 5 6 7"));
    IJobConfiguration jobB =
        IJobConfiguration.build(makeJob().newBuilder().setCronSchedule(null));

    expect(storageUtil.jobStore.fetchJobs(MANAGER_KEY)).andReturn(ImmutableList.of(jobA, jobB));
    expect(cronScheduler.isValidSchedule(jobA.getCronSchedule())).andReturn(false);

    control.replay();

    cron.schedulerActive(new SchedulerActive());
  }

  @Test(expected = IllegalStateException.class)
  public void testJobStoredTwice() throws Exception {
    // Simulate an inconsistent storage that contains two cron jobs under the same key.

    expect(cronScheduler.startAsync()).andReturn(cronScheduler);
    cronScheduler.awaitRunning();
    shutdownRegistry.addAction(EasyMock.<ExceptionalCommand<?>>anyObject());

    IJobConfiguration jobA =
        IJobConfiguration.build(makeJob().newBuilder().setCronSchedule("1 2 3 4 5"));
    IJobConfiguration jobB =
        IJobConfiguration.build(makeJob().newBuilder().setCronSchedule("* * * * *"));
    expect(storageUtil.jobStore.fetchJobs(MANAGER_KEY))
        .andReturn(ImmutableList.of(jobA, jobB));
    expectJobValidated(jobA);
    expect(cronScheduler.schedule(eq(jobA.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andReturn("keyA");
    expectJobValidated(jobB);

    control.replay();

    cron.schedulerActive(new SchedulerActive());
  }

  @Test(expected = ScheduleException.class)
  public void testInvalidCronSchedule() throws Exception {
    expect(cronScheduler.isValidSchedule(job.getCronSchedule())).andReturn(false);

    control.replay();

    cron.receiveJob(sanitizedConfiguration);
  }

  @Test
  public void testKillExistingCollisionFailedKill() throws Exception {
    IJobConfiguration killExisting = IJobConfiguration.build(
        job.newBuilder().setCronCollisionPolicy(CronCollisionPolicy.KILL_EXISTING));
    Capture<Runnable> jobTriggerCapture = expectJobAccepted(killExisting);
    expectActiveTaskFetch(TASK);
    scheduler.killTasks(Query.jobScoped(killExisting.getKey()).active(), CRON_USER);
    expectLastCall().andThrow(new ScheduleException("injected"));

    control.replay();

    cron.receiveJob(new SanitizedConfiguration(killExisting));
    jobTriggerCapture.getValue().run();
  }

  @Test
  public void testCancelNewCollision() throws Exception {
    IJobConfiguration killExisting = IJobConfiguration.build(
        job.newBuilder().setCronCollisionPolicy(CronCollisionPolicy.CANCEL_NEW));
    Capture<Runnable> jobTriggerCapture = expectJobAccepted(killExisting);
    expectActiveTaskFetch(TASK);

    control.replay();

    cron.receiveJob(new SanitizedConfiguration(killExisting));
    jobTriggerCapture.getValue().run();
  }

  @Test
  public void testRunOverlapCollision() throws Exception {
    IJobConfiguration killExisting = IJobConfiguration.build(
        job.newBuilder().setCronCollisionPolicy(CronCollisionPolicy.RUN_OVERLAP));
    Capture<Runnable> jobTriggerCapture = expectJobAccepted(killExisting);
    expectActiveTaskFetch(TASK);

    control.replay();

    cron.receiveJob(new SanitizedConfiguration(killExisting));
    jobTriggerCapture.getValue().run();
  }

  @Test(expected = ScheduleException.class)
  public void testScheduleFails() throws Exception {
    expectJobValidated(job);
    storageUtil.jobStore.saveAcceptedJob(MANAGER_KEY, sanitizedConfiguration.getJobConfig());
    expect(cronScheduler.schedule(eq(job.getCronSchedule()), EasyMock.<Runnable>anyObject()))
        .andThrow(new CronException("injected"));

    control.replay();

    cron.receiveJob(sanitizedConfiguration);
  }

  private IJobConfiguration makeJob() {
    return IJobConfiguration.build(new JobConfiguration()
        .setOwner(new Identity(OWNER, OWNER))
        .setKey(JobKeys.from(OWNER, ENVIRONMENT, JOB_NAME).newBuilder())
        .setCronSchedule("1 1 1 1 1")
        .setTaskConfig(defaultTask())
        .setInstanceCount(1));
  }

  private static TaskConfig defaultTask() {
    return new TaskConfig()
        .setContactEmail("testing@twitter.com")
        .setExecutorConfig(new ExecutorConfig("aurora", "data"))
        .setNumCpus(1)
        .setRamMb(1024)
        .setDiskMb(1024)
        .setJobName(JOB_NAME)
        .setOwner(new Identity(OWNER, OWNER))
        .setEnvironment(DEFAULT_ENVIRONMENT);
  }
}

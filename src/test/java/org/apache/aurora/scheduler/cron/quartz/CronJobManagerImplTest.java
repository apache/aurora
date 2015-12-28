/**
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
package org.apache.aurora.scheduler.cron.quartz;

import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.CronCollisionPolicy;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.cron.CronException;
import org.apache.aurora.scheduler.cron.CronJobManager;
import org.apache.aurora.scheduler.cron.CrontabEntry;
import org.apache.aurora.scheduler.cron.SanitizedCronJob;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.db.DbUtil;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CronJobManagerImplTest extends EasyMockTest {
  private Storage storage;
  private Scheduler scheduler;

  private CronJobManager cronJobManager;

  @Before
  public void setUp() {
    storage = DbUtil.createStorage();
    scheduler = createMock(Scheduler.class);

    cronJobManager = new CronJobManagerImpl(storage, scheduler, TimeZone.getTimeZone("GMT"));
  }

  @Test
  public void testStartJobNowExistent() throws Exception {
    populateStorage();
    scheduler.triggerJob(QuartzTestUtil.QUARTZ_JOB_KEY);

    control.replay();

    cronJobManager.startJobNow(QuartzTestUtil.AURORA_JOB_KEY);
  }

  @Test(expected = CronException.class)
  public void testStartJobNowFails() throws Exception {
    populateStorage();
    scheduler.triggerJob(QuartzTestUtil.QUARTZ_JOB_KEY);
    expectLastCall().andThrow(new SchedulerException());

    control.replay();

    cronJobManager.startJobNow(QuartzTestUtil.AURORA_JOB_KEY);
  }

  @Test(expected = CronException.class)
  public void testStartJobNowNonexistent() throws Exception {
    control.replay();

    cronJobManager.startJobNow(QuartzTestUtil.AURORA_JOB_KEY);
  }

  @Test
  public void testUpdateExistingJob() throws Exception {
    SanitizedCronJob sanitizedCronJob = QuartzTestUtil.makeSanitizedCronJob();

    expect(scheduler.deleteJob(QuartzTestUtil.QUARTZ_JOB_KEY)).andReturn(true);
    expect(scheduler.scheduleJob(anyObject(JobDetail.class), anyObject(Trigger.class)))
       .andReturn(null);

    populateStorage();

    control.replay();

    cronJobManager.updateJob(sanitizedCronJob);
    assertEquals(sanitizedCronJob.getSanitizedConfig().getJobConfig(), fetchFromStorage().orNull());
  }

  @Test
  public void testUpdateNonexistentJob() throws Exception {
    control.replay();

    try {
      cronJobManager.updateJob(QuartzTestUtil.makeUpdatedJob());
      fail();
    } catch (CronException e) {
      // Expected.
    }

    assertEquals(Optional.absent(), fetchFromStorage());
  }

  @Test
  public void testCreateNonexistentJob() throws Exception {
    SanitizedCronJob sanitizedCronJob = QuartzTestUtil.makeSanitizedCronJob();

    expect(scheduler.scheduleJob(anyObject(JobDetail.class), anyObject(Trigger.class)))
        .andReturn(null);

    control.replay();

    cronJobManager.createJob(sanitizedCronJob);

    assertEquals(
        sanitizedCronJob.getSanitizedConfig().getJobConfig(),
        fetchFromStorage().orNull());
  }

  @Test(expected = CronException.class)
  public void testCreateNonexistentJobFails() throws Exception {
    SanitizedCronJob sanitizedCronJob = QuartzTestUtil.makeSanitizedCronJob();

    expect(scheduler.scheduleJob(anyObject(JobDetail.class), anyObject(Trigger.class)))
        .andThrow(new SchedulerException());

    control.replay();

    cronJobManager.createJob(sanitizedCronJob);
  }

  @Test(expected = CronException.class)
  public void testCreateExistingJobFails() throws Exception {
    SanitizedCronJob sanitizedCronJob = QuartzTestUtil.makeSanitizedCronJob();
    populateStorage();
    control.replay();

    cronJobManager.createJob(sanitizedCronJob);
  }

  @Test
  public void testNoRunOverlap() throws Exception {
    SanitizedCronJob runOverlapJob = SanitizedCronJob.fromUnsanitized(
        TaskTestUtil.CONFIGURATION_MANAGER,
        IJobConfiguration.build(QuartzTestUtil.JOB.newBuilder()
            .setCronCollisionPolicy(CronCollisionPolicy.RUN_OVERLAP)));

    control.replay();

    try {
      cronJobManager.createJob(runOverlapJob);
      fail();
    } catch (CronException e) {
      // Expected.
    }

    try {
      cronJobManager.updateJob(runOverlapJob);
    } catch (CronException e) {
      // Expected.
    }

    assertEquals(Optional.absent(), fetchFromStorage());
  }

  @Test
  public void testDeleteJob() throws Exception {
    expect(scheduler.deleteJob(QuartzTestUtil.QUARTZ_JOB_KEY)).andReturn(true);

    control.replay();

    assertFalse(cronJobManager.deleteJob(QuartzTestUtil.AURORA_JOB_KEY));
    populateStorage();
    assertTrue(cronJobManager.deleteJob(QuartzTestUtil.AURORA_JOB_KEY));
    assertEquals(Optional.absent(), fetchFromStorage());
  }

  @Test
  public void testFailedDeleteJobDoesNotThrow() throws Exception {
    expect(scheduler.deleteJob(QuartzTestUtil.QUARTZ_JOB_KEY)).andThrow(new SchedulerException());

    control.replay();

    populateStorage();
    cronJobManager.deleteJob(QuartzTestUtil.AURORA_JOB_KEY);
  }

  @Test
  public void testGetScheduledJobs() throws Exception {
    CronTrigger cronTrigger = createMock(CronTrigger.class);
    expect(scheduler.getJobKeys(anyObject()))
        .andReturn(ImmutableSet.of(QuartzTestUtil.QUARTZ_JOB_KEY));
    EasyMock.
        <List<? extends Trigger>>expect(scheduler.getTriggersOfJob(QuartzTestUtil.QUARTZ_JOB_KEY))
        .andReturn(ImmutableList.of(cronTrigger));
    expect(cronTrigger.getDescription()).andReturn("* * * * *");

    control.replay();

    Map<IJobKey, CrontabEntry> scheduledJobs = cronJobManager.getScheduledJobs();
    assertEquals(CrontabEntry.parse("* * * * *"), scheduledJobs.get(QuartzTestUtil.AURORA_JOB_KEY));
  }

  @Test
  public void testGetScheduledJobsEmpty() throws Exception {
    expect(scheduler.getJobKeys(anyObject()))
        .andReturn(ImmutableSet.of(QuartzTestUtil.QUARTZ_JOB_KEY));
    EasyMock.
        <List<? extends Trigger>>expect(scheduler.getTriggersOfJob(QuartzTestUtil.QUARTZ_JOB_KEY))
        .andReturn(ImmutableList.of());

    control.replay();
    assertEquals(ImmutableMap.of(), cronJobManager.getScheduledJobs());
  }

  @Test(expected = RuntimeException.class)
  public void testGetScheduledJobsFails() throws Exception {
    expect(scheduler.getJobKeys(anyObject()))
        .andReturn(ImmutableSet.of(QuartzTestUtil.QUARTZ_JOB_KEY));
    EasyMock.
        <List<? extends Trigger>>expect(scheduler.getTriggersOfJob(QuartzTestUtil.QUARTZ_JOB_KEY))
        .andThrow(new SchedulerException());

    control.replay();
    cronJobManager.getScheduledJobs();
  }

  private void populateStorage() throws Exception {
    storage.write((NoResult.Quiet) storeProvider -> storeProvider.getCronJobStore().saveAcceptedJob(
        QuartzTestUtil.makeSanitizedCronJob().getSanitizedConfig().getJobConfig()));
  }

  private Optional<IJobConfiguration> fetchFromStorage() {
    return storage.read(
        storeProvider -> storeProvider.getCronJobStore().fetchJob(QuartzTestUtil.AURORA_JOB_KEY));
  }
}

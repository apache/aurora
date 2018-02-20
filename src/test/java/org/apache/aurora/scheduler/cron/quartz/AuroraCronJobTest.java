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

import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.BackoffHelper;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.CronCollisionPolicy;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.scheduler.BatchWorker;
import org.apache.aurora.scheduler.BatchWorker.RepeatableWork;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.cron.quartz.AuroraCronJob.CronBatchWorker;
import org.apache.aurora.scheduler.state.StateChangeResult;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.mem.MemStorageModule;
import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.impl.JobDetailImpl;

import static org.apache.aurora.scheduler.cron.quartz.QuartzTestUtil.AURORA_JOB_KEY;
import static org.apache.aurora.scheduler.testing.BatchWorkerUtil.expectBatchExecute;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AuroraCronJobTest extends EasyMockTest {
  private static final String TASK_ID = "A";
  private JobDetailImpl jobDetails;
  private Storage storage;
  private StateManager stateManager;
  private BackoffHelper backoffHelper;
  private CronBatchWorker batchWorker;
  private JobExecutionContext context;
  private AuroraCronJob auroraCronJob;

  @Before
  public void setUp() throws Exception {
    storage = MemStorageModule.newEmptyStorage();
    stateManager = createMock(StateManager.class);
    backoffHelper = createMock(BackoffHelper.class);
    context = createMock(JobExecutionContext.class);

    jobDetails = new JobDetailImpl();
    jobDetails.setKey(Quartz.jobKey(AURORA_JOB_KEY));
    jobDetails.setJobDataMap(new JobDataMap(new HashMap()));
    expect(context.getJobDetail()).andReturn(jobDetails).anyTimes();

    batchWorker = createMock(CronBatchWorker.class);
    expectBatchExecute(batchWorker, storage, control).anyTimes();

    auroraCronJob = new AuroraCronJob(
        new AuroraCronJob.Config(backoffHelper),
        stateManager,
        batchWorker);
  }

  @Test
  public void testExecuteNonexistentIsNoop() throws JobExecutionException {
    control.replay();

    auroraCronJob.doExecute(context);
  }

  @Test
  public void testEmptyStorage() throws JobExecutionException {
    stateManager.insertPendingTasks(anyObject(), anyObject(), anyObject());
    expectLastCall().times(3);

    control.replay();

    populateStorage(CronCollisionPolicy.CANCEL_NEW);
    auroraCronJob.doExecute(context);

    storage = MemStorageModule.newEmptyStorage();
    populateStorage(CronCollisionPolicy.KILL_EXISTING);
    auroraCronJob.doExecute(context);

    storage = MemStorageModule.newEmptyStorage();
    populateStorage(CronCollisionPolicy.RUN_OVERLAP);
    auroraCronJob.doExecute(context);
  }

  @Test
  public void testCancelNew() throws JobExecutionException {
    control.replay();

    populateTaskStore();
    populateStorage(CronCollisionPolicy.CANCEL_NEW);
    auroraCronJob.doExecute(context);
  }

  @Test
  public void testOverlap() throws JobExecutionException {
    control.replay();

    populateTaskStore();
    populateStorage(CronCollisionPolicy.RUN_OVERLAP);
    auroraCronJob.doExecute(context);
  }

  @Test
  public void testKillExisting() throws Exception {
    Capture<RepeatableWork<BatchWorker.NoResult>> killCapture = createCapture();
    CompletableFuture<BatchWorker.NoResult> killResult = new CompletableFuture<>();
    expect(batchWorker.executeWithReplay(anyObject(), capture(killCapture))).andReturn(killResult);

    expect(backoffHelper.getBackoffStrategy()).andReturn(null).anyTimes();
    expect(stateManager.changeState(
        anyObject(),
        eq(TASK_ID),
        eq(Optional.empty()),
        eq(ScheduleStatus.KILLING),
        eq(AuroraCronJob.KILL_AUDIT_MESSAGE)))
        .andReturn(StateChangeResult.SUCCESS);
    stateManager.insertPendingTasks(anyObject(), anyObject(), anyObject());
    expectLastCall().times(2);

    control.replay();

    populateStorage(CronCollisionPolicy.KILL_EXISTING);
    populateTaskStore();
    auroraCronJob.doExecute(context);

    storage.write(
        (NoResult.Quiet) storeProvider -> storeProvider.getUnsafeTaskStore().deleteAllTasks());
    storage.write((NoResult.Quiet) store -> killCapture.getValue().apply(store));

    // Simulate a trigger in progress.
    jobDetails.getJobDataMap().put(JobKeys.canonicalString(AURORA_JOB_KEY), null);
    assertEquals(
        jobDetails.getJobDataMap().get(JobKeys.canonicalString(AURORA_JOB_KEY)),
        null);

    // Attempt a concurrent run that must be rejected.
    auroraCronJob.doExecute(context);

    // Complete previous run and trigger another one.
    killResult.complete(BatchWorker.NO_RESULT);
    assertEquals(
        jobDetails.getJobDataMap().get(JobKeys.canonicalString(AURORA_JOB_KEY)),
        AURORA_JOB_KEY);
    auroraCronJob.doExecute(context);
    assertTrue(jobDetails.getJobDataMap().isEmpty());
  }

  @Test
  public void testNoConcurrentRun() throws Exception {
    jobDetails.getJobDataMap().put(JobKeys.canonicalString(AURORA_JOB_KEY), null);

    control.replay();

    auroraCronJob.doExecute(context);
  }

  private void populateTaskStore() {
    storage.write((NoResult.Quiet)
        storeProvider -> storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(
            IScheduledTask.build(new ScheduledTask()
                .setStatus(ScheduleStatus.RUNNING)
                .setAssignedTask(new AssignedTask()
                    .setTaskId(TASK_ID)
                    .setTask(QuartzTestUtil.JOB.getTaskConfig().newBuilder()))))));
  }

  private void populateStorage(CronCollisionPolicy policy) {
    storage.write((NoResult.Quiet) storeProvider -> storeProvider.getCronJobStore().saveAcceptedJob(
        QuartzTestUtil.makeSanitizedCronJob(policy).getSanitizedConfig().getJobConfig()));
  }
}

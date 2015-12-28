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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.base.ExceptionalSupplier;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.BackoffHelper;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.CronCollisionPolicy;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.state.StateChangeResult;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.db.DbUtil;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.quartz.JobExecutionException;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AuroraCronJobTest extends EasyMockTest {
  public static final String TASK_ID = "A";
  private Storage storage;
  private StateManager stateManager;
  private BackoffHelper backoffHelper;

  private AuroraCronJob auroraCronJob;

  @Before
  public void setUp() {
    storage = DbUtil.createStorage();
    stateManager = createMock(StateManager.class);
    backoffHelper = createMock(BackoffHelper.class);

    auroraCronJob = new AuroraCronJob(
        TaskTestUtil.CONFIGURATION_MANAGER,
        new AuroraCronJob.Config(backoffHelper), storage, stateManager);
  }

  @Test
  public void testExecuteNonexistentIsNoop() throws JobExecutionException {
    control.replay();

    auroraCronJob.doExecute(QuartzTestUtil.AURORA_JOB_KEY);
  }

  @Test
  public void testEmptyStorage() throws JobExecutionException {
    stateManager.insertPendingTasks(
        EasyMock.anyObject(),
        EasyMock.anyObject(),
        EasyMock.anyObject());
    expectLastCall().times(3);

    control.replay();
    populateStorage(CronCollisionPolicy.CANCEL_NEW);
    auroraCronJob.doExecute(QuartzTestUtil.AURORA_JOB_KEY);
    storage = DbUtil.createStorage();

    populateStorage(CronCollisionPolicy.KILL_EXISTING);
    auroraCronJob.doExecute(QuartzTestUtil.AURORA_JOB_KEY);
    storage = DbUtil.createStorage();

    populateStorage(CronCollisionPolicy.RUN_OVERLAP);
    auroraCronJob.doExecute(QuartzTestUtil.AURORA_JOB_KEY);
  }

  @Test
  public void testCancelNew() throws JobExecutionException {
    control.replay();

    populateTaskStore();
    populateStorage(CronCollisionPolicy.CANCEL_NEW);
    auroraCronJob.doExecute(QuartzTestUtil.AURORA_JOB_KEY);
  }

  @Test
  public void testKillExisting() throws Exception {
    Capture<ExceptionalSupplier<Boolean, RuntimeException>> capture = createCapture();

    expect(stateManager.changeState(
        EasyMock.anyObject(),
        eq(TASK_ID),
        eq(Optional.absent()),
        eq(ScheduleStatus.KILLING),
        eq(AuroraCronJob.KILL_AUDIT_MESSAGE)))
        .andReturn(StateChangeResult.SUCCESS);
    backoffHelper.doUntilSuccess(EasyMock.capture(capture));
    stateManager.insertPendingTasks(
        EasyMock.anyObject(),
        EasyMock.anyObject(),
        EasyMock.anyObject());

    control.replay();

    populateStorage(CronCollisionPolicy.KILL_EXISTING);
    populateTaskStore();
    auroraCronJob.doExecute(QuartzTestUtil.AURORA_JOB_KEY);
    assertFalse(capture.getValue().get());
    storage.write(
        (NoResult.Quiet) storeProvider -> storeProvider.getUnsafeTaskStore().deleteAllTasks());
    assertTrue(capture.getValue().get());
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

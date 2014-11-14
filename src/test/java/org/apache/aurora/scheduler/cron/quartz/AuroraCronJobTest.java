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

import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.twitter.common.base.Supplier;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.BackoffHelper;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.CronCollisionPolicy;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.scheduler.cron.CronJobManager;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.mem.MemStorage;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.quartz.JobExecutionException;

import static org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
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

  private static final String MANAGER_ID = "MANAGER_ID";

  @Before
  public void setUp() {
    storage = MemStorage.newEmptyStorage();
    stateManager = createMock(StateManager.class);
    CronJobManager cronJobManager = createMock(CronJobManager.class);
    backoffHelper = createMock(BackoffHelper.class);

    auroraCronJob = new AuroraCronJob(
        new AuroraCronJob.Config(backoffHelper), storage, stateManager, cronJobManager);
    expect(cronJobManager.getManagerKey()).andStubReturn(MANAGER_ID);
  }

  @Test
  public void testExecuteNonexistentIsNoop() throws JobExecutionException {
    control.replay();

    auroraCronJob.doExecute(QuartzTestUtil.AURORA_JOB_KEY);
  }

  @Test
  public void testInvalidConfigIsNoop() throws JobExecutionException {
    control.replay();
    storage.write(new Storage.MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getJobStore().saveAcceptedJob(
            MANAGER_ID,
            IJobConfiguration.build(QuartzTestUtil.JOB.newBuilder().setCronSchedule(null)));
      }
    });

    auroraCronJob.doExecute(QuartzTestUtil.AURORA_JOB_KEY);
  }

  @Test
  public void testEmptyStorage() throws JobExecutionException {
    stateManager.insertPendingTasks(
        EasyMock.<MutableStoreProvider>anyObject(),
        EasyMock.<ITaskConfig>anyObject(),
        EasyMock.<Set<Integer>>anyObject());
    expectLastCall().times(3);

    control.replay();
    populateStorage(CronCollisionPolicy.CANCEL_NEW);
    auroraCronJob.doExecute(QuartzTestUtil.AURORA_JOB_KEY);
    storage = MemStorage.newEmptyStorage();

    populateStorage(CronCollisionPolicy.KILL_EXISTING);
    auroraCronJob.doExecute(QuartzTestUtil.AURORA_JOB_KEY);
    storage = MemStorage.newEmptyStorage();

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
    Capture<Supplier<Boolean>> capture = createCapture();

    expect(stateManager.changeState(
        EasyMock.<MutableStoreProvider>anyObject(),
        eq(TASK_ID),
        eq(Optional.<ScheduleStatus>absent()),
        eq(ScheduleStatus.KILLING),
        eq(AuroraCronJob.KILL_AUDIT_MESSAGE)))
        .andReturn(true);
    backoffHelper.doUntilSuccess(EasyMock.capture(capture));
    stateManager.insertPendingTasks(
        EasyMock.<MutableStoreProvider>anyObject(),
        EasyMock.<ITaskConfig>anyObject(),
        EasyMock.<Set<Integer>>anyObject());

    control.replay();

    populateStorage(CronCollisionPolicy.KILL_EXISTING);
    populateTaskStore();
    auroraCronJob.doExecute(QuartzTestUtil.AURORA_JOB_KEY);
    assertFalse(capture.getValue().get());
    storage.write(
        new Storage.MutateWork.NoResult.Quiet() {
          @Override
          protected void execute(MutableStoreProvider storeProvider) {
            storeProvider.getUnsafeTaskStore().deleteAllTasks();
          }
        });
    assertTrue(capture.getValue().get());
  }

  private void populateTaskStore() {
    storage.write(new Storage.MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(
            IScheduledTask.build(new ScheduledTask()
                .setStatus(ScheduleStatus.RUNNING)
                .setAssignedTask(new AssignedTask()
                    .setTaskId(TASK_ID)
                    .setTask(QuartzTestUtil.JOB.getTaskConfig().newBuilder())))
        ));
      }
    });
  }

  private void populateStorage(final CronCollisionPolicy policy) {
    storage.write(new Storage.MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        storeProvider.getJobStore().saveAcceptedJob(
            MANAGER_ID,
            QuartzTestUtil.makeSanitizedCronJob(policy).getSanitizedConfig().getJobConfig());
      }
    });
  }
}

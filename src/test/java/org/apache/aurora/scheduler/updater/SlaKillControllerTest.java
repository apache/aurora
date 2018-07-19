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
package org.apache.aurora.scheduler.updater;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.BackoffStrategy;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateAction;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.PercentageSlaPolicy;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.SlaPolicy;
import org.apache.aurora.scheduler.base.InstanceKeys;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.sla.SlaManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ISlaPolicy;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeScheduledExecutor;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.aurora.scheduler.updater.UpdaterModule.UpdateActionBatchWorker;
import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.base.TaskTestUtil.JOB;
import static org.apache.aurora.scheduler.base.TaskTestUtil.makeTask;
import static org.apache.aurora.scheduler.testing.BatchWorkerUtil.expectBatchExecute;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.newCapture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SlaKillControllerTest extends EasyMockTest {

  private static final ITaskConfig OLD_CONFIG = TaskTestUtil.makeConfig(JOB);
  private static final SlaPolicy TEST_SLA_POLICY = SlaPolicy.percentageSlaPolicy(
      new PercentageSlaPolicy()
          .setPercentage(0)
          .setDurationSecs(0));
  private static final ITaskConfig NEW_CONFIG = ITaskConfig.build(
      TaskTestUtil.makeConfig(JOB).newBuilder().setSlaPolicy(TEST_SLA_POLICY));
  private static final IScheduledTask TASK = IScheduledTask.build(
      makeTask("id", OLD_CONFIG).newBuilder().setStatus(ScheduleStatus.RUNNING));
  private static final IAssignedTask ASSIGNED_TASK = TASK.getAssignedTask();
  private static final IInstanceKey INSTANCE_KEY = InstanceKeys.from(
      JOB,
      ASSIGNED_TASK.getInstanceId());
  private static final IJobUpdate UPDATE = IJobUpdate.build(
      new JobUpdate()
          .setInstructions(new JobUpdateInstructions()
              .setDesiredState(new InstanceTaskConfig()
                  .setTask(NEW_CONFIG.newBuilder()))
              .setSettings(new JobUpdateSettings()
                  .setSlaAware(true))));
  private static final IJobUpdateKey UPDATE_ID =
      IJobUpdateKey.build(new JobUpdateKey(JOB.newBuilder(), "update_id"));
  private static final String KILL_ATTEMPTS_STAT_NAME = SlaKillController.SLA_KILL_ATTEMPT
      + JobKeys.canonicalString(JOB);
  private static final String KILL_SUCCESSES_STAT_NAME = SlaKillController.SLA_KILL_SUCCESS
      + JobKeys.canonicalString(JOB);

  private StorageTestUtil storageUtil;
  private UpdateActionBatchWorker batchWorker;
  private SlaManager slaManager;
  private FakeScheduledExecutor clock;
  private BackoffStrategy backoffStrategy;
  private FakeStatsProvider statsProvider;
  private SlaKillController slaKillController;
  private CountDownLatch killCommandHasExecuted;
  private Consumer<Storage.MutableStoreProvider> fakeKillCommand;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    batchWorker = createMock(UpdateActionBatchWorker.class);
    slaManager = createMock(SlaManager.class);
    ScheduledExecutorService executor = createMock(ScheduledExecutorService.class);
    clock = FakeScheduledExecutor.scheduleExecutor(executor);
    backoffStrategy = createMock(BackoffStrategy.class);
    statsProvider = new FakeStatsProvider();
    slaKillController = new SlaKillController(
        executor,
        batchWorker,
        slaManager,
        clock,
        backoffStrategy,
        statsProvider);
    killCommandHasExecuted = new CountDownLatch(2);
    fakeKillCommand = mutableStoreProvider -> killCommandHasExecuted.countDown();
  }

  @Test
  public <T, E extends Exception> void testSlaKill() throws Exception {
    IJobUpdateDetails updateDetails = IJobUpdateDetails.build(
        new JobUpdateDetails(
            UPDATE.newBuilder(),
            ImmutableList.of(new JobUpdateEvent(JobUpdateStatus.ROLLING_FORWARD, 123L)),
            ImmutableList.of()));
    expect(storageUtil.jobUpdateStore.fetchJobUpdate(UPDATE_ID))
        .andReturn(Optional.of(updateDetails))
        .anyTimes();
    Capture<IJobInstanceUpdateEvent> instanceUpdateEventCapture = newCapture();
    storageUtil.jobUpdateStore.saveJobInstanceUpdateEvent(
        eq(UPDATE_ID),
        capture(instanceUpdateEventCapture));
    expectLastCall().times(2);
    storageUtil.expectTaskFetch(TASK.getAssignedTask().getTaskId(), TASK);
    Capture<Storage.MutateWork<T, E>> workCapture = createCapture();
    slaManager.checkSlaThenAct(
        eq(TASK),
        eq(ISlaPolicy.build(TEST_SLA_POLICY)),
        capture(workCapture),
        eq(ImmutableMap.of()),
        eq(false));
    expectBatchExecute(batchWorker, storageUtil.storage, control);
    expect(backoffStrategy.calculateBackoffMs(0)).andReturn(42L);

    control.replay();

    // Kill command has not been executed yet
    assertEquals(2, killCommandHasExecuted.getCount());

    // Start an SLA-aware kill
    slaKillController.slaKill(
        storageUtil.mutableStoreProvider,
        INSTANCE_KEY,
        TASK,
        UPDATE_ID,
        ISlaPolicy.build(TEST_SLA_POLICY),
        JobUpdateStatus.ROLLING_FORWARD,
        fakeKillCommand);

    // Ensure the SLA_CHECKING_MESSAGE message is added
    assertTrue(
        checkInstanceEventMatches(
            instanceUpdateEventCapture.getValue(),
            INSTANCE_KEY,
            JobUpdateAction.INSTANCE_UPDATING,
            SlaKillController.SLA_CHECKING_MESSAGE));
    instanceUpdateEventCapture.reset();
    assertFalse(instanceUpdateEventCapture.hasCaptured());

    // Pretend SLA passes, executes work
    workCapture.getValue().apply(storageUtil.mutableStoreProvider);
    assertEquals(1, killCommandHasExecuted.getCount());

    // Ensure the SLA_PASSED_MESSAGE message is added
    assertTrue(
        checkInstanceEventMatches(
            instanceUpdateEventCapture.getValue(),
            INSTANCE_KEY,
            JobUpdateAction.INSTANCE_UPDATING,
            SlaKillController.SLA_PASSED_MESSAGE));
  }

  /**
   * Test that SLA kills are retried in case the SLA check does not pass.
   */
  @Test
  public <T, E extends Exception> void testSlaKillRetry() throws Exception {
    IJobUpdateDetails updateDetails = IJobUpdateDetails.build(
        new JobUpdateDetails(
            UPDATE.newBuilder(),
            ImmutableList.of(new JobUpdateEvent(JobUpdateStatus.ROLLING_FORWARD, 123L)),
            ImmutableList.of()));
    expect(storageUtil.jobUpdateStore.fetchJobUpdate(UPDATE_ID))
        .andReturn(Optional.of(updateDetails))
        .anyTimes();
    storageUtil.jobUpdateStore.saveJobInstanceUpdateEvent(eq(UPDATE_ID), anyObject());
    expectLastCall().times(2);
    storageUtil.expectTaskFetch(TASK.getAssignedTask().getTaskId(), TASK).times(2);
    storageUtil.expectTaskFetch(
        TASK.getAssignedTask().getTaskId(),
        IScheduledTask.build(TASK.newBuilder().setStatus(ScheduleStatus.KILLING)));
    Capture<Storage.MutateWork<T, E>> workCapture = createCapture();
    slaManager.checkSlaThenAct(
        eq(TASK),
        eq(ISlaPolicy.build(TEST_SLA_POLICY)),
        capture(workCapture),
        eq(ImmutableMap.of()),
        eq(false));
    expectLastCall().times(2);
    expectBatchExecute(batchWorker, storageUtil.storage, control).times(3);
    expect(backoffStrategy.calculateBackoffMs(0L)).andReturn(42L);
    expect(backoffStrategy.calculateBackoffMs(42L)).andReturn(84L);

    control.replay();

    // Kill command has not been executed yet
    assertFalse(statsProvider.getAllValues().keySet().contains(KILL_ATTEMPTS_STAT_NAME));
    assertFalse(statsProvider.getAllValues().keySet().contains(KILL_SUCCESSES_STAT_NAME));
    assertFalse(workCapture.hasCaptured());
    assertEquals(killCommandHasExecuted.getCount(), 2);

    // Start an SLA-aware kill
    slaKillController.slaKill(
        storageUtil.mutableStoreProvider,
        INSTANCE_KEY,
        TASK,
        UPDATE_ID,
        ISlaPolicy.build(TEST_SLA_POLICY),
        JobUpdateStatus.ROLLING_FORWARD,
        fakeKillCommand);

    // SLA check is called and discarded, pretending it failed
    assertEquals(1, statsProvider.getLongValue(KILL_ATTEMPTS_STAT_NAME));
    assertFalse(statsProvider.getAllValues().keySet().contains(KILL_SUCCESSES_STAT_NAME));
    assertTrue(workCapture.hasCaptured());
    workCapture.reset();
    assertEquals(2, killCommandHasExecuted.getCount());
    assertFalse(workCapture.hasCaptured());

    // Another SLA kill is scheduled assuming the previous attempt failed
    assertEquals(1, clock.countDeferredWork());
    clock.advance(TimeAmount.of(42L, Time.MILLISECONDS));

    // The second SLA check passes and the kill function is called
    assertEquals(2, killCommandHasExecuted.getCount());
    workCapture.getValue().apply(storageUtil.mutableStoreProvider);
    assertEquals(1, killCommandHasExecuted.getCount());
    assertEquals(2, statsProvider.getLongValue(KILL_ATTEMPTS_STAT_NAME));
    assertEquals(1, statsProvider.getLongValue(KILL_SUCCESSES_STAT_NAME));

    // One more SLA kill is scheduled assuming the previous attempt failed. Since the previous
    // attempt did not fail, we do a NOOP since the task is already KILLING
    clock.advance(TimeAmount.of(84L, Time.MILLISECONDS));
    assertEquals(2, statsProvider.getLongValue(KILL_ATTEMPTS_STAT_NAME));
    assertEquals(1, statsProvider.getLongValue(KILL_SUCCESSES_STAT_NAME));
    assertEquals(1, killCommandHasExecuted.getCount());
    assertEquals(0, clock.countDeferredWork());
  }

  @Test
  public <T, E extends Exception> void testSlaKillRollingBack() throws Exception {
    IJobUpdateDetails updateDetails = IJobUpdateDetails.build(
        new JobUpdateDetails(
            UPDATE.newBuilder(),
            ImmutableList.of(new JobUpdateEvent(JobUpdateStatus.ROLLING_BACK, 123L)),
            ImmutableList.of()));
    expect(storageUtil.jobUpdateStore.fetchJobUpdate(UPDATE_ID))
        .andReturn(Optional.of(updateDetails))
        .anyTimes();
    Capture<IJobInstanceUpdateEvent> instanceUpdateEventCapture = newCapture();
    storageUtil.jobUpdateStore.saveJobInstanceUpdateEvent(
        eq(UPDATE_ID),
        capture(instanceUpdateEventCapture));
    expectLastCall().times(2);
    storageUtil.expectTaskFetch(TASK.getAssignedTask().getTaskId(), TASK);
    Capture<Storage.MutateWork<T, E>> workCapture = createCapture();
    slaManager.checkSlaThenAct(
        eq(TASK),
        eq(ISlaPolicy.build(TEST_SLA_POLICY)),
        capture(workCapture),
        eq(ImmutableMap.of()),
        eq(false));
    expectBatchExecute(batchWorker, storageUtil.storage, control);
    expect(backoffStrategy.calculateBackoffMs(0)).andReturn(42L);

    control.replay();

    // Kill command has not been executed yet
    assertEquals(2, killCommandHasExecuted.getCount());

    // Start an SLA-aware kill
    slaKillController.slaKill(
        storageUtil.mutableStoreProvider,
        INSTANCE_KEY,
        TASK,
        UPDATE_ID,
        ISlaPolicy.build(TEST_SLA_POLICY),
        JobUpdateStatus.ROLLING_BACK,
        fakeKillCommand);

    // Ensure the SLA_CHECKING_MESSAGE message is added with ROLLING_BACK action
    assertTrue(
        checkInstanceEventMatches(
            instanceUpdateEventCapture.getValue(),
            INSTANCE_KEY,
            JobUpdateAction.INSTANCE_ROLLING_BACK,
            SlaKillController.SLA_CHECKING_MESSAGE));
    instanceUpdateEventCapture.reset();
    assertFalse(instanceUpdateEventCapture.hasCaptured());

    // Pretend SLA passes, executes work
    workCapture.getValue().apply(storageUtil.mutableStoreProvider);
    assertEquals(1, killCommandHasExecuted.getCount());

    // Ensure the SLA_PASSED_MESSAGE message is added with ROLLING_BACK action
    assertTrue(
        checkInstanceEventMatches(
            instanceUpdateEventCapture.getValue(),
            INSTANCE_KEY,
            JobUpdateAction.INSTANCE_ROLLING_BACK,
            SlaKillController.SLA_PASSED_MESSAGE));
  }

  @Test
  public void testSlaKillFailOnPause() throws Exception {
    IJobUpdateDetails updateDetails = IJobUpdateDetails.build(
        new JobUpdateDetails(
            UPDATE.newBuilder(),
            ImmutableList.of(new JobUpdateEvent(JobUpdateStatus.ROLLING_FORWARD, 123L)),
            ImmutableList.of()));
    IJobUpdateDetails pausedUpdateDetails = IJobUpdateDetails.build(
        new JobUpdateDetails(
            UPDATE.newBuilder(),
            ImmutableList.of(new JobUpdateEvent(JobUpdateStatus.ROLL_FORWARD_PAUSED, 123L)),
            ImmutableList.of()));
    expect(storageUtil.jobUpdateStore.fetchJobUpdate(UPDATE_ID))
        .andReturn(Optional.of(updateDetails));
    storageUtil.jobUpdateStore.saveJobInstanceUpdateEvent(eq(UPDATE_ID), anyObject());
    expectLastCall();
    expectBatchExecute(batchWorker, storageUtil.storage, control);
    expect(storageUtil.jobUpdateStore.fetchJobUpdate(UPDATE_ID))
        .andReturn(Optional.of(pausedUpdateDetails));

    control.replay();

    // Kill command has not been executed yet
    assertEquals(2, killCommandHasExecuted.getCount());

    // Start an SLA-aware kill
    slaKillController.slaKill(
        storageUtil.mutableStoreProvider,
        INSTANCE_KEY,
        TASK,
        UPDATE_ID,
        ISlaPolicy.build(TEST_SLA_POLICY),
        JobUpdateStatus.ROLLING_FORWARD,
        fakeKillCommand);

    // Nothing should happen since status has changed
    assertEquals(2, killCommandHasExecuted.getCount());
    assertEquals(0, clock.countDeferredWork());
    assertFalse(statsProvider.getAllValues().keySet().contains(KILL_ATTEMPTS_STAT_NAME));
  }

  @Test
  public void testSlaKillNoDuplicateEvents() throws Exception {
    IJobUpdateDetails updateDetails = IJobUpdateDetails.build(
        new JobUpdateDetails(
            UPDATE.newBuilder(),
            ImmutableList.of(new JobUpdateEvent(JobUpdateStatus.ROLLING_FORWARD, 123L)),
            ImmutableList.of(
                new JobInstanceUpdateEvent()
                    .setInstanceId(INSTANCE_KEY.getInstanceId())
                    .setAction(JobUpdateAction.INSTANCE_UPDATING)
                    .setMessage(SlaKillController.SLA_CHECKING_MESSAGE))));
    IJobUpdateDetails pausedUpdateDetails = IJobUpdateDetails.build(
        new JobUpdateDetails(
            UPDATE.newBuilder(),
            ImmutableList.of(new JobUpdateEvent(JobUpdateStatus.ROLL_FORWARD_PAUSED, 123L)),
            ImmutableList.of()));
    expect(storageUtil.jobUpdateStore.fetchJobUpdate(UPDATE_ID))
        .andReturn(Optional.of(updateDetails));
    expectBatchExecute(batchWorker, storageUtil.storage, control);
    expect(storageUtil.jobUpdateStore.fetchJobUpdate(UPDATE_ID))
        .andReturn(Optional.of(pausedUpdateDetails));

    control.replay();

    // Start an SLA-aware kill, update already contains event so we don't expect a save
    slaKillController.slaKill(
        storageUtil.mutableStoreProvider,
        INSTANCE_KEY,
        TASK,
        UPDATE_ID,
        ISlaPolicy.build(TEST_SLA_POLICY),
        JobUpdateStatus.ROLLING_FORWARD,
        fakeKillCommand);
  }

  @Test
  public void testSlaKillInvalidStatus() {
    control.replay();

    // Start an SLA-aware kill, throws an exception since the kill was called while the update
    // was not active
    try {
      slaKillController.slaKill(
          storageUtil.mutableStoreProvider,
          INSTANCE_KEY,
          TASK,
          UPDATE_ID,
          ISlaPolicy.build(TEST_SLA_POLICY),
          JobUpdateStatus.ROLL_FORWARD_PAUSED,
          fakeKillCommand);
    } catch (RuntimeException e) {
      return;
    }

    fail();
  }

  private boolean checkInstanceEventMatches(IJobInstanceUpdateEvent event,
                                            IInstanceKey instance,
                                            JobUpdateAction action,
                                            String message) {

    return event.getInstanceId() == instance.getInstanceId()
        && event.getAction() == action
        && event.getMessage().equals(message);
  }
}

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
import java.util.function.Consumer;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.InstanceKey;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.PercentageSlaPolicy;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.SlaPolicy;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.state.StateChangeResult;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateInstructions;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.updater.InstanceActionHandler.KillTask;
import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KillTaskTest extends EasyMockTest {
  private static final String TASK_ID = "task_id";
  private static final IJobKey JOB = JobKeys.from("role", "env", "job");
  private static final IInstanceKey INSTANCE =
      IInstanceKey.build(new InstanceKey(JOB.newBuilder(), 0));
  private static final IJobUpdateKey UPDATE_ID =
      IJobUpdateKey.build(new JobUpdateKey(JOB.newBuilder(), "update_id"));
  private static final SlaPolicy TEST_SLA_POLICY_OLD = SlaPolicy.percentageSlaPolicy(
      new PercentageSlaPolicy()
          .setPercentage(95)
          .setDurationSecs(3600));
  private static final SlaPolicy TEST_SLA_POLICY_NEW = SlaPolicy.percentageSlaPolicy(
      new PercentageSlaPolicy()
          .setPercentage(0)
          .setDurationSecs(0));
  private static final ITaskConfig CONFIG_NO_SLA = ITaskConfig.build(
      TaskTestUtil.makeConfig(JOB, true, Optional.empty()).newBuilder());
  private static final ITaskConfig OLD_CONFIG_WITH_SLA =
      TaskTestUtil.makeConfig(JOB, true, Optional.of(TEST_SLA_POLICY_OLD));
  private static final ITaskConfig NEW_CONFIG_WITH_SLA =
      TaskTestUtil.makeConfig(JOB, true, Optional.of(TEST_SLA_POLICY_NEW));
  private static final IJobUpdateInstructions INSTRUCTIONS = IJobUpdateInstructions.build(
      new JobUpdateInstructions()
          .setSettings(
              new JobUpdateSettings()
                  .setMinWaitInInstanceRunningMs(1000)));
  private static final IJobUpdateInstructions INSTRUCTIONS_SLA_AWARE = IJobUpdateInstructions.build(
      new JobUpdateInstructions()
          .setSettings(
              new JobUpdateSettings()
                  .setMinWaitInInstanceRunningMs(1000)
                  .setSlaAware(true))
          .setInitialState(ImmutableSet.of(new InstanceTaskConfig(
              OLD_CONFIG_WITH_SLA.newBuilder(),
              ImmutableSet.of(new Range(0, 0)))))
          .setDesiredState(new InstanceTaskConfig(
              NEW_CONFIG_WITH_SLA.newBuilder(),
              ImmutableSet.of(new Range(0, 0)))));
  private static final IJobUpdateInstructions INSTRUCTIONS_SLA_AWARE_NO_POLICY
      = IJobUpdateInstructions.build(
          new JobUpdateInstructions()
              .setSettings(
                  new JobUpdateSettings()
                      .setMinWaitInInstanceRunningMs(1000)
                      .setSlaAware(true))
              .setDesiredState(new InstanceTaskConfig(
                  CONFIG_NO_SLA.newBuilder(),
                  ImmutableSet.of(new Range(0, 0)))));

  private StorageTestUtil storageUtil;
  private StateManager stateManager;
  private InstanceActionHandler handler;
  private UpdateAgentReserver updateAgentReserver;
  private SlaKillController slaKillController;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    stateManager = createMock(StateManager.class);
    updateAgentReserver = createMock(UpdateAgentReserver.class);
    handler = new KillTask(false);
    slaKillController = createMock(SlaKillController.class);
  }

  @Test
  public void testInstanceKill() throws Exception {
    storageUtil.expectTaskFetch(
        Query.instanceScoped(INSTANCE).active(),
        TaskTestUtil.makeTask(TASK_ID, INSTANCE.getJobKey()));

    expect(stateManager.changeState(
        storageUtil.mutableStoreProvider,
        TASK_ID,
        Optional.empty(),
        ScheduleStatus.KILLING,
        Optional.of("Killed for job update " + UPDATE_ID.getId())))
            .andReturn(StateChangeResult.SUCCESS);

    control.replay();

    handler.getReevaluationDelay(
        INSTANCE,
        INSTRUCTIONS,
        storageUtil.mutableStoreProvider,
        stateManager,
        updateAgentReserver,
        JobUpdateStatus.ROLLING_BACK,
        UPDATE_ID,
        slaKillController);
  }

  @Test
  public void testKillForUpdateReservesAgentForInstance() throws Exception {
    IScheduledTask task = TaskTestUtil.makeTask(TASK_ID, INSTANCE.getJobKey(), 1, "agent01");
    storageUtil.expectTaskFetch(Query.instanceScoped(INSTANCE).active(), task);

    expect(stateManager.changeState(
        storageUtil.mutableStoreProvider,
        TASK_ID,
        Optional.empty(),
        ScheduleStatus.KILLING,
        Optional.of("Killed for job update " + UPDATE_ID.getId())))
        .andReturn(StateChangeResult.SUCCESS);
    updateAgentReserver.reserve(task.getAssignedTask().getSlaveId(), INSTANCE);
    expectLastCall();

    control.replay();

    new KillTask(true).getReevaluationDelay(
        INSTANCE,
        INSTRUCTIONS,
        storageUtil.mutableStoreProvider,
        stateManager,
        updateAgentReserver,
        JobUpdateStatus.ROLLING_BACK,
        UPDATE_ID,
        slaKillController);
  }

  @Test
  public void testInstanceNotFoundDoesNotThrow() throws Exception {
    storageUtil.expectTaskFetch(Query.instanceScoped(INSTANCE).active());

    control.replay();

    handler.getReevaluationDelay(
        INSTANCE,
        INSTRUCTIONS,
        storageUtil.mutableStoreProvider,
        stateManager,
        updateAgentReserver,
        JobUpdateStatus.ROLLING_BACK,
        UPDATE_ID,
        slaKillController);
  }

  /**
   * Ensures that if an instance is killed with code {@code slaAware} option in the instructions,
   * then the kill is sent to be handled by the {@link SlaKillController}.
   */
  @Test
  public void testInstanceKillSlaAware() throws Exception {
    IScheduledTask task = TaskTestUtil.makeTask(TASK_ID, INSTANCE.getJobKey());
    storageUtil.expectTaskFetch(Query.instanceScoped(INSTANCE).active(), task);

    slaKillController.slaKill(
        eq(storageUtil.mutableStoreProvider),
        eq(INSTANCE),
        eq(task),
        eq(UPDATE_ID),
        eq(INSTRUCTIONS_SLA_AWARE.getDesiredState().getTask().getSlaPolicy()),
        eq(JobUpdateStatus.ROLLING_FORWARD),
        anyObject());
    expectLastCall();

    control.replay();

    handler.getReevaluationDelay(
        INSTANCE,
        INSTRUCTIONS_SLA_AWARE,
        storageUtil.mutableStoreProvider,
        stateManager,
        updateAgentReserver,
        JobUpdateStatus.ROLLING_FORWARD,
        UPDATE_ID,
        slaKillController);
  }

  /**
   * Ensures that if an instance killed while {@link JobUpdateStatus#ROLLING_BACK}, it uses the old
   * configuration.
   */
  @Test
  public void testInstanceKillSlaAwareRollback() throws Exception {
    IScheduledTask task = TaskTestUtil.makeTask(TASK_ID, INSTANCE.getJobKey(), 1, "agent01");
    storageUtil.expectTaskFetch(Query.instanceScoped(INSTANCE).active(), task);

    Capture<Consumer<Storage.MutableStoreProvider>> killCommandCapture = createCapture();
    slaKillController.slaKill(
        eq(storageUtil.mutableStoreProvider),
        eq(INSTANCE),
        eq(task),
        eq(UPDATE_ID),
        eq(INSTRUCTIONS_SLA_AWARE.getInitialState().asList().get(0).getTask().getSlaPolicy()),
        eq(JobUpdateStatus.ROLLING_BACK),
        capture(killCommandCapture));
    expectLastCall();

    control.replay();

    handler.getReevaluationDelay(
        INSTANCE,
        INSTRUCTIONS_SLA_AWARE,
        storageUtil.mutableStoreProvider,
        stateManager,
        updateAgentReserver,
        JobUpdateStatus.ROLLING_BACK,
        UPDATE_ID,
        slaKillController);
  }

  /**
   * Ensure the correct behavior of the consumer passed into
   * {@link SlaKillController#slaKill}. It should behave as
   * {@link KillTask#killAndMaybeReserve}.
   */
  @Test
  public void testInstanceKillSlaAwareKillCommand() throws Exception {
    IScheduledTask task = TaskTestUtil.makeTask(TASK_ID, INSTANCE.getJobKey(), 1, "agent01");
    storageUtil.expectTaskFetch(Query.instanceScoped(INSTANCE).active(), task);

    Capture<Consumer<Storage.MutableStoreProvider>> killCommandCapture = createCapture();
    slaKillController.slaKill(
        eq(storageUtil.mutableStoreProvider),
        eq(INSTANCE),
        eq(task),
        eq(UPDATE_ID),
        eq(INSTRUCTIONS_SLA_AWARE.getDesiredState().getTask().getSlaPolicy()),
        eq(JobUpdateStatus.ROLLING_FORWARD),
        capture(killCommandCapture));
    expectLastCall();

    // Ensure the correct kill command consumer has been passed in and expected behavior occurs
    // when executed.
    expect(stateManager.changeState(
        storageUtil.mutableStoreProvider,
        TASK_ID,
        Optional.empty(),
        ScheduleStatus.KILLING,
        Optional.of("Killed for job update " + UPDATE_ID.getId())))
        .andReturn(StateChangeResult.SUCCESS);
    updateAgentReserver.reserve(task.getAssignedTask().getSlaveId(), INSTANCE);
    expectLastCall();

    control.replay();

    new KillTask(true).getReevaluationDelay(
        INSTANCE,
        INSTRUCTIONS_SLA_AWARE,
        storageUtil.mutableStoreProvider,
        stateManager,
        updateAgentReserver,
        JobUpdateStatus.ROLLING_FORWARD,
        UPDATE_ID,
        slaKillController);

    assertTrue(killCommandCapture.hasCaptured());
    killCommandCapture.getValue().accept(storageUtil.mutableStoreProvider);
  }

  /**
   * Ensures that if an instance is killed with code {@code slaAware} option in the instructions
   * but no {@link org.apache.aurora.gen.SlaPolicy} with the task, then the kill does not fail but
   * instead continues as a non-sla-aware kill.
   */
  @Test
  public void testInstanceKillSlaAwareMissingSlaPolicy() throws Exception {
    IScheduledTask task = TaskTestUtil.makeTask(TASK_ID, INSTANCE.getJobKey(), 1, "agent01");
    storageUtil.expectTaskFetch(Query.instanceScoped(INSTANCE).active(), task);
    expect(stateManager.changeState(
        storageUtil.mutableStoreProvider,
        TASK_ID,
        Optional.empty(),
        ScheduleStatus.KILLING,
        Optional.of("Killed for job update " + UPDATE_ID.getId())))
        .andReturn(StateChangeResult.SUCCESS);

    control.replay();

    handler.getReevaluationDelay(
        INSTANCE,
        INSTRUCTIONS_SLA_AWARE_NO_POLICY,
        storageUtil.mutableStoreProvider,
        stateManager,
        updateAgentReserver,
        JobUpdateStatus.ROLLING_FORWARD,
        UPDATE_ID,
        slaKillController);
  }

  /**
   * Ensures that if SLA-aware kill is called while not in an active state we throw an exception.
   */
  @Test
  public void testInstanceKillSlaAwareBadStatus() {
    IScheduledTask task = TaskTestUtil.makeTask(TASK_ID, INSTANCE.getJobKey(), 1, "agent01");
    storageUtil.expectTaskFetch(Query.instanceScoped(INSTANCE).active(), task);

    control.replay();

    try {
      handler.getReevaluationDelay(
          INSTANCE,
          INSTRUCTIONS_SLA_AWARE_NO_POLICY,
          storageUtil.mutableStoreProvider,
          stateManager,
          updateAgentReserver,
          JobUpdateStatus.ROLL_FORWARD_PAUSED,
          UPDATE_ID,
          slaKillController);
      fail();
    } catch (UpdateStateException e) {
      // Expected
    }
  }
}

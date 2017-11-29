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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.eventbus.EventBus;
import com.google.common.primitives.Ints;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

import org.apache.aurora.common.application.Lifecycle;
import org.apache.aurora.common.base.Command;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.Stats;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.common.util.TruncatedBinaryBackoff;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateAction;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdatePulseStatus;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateState;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.Metadata;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.SchedulerModule.TaskEventBatchWorker;
import org.apache.aurora.scheduler.TaskIdGenerator;
import org.apache.aurora.scheduler.TaskIdGenerator.TaskIdGeneratorImpl;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.mesos.Driver;
import org.apache.aurora.scheduler.scheduling.RescheduleCalculator;
import org.apache.aurora.scheduler.scheduling.RescheduleCalculator.RescheduleCalculatorImpl;
import org.apache.aurora.scheduler.state.StateChangeResult;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.state.StateManagerImpl;
import org.apache.aurora.scheduler.state.UUIDGenerator;
import org.apache.aurora.scheduler.state.UUIDGenerator.UUIDGeneratorImpl;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IInstanceTaskConfig;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.mem.MemStorageModule;
import org.apache.aurora.scheduler.testing.FakeScheduledExecutor;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.aurora.scheduler.updater.JobUpdateController.AuditData;
import org.apache.aurora.scheduler.updater.StateEvaluator.Failure;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.JobUpdateAction.INSTANCE_ROLLBACK_FAILED;
import static org.apache.aurora.gen.JobUpdateAction.INSTANCE_ROLLED_BACK;
import static org.apache.aurora.gen.JobUpdateAction.INSTANCE_ROLLING_BACK;
import static org.apache.aurora.gen.JobUpdateAction.INSTANCE_UPDATED;
import static org.apache.aurora.gen.JobUpdateAction.INSTANCE_UPDATE_FAILED;
import static org.apache.aurora.gen.JobUpdateAction.INSTANCE_UPDATING;
import static org.apache.aurora.gen.JobUpdateStatus.ABORTED;
import static org.apache.aurora.gen.JobUpdateStatus.ERROR;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLED_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLED_FORWARD;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_FORWARD;
import static org.apache.aurora.gen.JobUpdateStatus.ROLL_BACK_PAUSED;
import static org.apache.aurora.gen.JobUpdateStatus.ROLL_FORWARD_AWAITING_PULSE;
import static org.apache.aurora.gen.JobUpdateStatus.ROLL_FORWARD_PAUSED;
import static org.apache.aurora.gen.ScheduleStatus.ASSIGNED;
import static org.apache.aurora.gen.ScheduleStatus.FAILED;
import static org.apache.aurora.gen.ScheduleStatus.FINISHED;
import static org.apache.aurora.gen.ScheduleStatus.KILLED;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.gen.ScheduleStatus.STARTING;
import static org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import static org.apache.aurora.scheduler.testing.BatchWorkerUtil.expectBatchExecute;
import static org.apache.aurora.scheduler.updater.UpdateFactory.UpdateFactoryImpl.expandInstanceIds;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class JobUpdaterIT extends EasyMockTest {

  private static final String USER = "user";
  private static final AuditData AUDIT = new AuditData(USER, Optional.of("message"));
  private static final IJobKey JOB = JobKeys.from("role", "env", "job1");
  private static final IJobUpdateKey UPDATE_ID =
      IJobUpdateKey.build(new JobUpdateKey(JOB.newBuilder(), "update_id"));
  private static final Amount<Long, Time> WATCH_TIMEOUT = Amount.of(2000L, Time.MILLISECONDS);
  private static final TimeAmount FLAPPING_THRESHOLD = new TimeAmount(1L, Time.MILLISECONDS);
  private static final Amount<Long, Time> ONE_DAY = Amount.of(1L, Time.DAYS);
  private static final Amount<Long, Time> ONE_HOUR = Amount.of(1L, Time.HOURS);
  private static final Amount<Long, Time> ONE_MINUTE = Amount.of(1L, Time.MINUTES);
  private static final ITaskConfig OLD_CONFIG =
      setExecutorData(TaskTestUtil.makeConfig(JOB), "olddata");
  private static final ITaskConfig NEW_CONFIG = setExecutorData(OLD_CONFIG, "newdata");
  private static final long PULSE_TIMEOUT_MS = 10000;
  private static final ImmutableSet<Metadata> METADATA = ImmutableSet.of(
      new Metadata("k1", "v1"), new Metadata("k2", "v2"));

  private FakeScheduledExecutor clock;
  private JobUpdateController updater;
  private Driver driver;
  private EventBus eventBus;
  private Storage storage;
  private StateManager stateManager;
  private JobUpdateEventSubscriber subscriber;
  private Command shutdownCommand;

  private static ITaskConfig setExecutorData(ITaskConfig task, String executorData) {
    TaskConfig builder = task.newBuilder();
    builder.getExecutorConfig().setData(executorData);
    return ITaskConfig.build(builder);
  }

  @Before
  public void setUp() throws Exception {
    // Avoid console spam due to stats registered multiple times.
    Stats.flush();
    ScheduledExecutorService executor = createMock(ScheduledExecutorService.class);
    clock = FakeScheduledExecutor.scheduleExecutor(executor);
    driver = createMock(Driver.class);
    shutdownCommand = createMock(Command.class);
    eventBus = new EventBus();
    TaskEventBatchWorker batchWorker = createMock(TaskEventBatchWorker.class);

    UpdaterModule.Options options = new UpdaterModule.Options();
    options.enableAffinity = true;

    Injector injector = Guice.createInjector(
        new UpdaterModule(executor, options),
        new MemStorageModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(StatsProvider.class).toInstance(new FakeStatsProvider());
            bind(Clock.class).toInstance(clock);
            bind(StateManager.class).to(StateManagerImpl.class);
            bind(Driver.class).toInstance(driver);
            bind(TaskIdGenerator.class).to(TaskIdGeneratorImpl.class);
            bind(RescheduleCalculator.class).to(RescheduleCalculatorImpl.class);
            bind(RescheduleCalculatorImpl.RescheduleCalculatorSettings.class)
                .toInstance(new RescheduleCalculatorImpl.RescheduleCalculatorSettings(
                    new TruncatedBinaryBackoff(
                        Amount.of(1L, Time.SECONDS), Amount.of(1L, Time.MINUTES)),
                    FLAPPING_THRESHOLD,
                    new TimeAmount(1, Time.MINUTES)));
            bind(EventSink.class).toInstance(eventBus::post);
            bind(UUIDGenerator.class).to(UUIDGeneratorImpl.class);
            bind(Lifecycle.class).toInstance(new Lifecycle(shutdownCommand));
            bind(TaskEventBatchWorker.class).toInstance(batchWorker);
          }
        });
    updater = injector.getInstance(JobUpdateController.class);
    storage = injector.getInstance(Storage.class);
    storage.prepare();
    stateManager = injector.getInstance(StateManager.class);
    eventBus.register(injector.getInstance(JobUpdateEventSubscriber.class));
    subscriber = injector.getInstance(JobUpdateEventSubscriber.class);
    expectBatchExecute(batchWorker, storage, control).anyTimes();
  }

  @After
  public void validateExitState() {
    clock.assertEmpty();
  }

  private String getTaskId(IJobKey job, int instanceId) {
    return Tasks.id(Iterables.getOnlyElement(
        Storage.Util.fetchTasks(
            storage,
            Query.instanceScoped(job, instanceId).active())));
  }

  private void changeState(
      IJobKey job,
      int instanceId,
      ScheduleStatus status,
      ScheduleStatus... statuses) {

    for (ScheduleStatus s
        : ImmutableList.<ScheduleStatus>builder().add(status).add(statuses).build()) {

      storage.write((NoResult.Quiet) storeProvider ->
          assertEquals(
              StateChangeResult.SUCCESS,
              stateManager.changeState(
                  storeProvider,
                  getTaskId(job, instanceId),
                  Optional.absent(),
                  s,
                  Optional.absent())));
    }
  }

  private static final Ordering<IJobInstanceUpdateEvent> EVENT_ORDER = Ordering.natural()
      .onResultOf(IJobInstanceUpdateEvent::getTimestampMs);
  private static final Function<IJobInstanceUpdateEvent, Integer> EVENT_TO_INSTANCE =
      IJobInstanceUpdateEvent::getInstanceId;

  private IJobUpdateDetails getDetails() {
    return storage.read(
        storeProvider -> storeProvider.getJobUpdateStore().fetchJobUpdate(UPDATE_ID).get());
  }

  private IJobUpdateDetails getDetails(IJobUpdateKey key) {
    return storage.read(
        storeProvider -> storeProvider.getJobUpdateStore().fetchJobUpdate(key).get());
  }

  private void assertLatestUpdateMessage(String expected) {
    IJobUpdateDetails details = getDetails();
    assertEquals(expected, Iterables.getLast(details.getUpdateEvents()).getMessage());
  }

  private void assertState(
      JobUpdateStatus expected,
      Multimap<Integer, JobUpdateAction> expectedActions) {

    assertStateUpdate(UPDATE_ID, expected, expectedActions);
  }

  private void assertStateUpdate(
      IJobUpdateKey key,
      JobUpdateStatus expected,
      Multimap<Integer, JobUpdateAction> expectedActions) {

    IJobUpdateDetails details = getDetails(key);
    Iterable<IJobInstanceUpdateEvent> orderedEvents =
        EVENT_ORDER.sortedCopy(details.getInstanceEvents());
    Multimap<Integer, IJobInstanceUpdateEvent> eventsByInstance =
        Multimaps.index(orderedEvents, EVENT_TO_INSTANCE);
    Multimap<Integer, JobUpdateAction> actionsByInstance =
        Multimaps.transformValues(eventsByInstance, JobUpdateControllerImpl.EVENT_TO_ACTION);
    assertEquals(expectedActions, actionsByInstance);
    assertEquals(expected, details.getUpdate().getSummary().getState().getStatus());
  }

  private IExpectationSetters<String> expectTaskKilled() {
    driver.killTask(EasyMock.anyObject());
    return expectLastCall();
  }

  private void insertPendingTasks(ITaskConfig task, Set<Integer> instanceIds) {
    storage.write((NoResult.Quiet) storeProvider ->
        stateManager.insertPendingTasks(storeProvider, task, instanceIds));
  }

  private void insertInitialTasks(IJobUpdate update) {
    storage.write((NoResult.Quiet) storeProvider -> {
      for (IInstanceTaskConfig config : update.getInstructions().getInitialState()) {
        insertPendingTasks(config.getTask(), expandInstanceIds(ImmutableSet.of(config)));
      }
    });
  }

  private void assertJobState(IJobKey job, Map<Integer, ITaskConfig> expected) {
    Iterable<IScheduledTask> tasks =
        Storage.Util.fetchTasks(storage, Query.jobScoped(job).active());

    Map<Integer, IScheduledTask> tasksByInstance =
        Maps.uniqueIndex(tasks, Tasks::getInstanceId);
    assertEquals(
        expected,
        ImmutableMap.copyOf(Maps.transformValues(tasksByInstance, Tasks::getConfig)));
  }

  @Test
  public void testSuccessfulUpdate() throws Exception {
    expectTaskKilled();

    control.replay();

    IJobUpdate update = makeJobUpdate(
        // No-op - task is already matching the new config.
        makeInstanceConfig(0, 0, NEW_CONFIG),
        // Task needing update.
        makeInstanceConfig(2, 2, OLD_CONFIG));
    insertInitialTasks(update);

    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 2, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    ImmutableMultimap.Builder<Integer, JobUpdateAction> actions = ImmutableMultimap.builder();

    // Instance 1 is added
    updater.start(update, AUDIT);
    actions.putAll(1, INSTANCE_UPDATING);
    assertState(ROLLING_FORWARD, actions.build());
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);

    // Updates may be paused for arbitrarily-long amounts of time, and the updater should not
    // take action while paused.
    updater.pause(UPDATE_ID, AUDIT);
    updater.pause(UPDATE_ID, AUDIT);  // Pausing again is a no-op.
    assertState(ROLL_FORWARD_PAUSED, actions.build());
    clock.advance(ONE_DAY);
    changeState(JOB, 1, FAILED, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 2, FAILED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);
    updater.resume(UPDATE_ID, AUDIT);

    actions.putAll(1, INSTANCE_UPDATED).put(2, INSTANCE_UPDATING);
    assertState(ROLLING_FORWARD, actions.build());

    // A task outside the scope of the update should be ignored by the updater.
    insertPendingTasks(NEW_CONFIG, ImmutableSet.of(100));

    // Instance 2 is updated
    changeState(JOB, 2, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    actions.put(2, INSTANCE_UPDATED);
    assertState(ROLLED_FORWARD, actions.build());
    assertJobState(
        JOB,
        ImmutableMap.of(0, NEW_CONFIG, 1, NEW_CONFIG, 2, NEW_CONFIG, 100, NEW_CONFIG));

    // Attempting to abort a finished update should fail.
    try {
      updater.abort(UPDATE_ID, AUDIT);
      fail("It should not be possible to abort a completed update.");
    } catch (UpdateStateException e) {
      // Expected.
    }
  }

  @Test
  public void testSuccessfulCoordinatedUpdate() throws Exception {
    expectTaskKilled().times(2);

    control.replay();

    JobUpdate builder = makeJobUpdate(
        // No-op - task is already matching the new config.
        makeInstanceConfig(0, 0, NEW_CONFIG),
        // Tasks needing update.
        makeInstanceConfig(1, 2, OLD_CONFIG)).newBuilder();

    builder.getInstructions().getSettings().setBlockIfNoPulsesAfterMs((int) PULSE_TIMEOUT_MS);
    insertInitialTasks(IJobUpdate.build(builder));

    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 2, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    ImmutableMultimap.Builder<Integer, JobUpdateAction> actions = ImmutableMultimap.builder();
    updater.start(IJobUpdate.build(builder), AUDIT);

    // The update is blocked initially waiting for a pulse.
    assertState(ROLL_FORWARD_AWAITING_PULSE, actions.build());

    // Pulse arrives and update starts.
    assertEquals(JobUpdatePulseStatus.OK, updater.pulse(UPDATE_ID));
    changeState(JOB, 1, KILLED, ASSIGNED, STARTING, RUNNING);
    actions.put(1, INSTANCE_UPDATING);
    assertState(ROLLING_FORWARD, actions.build());
    clock.advance(WATCH_TIMEOUT);
    actions.put(1, INSTANCE_UPDATED);

    // The update is blocked due to expired pulse timeout.
    clock.advance(Amount.of(PULSE_TIMEOUT_MS, Time.MILLISECONDS));
    actions.put(2, INSTANCE_UPDATING);
    changeState(JOB, 2, KILLED);
    assertState(ROLL_FORWARD_AWAITING_PULSE, actions.build());
    assertLatestUpdateMessage(JobUpdateControllerImpl.PULSE_TIMEOUT_MESSAGE);

    // Pulse arrives and instance 2 is updated.
    assertEquals(JobUpdatePulseStatus.OK, updater.pulse(UPDATE_ID));
    changeState(JOB, 2, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);
    actions.put(2, INSTANCE_UPDATED);

    assertState(ROLLED_FORWARD, actions.build());
    assertJobState(JOB, ImmutableMap.of(0, NEW_CONFIG, 1, NEW_CONFIG, 2, NEW_CONFIG));
    assertEquals(JobUpdatePulseStatus.FINISHED, updater.pulse(UPDATE_ID));
  }

  @Test
  public void testRecoverCoordinatedUpdateFromStorage() throws Exception {
    expectTaskKilled().times(2);

    control.replay();

    JobUpdate builder =
        setInstanceCount(makeJobUpdate(makeInstanceConfig(0, 1, OLD_CONFIG)), 2).newBuilder();
    builder.getInstructions().getSettings().setBlockIfNoPulsesAfterMs((int) PULSE_TIMEOUT_MS);
    IJobUpdate update = IJobUpdate.build(builder);
    insertInitialTasks(update);
    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    clock.advance(ONE_DAY);

    storage.write((NoResult.Quiet) storeProvider ->
        saveJobUpdate(storeProvider.getJobUpdateStore(), update, ROLLING_FORWARD));

    clock.advance(ONE_MINUTE);

    subscriber.startAsync().awaitRunning();
    ImmutableMultimap.Builder<Integer, JobUpdateAction> actions = ImmutableMultimap.builder();

    // The update is blocked initially waiting for a pulse.
    assertState(ROLL_FORWARD_AWAITING_PULSE, actions.build());

    assertEquals(JobUpdatePulseStatus.OK, updater.pulse(UPDATE_ID));

    // Instance 0 is updated.
    changeState(JOB, 0, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    // Instance 1 is updated.
    changeState(JOB, 1, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    actions.putAll(0, INSTANCE_UPDATING, INSTANCE_UPDATED)
        .putAll(1, INSTANCE_UPDATING, INSTANCE_UPDATED);

    assertState(ROLLED_FORWARD, actions.build());
    assertEquals(JobUpdatePulseStatus.FINISHED, updater.pulse(UPDATE_ID));
  }

  @Test
  public void testRecoverLongPulseTimeoutCoordinatedUpdateFromStorage() throws Exception {
    // A brief failover in the middle of a rolling forward update with a long pulse timeout should
    // mean that after scheduler startup the update is not waiting for a pulse.
    expectTaskKilled().times(1);

    control.replay();

    JobUpdate builder =
        setInstanceCount(makeJobUpdate(makeInstanceConfig(0, 0, OLD_CONFIG)), 1).newBuilder();
    builder.getInstructions().getSettings()
        .setBlockIfNoPulsesAfterMs(Ints.checkedCast(ONE_HOUR.as(Time.MILLISECONDS)));
    IJobUpdate update = IJobUpdate.build(builder);
    insertInitialTasks(update);
    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    clock.advance(ONE_DAY);

    storage.write((NoResult.Quiet) storeProvider ->
        saveJobUpdate(storeProvider.getJobUpdateStore(), update, ROLL_FORWARD_AWAITING_PULSE));

    // The first pulse comes after one minute
    clock.advance(ONE_MINUTE);

    storage.write(
        (NoResult.Quiet) storeProvider ->
            saveJobUpdateEvent(storeProvider.getJobUpdateStore(), update, ROLLING_FORWARD));

    clock.advance(ONE_MINUTE);

    subscriber.startAsync().awaitRunning();
    ImmutableMultimap.Builder<Integer, JobUpdateAction> actions = ImmutableMultimap.builder();

    actions.putAll(0, INSTANCE_UPDATING);
    // Since the pulse interval is so large and the downtime was so short, the update does not need
    // to wait for a pulse.
    assertState(ROLLING_FORWARD, actions.build());

    // Instance 0 is updated.
    changeState(JOB, 0, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    actions.putAll(0, INSTANCE_UPDATED);

    assertState(ROLLED_FORWARD, actions.build());
    assertEquals(JobUpdatePulseStatus.FINISHED, updater.pulse(UPDATE_ID));
  }

  @Test
  public void testRecoverAwaitingPulseFromStorage() throws Exception {
    expectTaskKilled();

    control.replay();

    JobUpdate builder =
        setInstanceCount(makeJobUpdate(makeInstanceConfig(0, 0, OLD_CONFIG)), 1).newBuilder();
    builder.getInstructions().getSettings().setBlockIfNoPulsesAfterMs((int) PULSE_TIMEOUT_MS);
    IJobUpdate update = IJobUpdate.build(builder);
    insertInitialTasks(update);
    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    clock.advance(ONE_DAY);

    storage.write((NoResult.Quiet) storeProvider ->
        saveJobUpdate(storeProvider.getJobUpdateStore(), update, ROLL_FORWARD_AWAITING_PULSE));

    subscriber.startAsync().awaitRunning();
    ImmutableMultimap.Builder<Integer, JobUpdateAction> actions = ImmutableMultimap.builder();

    assertState(ROLL_FORWARD_AWAITING_PULSE, actions.build());
    assertEquals(JobUpdatePulseStatus.OK, updater.pulse(UPDATE_ID));

    changeState(JOB, 0, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);
    actions.putAll(0, INSTANCE_UPDATING, INSTANCE_UPDATED);

    assertState(ROLLED_FORWARD, actions.build());
    assertEquals(JobUpdatePulseStatus.FINISHED, updater.pulse(UPDATE_ID));
  }

  @Test
  public void testRecoverCoordinatedPausedFromStorage() throws Exception {
    expectTaskKilled();

    control.replay();

    JobUpdate builder =
        setInstanceCount(makeJobUpdate(makeInstanceConfig(0, 0, OLD_CONFIG)), 1).newBuilder();
    builder.getInstructions().getSettings().setBlockIfNoPulsesAfterMs((int) PULSE_TIMEOUT_MS);
    IJobUpdate update = IJobUpdate.build(builder);
    insertInitialTasks(update);
    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    clock.advance(ONE_DAY);

    storage.write((NoResult.Quiet) storeProvider ->
        saveJobUpdate(storeProvider.getJobUpdateStore(), update, ROLL_FORWARD_PAUSED));

    subscriber.startAsync().awaitRunning();
    ImmutableMultimap.Builder<Integer, JobUpdateAction> actions = ImmutableMultimap.builder();

    assertState(ROLL_FORWARD_PAUSED, actions.build());
    assertEquals(JobUpdatePulseStatus.OK, updater.pulse(UPDATE_ID));

    updater.resume(UPDATE_ID, AUDIT);

    changeState(JOB, 0, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);
    actions.putAll(0, INSTANCE_UPDATING, INSTANCE_UPDATED);

    assertState(ROLLED_FORWARD, actions.build());
    assertEquals(JobUpdatePulseStatus.FINISHED, updater.pulse(UPDATE_ID));
  }

  @Test
  public void testResumeToAwaitingPulse() throws Exception {
    expectTaskKilled().times(2);

    control.replay();

    JobUpdate builder =
        setInstanceCount(makeJobUpdate(makeInstanceConfig(0, 1, OLD_CONFIG)), 2).newBuilder();
    builder.getInstructions().getSettings().setBlockIfNoPulsesAfterMs((int) PULSE_TIMEOUT_MS);
    IJobUpdate update = IJobUpdate.build(builder);
    insertInitialTasks(update);
    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    clock.advance(ONE_DAY);

    ImmutableMultimap.Builder<Integer, JobUpdateAction> actions = ImmutableMultimap.builder();
    updater.start(IJobUpdate.build(builder), AUDIT);

    // The update is blocked initially waiting for a pulse.
    assertState(ROLL_FORWARD_AWAITING_PULSE, actions.build());

    // Pause the awaiting pulse update.
    updater.pause(UPDATE_ID, AUDIT);
    assertState(ROLL_FORWARD_PAUSED, actions.build());

    // Resume into awaiting pulse state.
    updater.resume(UPDATE_ID, AUDIT);
    assertState(ROLL_FORWARD_AWAITING_PULSE, actions.build());

    assertEquals(JobUpdatePulseStatus.OK, updater.pulse(UPDATE_ID));

    // Instance 0 is updated.
    changeState(JOB, 0, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    // Instance 1 is updated.
    changeState(JOB, 1, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    actions.putAll(0, INSTANCE_UPDATING, INSTANCE_UPDATED)
        .putAll(1, INSTANCE_UPDATING, INSTANCE_UPDATED);

    assertState(ROLLED_FORWARD, actions.build());
    assertEquals(JobUpdatePulseStatus.FINISHED, updater.pulse(UPDATE_ID));
  }

  @Test
  public void testPulsePausedUpdate() throws Exception {
    expectTaskKilled().times(2);

    control.replay();

    JobUpdate builder = makeJobUpdate(
        // No-op - task is already matching the new config.
        makeInstanceConfig(0, 0, NEW_CONFIG),
        // Tasks needing update.
        makeInstanceConfig(1, 2, OLD_CONFIG)).newBuilder();

    builder.getInstructions().getSettings().setBlockIfNoPulsesAfterMs((int) PULSE_TIMEOUT_MS);
    insertInitialTasks(IJobUpdate.build(builder));

    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 2, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    ImmutableMultimap.Builder<Integer, JobUpdateAction> actions = ImmutableMultimap.builder();
    updater.start(IJobUpdate.build(builder), AUDIT);

    // The update is blocked initially waiting for a pulse.
    assertState(ROLL_FORWARD_AWAITING_PULSE, actions.build());

    // Pulse arrives and update starts.
    assertEquals(JobUpdatePulseStatus.OK, updater.pulse(UPDATE_ID));
    changeState(JOB, 1, KILLED, ASSIGNED, STARTING, RUNNING);
    actions.put(1, INSTANCE_UPDATING);
    clock.advance(WATCH_TIMEOUT);
    actions.put(1, INSTANCE_UPDATED);
    actions.put(2, INSTANCE_UPDATING);
    clock.advance(Amount.of(PULSE_TIMEOUT_MS, Time.MILLISECONDS));

    // Update is paused
    updater.pause(UPDATE_ID, AUDIT);
    assertState(ROLL_FORWARD_PAUSED, actions.build());

    // A paused update is pulsed.
    assertEquals(JobUpdatePulseStatus.OK, updater.pulse(UPDATE_ID));

    // Update is resumed
    updater.resume(UPDATE_ID, AUDIT);
    assertState(ROLLING_FORWARD, actions.build());

    // Instance 2 is updated.
    changeState(JOB, 2, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);
    actions.put(2, INSTANCE_UPDATED);

    assertState(ROLLED_FORWARD, actions.build());
    assertJobState(JOB, ImmutableMap.of(0, NEW_CONFIG, 1, NEW_CONFIG, 2, NEW_CONFIG));

    assertEquals(JobUpdatePulseStatus.FINISHED, updater.pulse(UPDATE_ID));
  }

  @Test
  public void testUnblockDeletedUpdate() throws Exception {
    control.replay();

    JobUpdate builder =
        setInstanceCount(makeJobUpdate(makeInstanceConfig(0, 1, OLD_CONFIG)), 2).newBuilder();
    builder.getInstructions().getSettings().setBlockIfNoPulsesAfterMs((int) PULSE_TIMEOUT_MS);
    IJobUpdate update = IJobUpdate.build(builder);
    insertInitialTasks(update);
    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    clock.advance(ONE_DAY);

    storage.write((NoResult.Quiet) storeProvider ->
        saveJobUpdate(storeProvider.getJobUpdateStore(), update, ROLLING_FORWARD));

    clock.advance(ONE_MINUTE);

    subscriber.startAsync().awaitRunning();

    ImmutableMultimap.Builder<Integer, JobUpdateAction> actions = ImmutableMultimap.builder();
    // The update is blocked initially waiting for a pulse.
    assertState(ROLL_FORWARD_AWAITING_PULSE, actions.build());

    storage.write((NoResult.Quiet) storeProvider -> {
      storeProvider.getJobUpdateStore().deleteAllUpdates();
    });

    // The pulse still returns OK but the error is handled.
    assertEquals(JobUpdatePulseStatus.OK, updater.pulse(UPDATE_ID));
  }

  @Test
  public void testPulseInvalidUpdateId() throws Exception {
    control.replay();

    assertEquals(
        JobUpdatePulseStatus.FINISHED,
        updater.pulse(IJobUpdateKey.build(new JobUpdateKey(JOB.newBuilder(), "invalid"))));
  }

  @Test(expected = IllegalStateException.class)
  public void testShutdownOnFailedPulse() throws Exception {
    // Missing kill expectation will trigger failure.
    shutdownCommand.execute();
    expectLastCall().andAnswer(() -> {
      throw new IllegalStateException("Expected shutdown triggered.");
    });

    control.replay();

    JobUpdate builder = makeJobUpdate(
        // No-op - task is already matching the new config.
        makeInstanceConfig(0, 0, NEW_CONFIG),
        // Tasks needing update.
        makeInstanceConfig(1, 2, OLD_CONFIG)).newBuilder();

    builder.getInstructions().getSettings().setBlockIfNoPulsesAfterMs((int) PULSE_TIMEOUT_MS);
    insertInitialTasks(IJobUpdate.build(builder));

    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 2, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    ImmutableMultimap.Builder<Integer, JobUpdateAction> actions = ImmutableMultimap.builder();
    updater.start(IJobUpdate.build(builder), AUDIT);

    // The update is blocked initially waiting for a pulse.
    assertState(ROLL_FORWARD_AWAITING_PULSE, actions.build());

    // Pulse arrives and update starts.
    assertEquals(JobUpdatePulseStatus.OK, updater.pulse(UPDATE_ID));
    changeState(JOB, 1, KILLED, ASSIGNED, STARTING, RUNNING);
  }

  @Test
  public void testSuccessfulBatchedUpdate() throws Exception {
    expectTaskKilled().times(3);

    control.replay();

    JobUpdate builder = makeJobUpdate(makeInstanceConfig(0, 2, OLD_CONFIG)).newBuilder();
    builder.getInstructions().getSettings()
        .setWaitForBatchCompletion(true)
        .setUpdateGroupSize(2);
    IJobUpdate update = IJobUpdate.build(builder);
    insertInitialTasks(update);

    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 2, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    ImmutableMultimap.Builder<Integer, JobUpdateAction> actions = ImmutableMultimap.builder();

    // Instances 0 and 1 are updated.
    updater.start(update, AUDIT);
    actions.putAll(0, INSTANCE_UPDATING)
        .putAll(1, INSTANCE_UPDATING);
    assertState(ROLLING_FORWARD, actions.build());
    changeState(JOB, 1, FINISHED, ASSIGNED, STARTING, RUNNING);
    clock.advance(Amount.of(WATCH_TIMEOUT.getValue() / 2, Time.MILLISECONDS));
    changeState(JOB, 0, FINISHED, ASSIGNED, STARTING, RUNNING);
    clock.advance(Amount.of(WATCH_TIMEOUT.getValue() / 2, Time.MILLISECONDS));

    // Instance 1 finished first, but update does not yet proceed until 0 finishes.
    actions.putAll(1, INSTANCE_UPDATED);
    assertState(ROLLING_FORWARD, actions.build());
    clock.advance(WATCH_TIMEOUT);
    actions.putAll(0, INSTANCE_UPDATED);

    // Instance 2 is updated.
    changeState(JOB, 2, FINISHED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);
    actions.putAll(2, INSTANCE_UPDATING, INSTANCE_UPDATED);
    assertState(ROLLED_FORWARD, actions.build());

    assertJobState(
        JOB,
        ImmutableMap.of(0, NEW_CONFIG, 1, NEW_CONFIG, 2, NEW_CONFIG));
  }

  @Test
  public void testUpdateSpecificInstances() throws Exception {
    expectTaskKilled();

    control.replay();

    JobUpdate builder =
        setInstanceCount(makeJobUpdate(makeInstanceConfig(0, 0, OLD_CONFIG)), 1).newBuilder();
    builder.getInstructions().getSettings().setUpdateOnlyTheseInstances(
        ImmutableSet.of(new Range(0, 0)));
    IJobUpdate update = IJobUpdate.build(builder);
    insertPendingTasks(OLD_CONFIG, ImmutableSet.of(0, 1));

    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    // Instance 0 is updated
    updater.start(update, AUDIT);
    changeState(JOB, 0, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    ImmutableMultimap.Builder<Integer, JobUpdateAction> actions = ImmutableMultimap.builder();
    assertState(
        ROLLED_FORWARD,
        actions.putAll(0, INSTANCE_UPDATING, INSTANCE_UPDATED).build());
    assertJobState(
        JOB,
        ImmutableMap.of(0, NEW_CONFIG, 1, OLD_CONFIG));
  }

  @Test
  public void testUpdateSpecificInstancesSkipUnchanged() throws Exception {
    control.replay();

    JobUpdate builder = makeJobUpdate().newBuilder();

    builder.getInstructions().getDesiredState().setInstances(ImmutableSet.of(new Range(1, 1)));
    builder.getInstructions().getSettings().setUpdateOnlyTheseInstances(
        ImmutableSet.of(new Range(0, 1)));
    IJobUpdate update = IJobUpdate.build(builder);
    insertPendingTasks(NEW_CONFIG, ImmutableSet.of(0));
    insertPendingTasks(OLD_CONFIG, ImmutableSet.of(2));

    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 2, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    // Instance 1 is added, while instance 0 is skipped
    updater.start(update, AUDIT);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    ImmutableMultimap.Builder<Integer, JobUpdateAction> actions = ImmutableMultimap.builder();
    assertState(
        ROLLED_FORWARD,
        actions.putAll(1, INSTANCE_UPDATING, INSTANCE_UPDATED).build());
    assertJobState(
        JOB,
        ImmutableMap.of(0, NEW_CONFIG, 1, NEW_CONFIG, 2, OLD_CONFIG));
  }

  @Test
  public void testRollback() throws Exception {
    expectTaskKilled().times(4);

    control.replay();

    IJobUpdate update = makeJobUpdate(
            makeInstanceConfig(0, 0, OLD_CONFIG),
            makeInstanceConfig(2, 3, OLD_CONFIG));
    insertInitialTasks(update);

    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 2, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 3, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    ImmutableMultimap.Builder<Integer, JobUpdateAction> actions = ImmutableMultimap.builder();

    // Instance 1 is added.
    updater.start(update, AUDIT);
    actions.putAll(1, INSTANCE_UPDATING);
    assertState(ROLLING_FORWARD, actions.build());
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    // Instance 0 is updated.
    changeState(JOB, 0, KILLED, ASSIGNED, STARTING, RUNNING);
    actions.putAll(1, INSTANCE_UPDATED)
        .putAll(0, INSTANCE_UPDATING, INSTANCE_UPDATED);
    clock.advance(WATCH_TIMEOUT);

    // Instance 2 is updated, but fails.
    changeState(JOB, 2, KILLED, ASSIGNED, STARTING, RUNNING);
    actions.putAll(2, INSTANCE_UPDATING, INSTANCE_UPDATE_FAILED, INSTANCE_ROLLING_BACK);
    clock.advance(FLAPPING_THRESHOLD);
    changeState(JOB, 2, FAILED);

    // Instance 2 is rolled back.
    assertState(ROLLING_BACK, actions.build());
    assertLatestUpdateMessage(JobUpdateControllerImpl.failureMessage(2, Failure.EXITED));
    changeState(JOB, 2, ASSIGNED, STARTING, RUNNING);
    actions.putAll(0, INSTANCE_ROLLING_BACK)
        .putAll(2, INSTANCE_ROLLED_BACK);
    clock.advance(WATCH_TIMEOUT);

    // A rollback may be paused.
    updater.pause(UPDATE_ID, AUDIT);
    assertState(ROLL_BACK_PAUSED, actions.build());
    clock.advance(ONE_DAY);
    updater.resume(UPDATE_ID, AUDIT);
    assertState(ROLLING_BACK, actions.build());

    // Instance 0 is rolled back.
    changeState(JOB, 0, KILLED, ASSIGNED, STARTING, RUNNING);
    actions.putAll(0, INSTANCE_ROLLED_BACK);
    clock.advance(WATCH_TIMEOUT);

    // Instance 1 is removed.
    changeState(JOB, 1, KILLED);
    actions.putAll(1, INSTANCE_ROLLING_BACK, INSTANCE_ROLLED_BACK);
    clock.advance(WATCH_TIMEOUT);

    assertState(ROLLED_BACK, actions.build());
    assertJobState(JOB, ImmutableMap.of(0, OLD_CONFIG, 2, OLD_CONFIG, 3, OLD_CONFIG));
  }

  @Test
  public void testRollbackDisabled() throws Exception {
    expectTaskKilled().times(2);

    control.replay();

    JobUpdate builder = makeJobUpdate(
        makeInstanceConfig(0, 0, OLD_CONFIG),
        makeInstanceConfig(2, 3, OLD_CONFIG))
        .newBuilder();
    builder.getInstructions().getSettings().setRollbackOnFailure(false);
    IJobUpdate update = IJobUpdate.build(builder);
    insertInitialTasks(update);

    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 2, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 3, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    ImmutableMultimap.Builder<Integer, JobUpdateAction> actions = ImmutableMultimap.builder();

    // Instance 1 is added.
    updater.start(update, AUDIT);
    actions.putAll(1, INSTANCE_UPDATING);
    assertState(ROLLING_FORWARD, actions.build());
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    // Instance 0 is updated.
    changeState(JOB, 0, KILLED, ASSIGNED, STARTING, RUNNING);
    actions.putAll(1, INSTANCE_UPDATED)
        .putAll(0, INSTANCE_UPDATING, INSTANCE_UPDATED);
    clock.advance(WATCH_TIMEOUT);

    // Instance 2 is updated, but fails.
    changeState(JOB, 2, KILLED, ASSIGNED, STARTING, RUNNING);
    actions.putAll(2, INSTANCE_UPDATING, INSTANCE_UPDATE_FAILED);
    clock.advance(FLAPPING_THRESHOLD);
    changeState(JOB, 2, FAILED);
    clock.advance(WATCH_TIMEOUT);

    // Rollback is disabled, update fails.
    assertState(JobUpdateStatus.FAILED, actions.build());
  }

  @Test
  public void testAbort() throws Exception {
    expectTaskKilled();

    control.replay();

    IJobUpdate update = makeJobUpdate(makeInstanceConfig(0, 2, OLD_CONFIG));
    insertInitialTasks(update);

    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 2, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    // Instance 0 is updated
    updater.start(update, AUDIT);
    changeState(JOB, 0, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    ImmutableMultimap.Builder<Integer, JobUpdateAction> actions = ImmutableMultimap.builder();
    actions.putAll(0, INSTANCE_UPDATING, INSTANCE_UPDATED)
        .putAll(1, INSTANCE_UPDATING);

    updater.abort(UPDATE_ID, AUDIT);
    assertState(ABORTED, actions.build());
    clock.advance(WATCH_TIMEOUT);
    assertJobState(JOB, ImmutableMap.of(0, NEW_CONFIG, 1, NEW_CONFIG, 2, OLD_CONFIG));
  }

  @Test
  public void testRollbackFailed() throws Exception {
    expectTaskKilled().times(2);

    control.replay();

    IJobUpdate update = makeJobUpdate(
        makeInstanceConfig(0, 2, OLD_CONFIG));
    insertInitialTasks(update);

    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 2, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    ImmutableMultimap.Builder<Integer, JobUpdateAction> actions = ImmutableMultimap.builder();

    // Instance 0 is updated.
    updater.start(update, AUDIT);
    actions.putAll(0, INSTANCE_UPDATING);
    assertState(ROLLING_FORWARD, actions.build());
    changeState(JOB, 0, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    // Instance 1 is updated, but fails.
    changeState(JOB, 1, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(FLAPPING_THRESHOLD);
    changeState(JOB, 1, FAILED);

    // Instance 1 is rolled back, but fails.
    actions.putAll(0, INSTANCE_UPDATED)
        .putAll(1, INSTANCE_UPDATING, INSTANCE_UPDATE_FAILED, INSTANCE_ROLLING_BACK);
    assertState(ROLLING_BACK, actions.build());
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    clock.advance(FLAPPING_THRESHOLD);
    changeState(JOB, 1, FAILED);

    actions.putAll(1, INSTANCE_ROLLBACK_FAILED);
    assertState(JobUpdateStatus.FAILED, actions.build());
    clock.advance(WATCH_TIMEOUT);
    assertJobState(JOB, ImmutableMap.of(0, NEW_CONFIG, 1, OLD_CONFIG, 2, OLD_CONFIG));
  }

  private void expectInvalid(JobUpdate update)
      throws UpdateStateException, UpdateConfigurationException {

    try {
      updater.start(IJobUpdate.build(update), AUDIT);
      fail();
    } catch (IllegalArgumentException e) {
      // Expected.
    }
  }

  @Test
  public void testStartInvalidUpdate() throws Exception {
    control.replay();

    JobUpdate update = makeJobUpdate().newBuilder();
    update.getInstructions().getSettings().setUpdateGroupSize(-1);
    expectInvalid(update);

    update = makeJobUpdate().newBuilder();
    update.getInstructions().getSettings().setMinWaitInInstanceRunningMs(-1);
    expectInvalid(update);
  }

  @Test
  public void testConfigurationPolicyChange() throws Exception {
    // Simulates a change in input validation after a job update has been persisted.

    expectTaskKilled().times(2);

    control.replay();

    IJobUpdate update = setInstanceCount(makeJobUpdate(makeInstanceConfig(0, 1, OLD_CONFIG)), 2);
    insertInitialTasks(update);

    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    ImmutableMultimap.Builder<Integer, JobUpdateAction> actions = ImmutableMultimap.builder();

    // Instance 0 is updated
    updater.start(update, AUDIT);
    actions.putAll(0, INSTANCE_UPDATING);
    assertState(ROLLING_FORWARD, actions.build());

    storage.write((NoResult.Quiet) storeProvider -> {
      JobUpdateStore.Mutable store = storeProvider.getJobUpdateStore();
      store.deleteAllUpdates();

      JobUpdate builder = update.newBuilder();
      builder.getInstructions().getSettings().setUpdateGroupSize(0);
      saveJobUpdate(store, IJobUpdate.build(builder), ROLLING_FORWARD);
    });

    changeState(JOB, 0, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    // Instance 1 is updated, but fails.
    changeState(JOB, 1, KILLED, ASSIGNED, STARTING, RUNNING, FAILED);
    // Actions is reset here since we wiped the updates tables earlier in the test case.
    actions = ImmutableMultimap.builder();
    actions.putAll(0, INSTANCE_UPDATED)
        .putAll(1, INSTANCE_UPDATING, INSTANCE_UPDATE_FAILED);
    clock.advance(WATCH_TIMEOUT);

    assertState(ERROR, actions.build());
  }

  private void saveJobUpdate(
      JobUpdateStore.Mutable store,
      IJobUpdate update,
      JobUpdateStatus status) {

    store.saveJobUpdate(update);
    saveJobUpdateEvent(store, update, status);
  }

  private void saveJobUpdateEvent(
      JobUpdateStore.Mutable store,
      IJobUpdate update,
      JobUpdateStatus status) {

    store.saveJobUpdateEvent(
        update.getSummary().getKey(),
        IJobUpdateEvent.build(
            new JobUpdateEvent()
                .setStatus(status)
                .setTimestampMs(clock.nowMillis())));
  }

  @Test
  public void testRecoverFromStorage() throws Exception {
    expectTaskKilled().times(2);

    control.replay();

    IJobUpdate update = setInstanceCount(makeJobUpdate(makeInstanceConfig(0, 1, OLD_CONFIG)), 2);
    insertInitialTasks(update);
    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    clock.advance(ONE_DAY);

    storage.write((NoResult.Quiet) storeProvider ->
        saveJobUpdate(storeProvider.getJobUpdateStore(), update, ROLLING_FORWARD));

    subscriber.startAsync().awaitRunning();

    // Instance 0 is updated.
    changeState(JOB, 0, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    // Instance 1 is updated.
    changeState(JOB, 1, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    ImmutableMultimap.Builder<Integer, JobUpdateAction> actions = ImmutableMultimap.builder();
    actions.putAll(0, INSTANCE_UPDATING, INSTANCE_UPDATED)
        .putAll(1, INSTANCE_UPDATING, INSTANCE_UPDATED);

    assertState(ROLLED_FORWARD, actions.build());
  }

  @Test
  public void testImmediatelySuccessfulUpdate() throws Exception {
    control.replay();

    IJobUpdate update = makeJobUpdate(makeInstanceConfig(0, 2, NEW_CONFIG));
    insertInitialTasks(update);
    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 2, ASSIGNED, STARTING, RUNNING);
    clock.advance(ONE_DAY);
    updater.start(update, AUDIT);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoopUpdateEmptyDiff() throws Exception {
    control.replay();

    IJobUpdate update = makeJobUpdate();
    JobUpdate builder = update.newBuilder();
    builder.getInstructions().unsetDesiredState();

    updater.start(IJobUpdate.build(builder), AUDIT);
  }

  @Test
  public void testSlowToScheduleTask() throws Exception {
    expectTaskKilled().times(2);

    control.replay();

    IJobUpdate update = setInstanceCount(makeJobUpdate(makeInstanceConfig(0, 1, OLD_CONFIG)), 2);
    insertInitialTasks(update);
    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);

    ImmutableMultimap.Builder<Integer, JobUpdateAction> actions = ImmutableMultimap.builder();

    // Instance 0 is updated.
    updater.start(update, AUDIT);
    actions.putAll(0, INSTANCE_UPDATING);
    assertState(ROLLING_FORWARD, actions.build());
    changeState(JOB, 0, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    // Instance 1 is not advancing past PENDING.
    changeState(JOB, 1, KILLED);
    actions.putAll(0, INSTANCE_UPDATED)
        .putAll(1, INSTANCE_UPDATING);
    assertState(ROLLING_FORWARD, actions.build());

    updater.abort(update.getSummary().getKey(), AUDIT);
    assertState(ABORTED, actions.build());
  }

  @Test
  public void testAddInstances() throws Exception {
    control.replay();

    IJobUpdate update = makeJobUpdate();
    insertPendingTasks(NEW_CONFIG, ImmutableSet.of(0, 1));

    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    ImmutableMultimap.Builder<Integer, JobUpdateAction> actions = ImmutableMultimap.builder();

    // Instance 2 is added
    updater.start(update, AUDIT);
    actions.putAll(2, INSTANCE_UPDATING);
    assertState(ROLLING_FORWARD, actions.build());
    changeState(JOB, 2, ASSIGNED, STARTING, RUNNING);

    clock.advance(WATCH_TIMEOUT);
    actions.putAll(2, INSTANCE_UPDATED);
    assertState(ROLLED_FORWARD, actions.build());

    assertJobState(
        JOB,
        ImmutableMap.of(0, NEW_CONFIG, 1, NEW_CONFIG, 2, NEW_CONFIG));
  }

  @Test
  public void testRemoveInstances() throws Exception {
    expectTaskKilled();

    control.replay();

    // Set instance count such that instance 1 is removed.
    IJobUpdate update = setInstanceCount(makeJobUpdate(makeInstanceConfig(0, 1, NEW_CONFIG)), 1);
    insertInitialTasks(update);

    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    ImmutableMultimap.Builder<Integer, JobUpdateAction> actions = ImmutableMultimap.builder();

    // Instance 1 is removed.
    updater.start(update, AUDIT);
    actions.putAll(1, INSTANCE_UPDATING);
    changeState(JOB, 1, KILLED);
    clock.advance(WATCH_TIMEOUT);

    actions.put(1, INSTANCE_UPDATED);
    assertState(ROLLED_FORWARD, actions.build());
    assertJobState(JOB, ImmutableMap.of(0, NEW_CONFIG));
  }

  @Test
  public void testBadPubsubUpdate() {
    control.replay();

    subscriber.taskChangedState(
        PubsubEvent.TaskStateChange.transition(IScheduledTask.build(new ScheduledTask()), RUNNING));
  }

  @Test(expected = UpdateStateException.class)
  public void testPauseUnknownUpdate() throws Exception {
    control.replay();

    updater.pause(UPDATE_ID, AUDIT);
  }

  @Test(expected = UpdateStateException.class)
  public void testResumeUnknownUpdate() throws Exception {
    control.replay();

    updater.resume(UPDATE_ID, AUDIT);
  }

  @Test
  public void testFailToRollbackCompletedUpdate() throws Exception {
    expectTaskKilled().times(3);

    control.replay();

    JobUpdate builder = makeJobUpdate(makeInstanceConfig(0, 2, OLD_CONFIG)).newBuilder();
    builder.getInstructions().getSettings()
        .setWaitForBatchCompletion(true)
        .setUpdateGroupSize(2);
    IJobUpdate update = IJobUpdate.build(builder);
    insertInitialTasks(update);

    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 2, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    ImmutableMultimap.Builder<Integer, JobUpdateAction> actions = ImmutableMultimap.builder();

    // Instances 0 and 1 are updated.
    updater.start(update, AUDIT);
    actions.putAll(0, INSTANCE_UPDATING)
        .putAll(1, INSTANCE_UPDATING);
    assertState(ROLLING_FORWARD, actions.build());
    changeState(JOB, 1, FINISHED, ASSIGNED, STARTING, RUNNING);
    clock.advance(Amount.of(WATCH_TIMEOUT.getValue() / 2, Time.MILLISECONDS));
    changeState(JOB, 0, FINISHED, ASSIGNED, STARTING, RUNNING);
    clock.advance(Amount.of(WATCH_TIMEOUT.getValue() / 2, Time.MILLISECONDS));

    // Instance 1 finished first, but update does not yet proceed until 0 finishes.
    actions.putAll(1, INSTANCE_UPDATED);
    assertState(ROLLING_FORWARD, actions.build());
    clock.advance(WATCH_TIMEOUT);
    actions.putAll(0, INSTANCE_UPDATED);

    // Instance 2 is updated.
    changeState(JOB, 2, FINISHED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);
    actions.putAll(2, INSTANCE_UPDATING, INSTANCE_UPDATED);
    assertState(ROLLED_FORWARD, actions.build());

    assertJobState(
        JOB,
        ImmutableMap.of(0, NEW_CONFIG, 1, NEW_CONFIG, 2, NEW_CONFIG));

    try {
      updater.rollback(UPDATE_ID, AUDIT);
      fail();
    } catch (UpdateStateException e) {
      // Expected.
    }
  }

  @Test
  public void testRollbackDuringUpgrade() throws Exception {
    expectTaskKilled().times(5);

    control.replay();

    JobUpdate builder = makeJobUpdate(makeInstanceConfig(0, 2, OLD_CONFIG)).newBuilder();
    builder.getInstructions().getSettings()
        .setWaitForBatchCompletion(true)
        .setUpdateGroupSize(2);
    IJobUpdate update = IJobUpdate.build(builder);
    insertInitialTasks(update);

    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 2, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    ImmutableMultimap.Builder<Integer, JobUpdateAction> actions = ImmutableMultimap.builder();

    // Instances 0 and 1 are updated.
    updater.start(update, AUDIT);
    actions.putAll(0, INSTANCE_UPDATING)
        .putAll(1, INSTANCE_UPDATING);
    assertState(ROLLING_FORWARD, actions.build());
    changeState(JOB, 1, FINISHED, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 0, FINISHED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    actions.putAll(0, INSTANCE_UPDATED)
        .putAll(1, INSTANCE_UPDATED)
        .putAll(2, INSTANCE_UPDATING);
    assertState(ROLLING_FORWARD, actions.build());
    clock.advance(WATCH_TIMEOUT);

    updater.rollback(UPDATE_ID, AUDIT);

    actions.putAll(1, INSTANCE_ROLLING_BACK);
    actions.putAll(2, INSTANCE_ROLLING_BACK);
    changeState(JOB, 1, KILLED);
    changeState(JOB, 2, KILLED);
    clock.advance(WATCH_TIMEOUT);

    assertState(ROLLING_BACK, actions.build());
    clock.advance(WATCH_TIMEOUT);

    changeState(JOB, 2, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    actions.putAll(2, INSTANCE_ROLLED_BACK)
        .putAll(1, INSTANCE_ROLLED_BACK);
    changeState(JOB, 0, KILLED);
    actions.putAll(0, INSTANCE_ROLLING_BACK);
    clock.advance(WATCH_TIMEOUT);

    assertState(ROLLING_BACK, actions.build());

    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    actions.putAll(0, INSTANCE_ROLLED_BACK);
    clock.advance(WATCH_TIMEOUT);

    assertState(ROLLED_BACK, actions.build());

    assertJobState(
        JOB,
        ImmutableMap.of(0, OLD_CONFIG, 1, OLD_CONFIG, 2, OLD_CONFIG));
  }

  @Test
  public void testRollbackCoordinatedUpdate() throws Exception {
    control.replay();

    JobUpdate builder = makeJobUpdate(
        // No-op - task is already matching the new config.
        makeInstanceConfig(0, 0, NEW_CONFIG),
        // Tasks needing update.
        makeInstanceConfig(1, 2, OLD_CONFIG)).newBuilder();

    builder.getInstructions().getSettings().setBlockIfNoPulsesAfterMs((int) PULSE_TIMEOUT_MS);
    insertInitialTasks(IJobUpdate.build(builder));

    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 2, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    ImmutableMultimap.Builder<Integer, JobUpdateAction> actions = ImmutableMultimap.builder();
    updater.start(IJobUpdate.build(builder), AUDIT);

    // The update is blocked initially waiting for a pulse.
    assertState(ROLL_FORWARD_AWAITING_PULSE, actions.build());

    updater.rollback(UPDATE_ID, AUDIT);

    clock.advance(WATCH_TIMEOUT);
    assertState(ROLLED_BACK, actions.build());
  }

  @Test
  public void testRollbackPausedForwardUpdate() throws Exception {
    expectTaskKilled().times(2);

    control.replay();

    JobUpdate builder = makeJobUpdate(
        // No-op - task is already matching the new config.
        makeInstanceConfig(0, 0, NEW_CONFIG),
        // Tasks needing update.
        makeInstanceConfig(1, 2, OLD_CONFIG)).newBuilder();

    insertInitialTasks(IJobUpdate.build(builder));

    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 2, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    ImmutableMultimap.Builder<Integer, JobUpdateAction> actions = ImmutableMultimap.builder();
    updater.start(IJobUpdate.build(builder), AUDIT);

    actions.putAll(1, INSTANCE_UPDATING);
    assertState(ROLLING_FORWARD, actions.build());
    clock.advance(WATCH_TIMEOUT);
    changeState(JOB, 1, KILLED, ASSIGNED, STARTING, RUNNING);

    updater.pause(UPDATE_ID, AUDIT);
    assertState(ROLL_FORWARD_PAUSED, actions.build());

    updater.rollback(UPDATE_ID, AUDIT);

    actions.putAll(1, INSTANCE_ROLLING_BACK);
    clock.advance(WATCH_TIMEOUT);
    assertState(ROLLING_BACK, actions.build());

    actions.putAll(1, INSTANCE_ROLLED_BACK);
    changeState(JOB, 1, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);
    assertState(ROLLED_BACK, actions.build());

    assertJobState(
        JOB,
        ImmutableMap.of(0, NEW_CONFIG, 1, OLD_CONFIG, 2, OLD_CONFIG));
  }

  @Test
  public void testInProgressUpdate() throws Exception {
    control.replay();

    IJobUpdate inProgress = makeJobUpdate();
    storage.write((NoResult.Quiet) storeProvider ->
        saveJobUpdate(storeProvider.getJobUpdateStore(), inProgress, ROLLING_FORWARD));
    IJobUpdate anotherUpdate = makeJobUpdate();
    try {
      updater.start(anotherUpdate, AUDIT);
      fail("update cannot start when another is in-progress");
    } catch (UpdateInProgressException e) {
      // Expected.
      assertEquals(
          inProgress.getSummary().newBuilder().setState(new JobUpdateState(ROLLING_FORWARD, 0, 0)),
          e.getInProgressUpdateSummary().newBuilder());
    }
  }

  private static IJobUpdateSummary makeUpdateSummary(IJobUpdateKey key) {
    return IJobUpdateSummary.build(new JobUpdateSummary()
        .setUser("user")
        .setKey(key.newBuilder()));
  }

  private static IJobUpdate makeJobUpdate(IInstanceTaskConfig... configs) {
    JobUpdate builder = new JobUpdate()
        .setSummary(makeUpdateSummary(UPDATE_ID).newBuilder().setMetadata(METADATA))
        .setInstructions(new JobUpdateInstructions()
            .setDesiredState(new InstanceTaskConfig()
                .setTask(NEW_CONFIG.newBuilder())
                .setInstances(ImmutableSet.of(new Range(0, 2))))
            .setSettings(new JobUpdateSettings()
                .setUpdateGroupSize(1)
                .setRollbackOnFailure(true)
                .setMinWaitInInstanceRunningMs(WATCH_TIMEOUT.as(Time.MILLISECONDS).intValue())
                .setUpdateOnlyTheseInstances(ImmutableSet.of())));

    for (IInstanceTaskConfig config : configs) {
      builder.getInstructions().addToInitialState(config.newBuilder());
    }

    return IJobUpdate.build(builder);
  }

  private static IJobUpdate setInstanceCount(IJobUpdate update, int instanceCount) {
    JobUpdate builder = update.newBuilder();
    builder.getInstructions().getDesiredState().setInstances(
        ImmutableSet.of(new Range(0, instanceCount - 1)));
    return IJobUpdate.build(builder);
  }

  private static IInstanceTaskConfig makeInstanceConfig(int start, int end, ITaskConfig config) {
    return IInstanceTaskConfig.build(new InstanceTaskConfig()
        .setInstances(ImmutableSet.of(new Range(start, end)))
        .setTask(config.newBuilder()));
  }
}

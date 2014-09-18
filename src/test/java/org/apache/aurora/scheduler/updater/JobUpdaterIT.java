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
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.twitter.common.inject.Bindings;
import com.twitter.common.inject.Bindings.KeyFactory;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.Clock;
import com.twitter.common.util.TruncatedBinaryBackoff;

import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.Driver;
import org.apache.aurora.scheduler.TaskIdGenerator;
import org.apache.aurora.scheduler.TaskIdGenerator.TaskIdGeneratorImpl;
import org.apache.aurora.scheduler.async.RescheduleCalculator;
import org.apache.aurora.scheduler.async.RescheduleCalculator.RescheduleCalculatorImpl;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.state.LockManager;
import org.apache.aurora.scheduler.state.LockManagerImpl;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.state.StateManagerImpl;
import org.apache.aurora.scheduler.state.UUIDGenerator;
import org.apache.aurora.scheduler.state.UUIDGenerator.UUIDGeneratorImpl;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.Storage.Work;
import org.apache.aurora.scheduler.storage.db.DbModule;
import org.apache.aurora.scheduler.storage.entities.IInstanceTaskConfig;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.ILockKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.mem.MemStorage.Delegated;
import org.apache.aurora.scheduler.storage.mem.MemStorageModule;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.JobUpdateStatus.ABORTED;
import static org.apache.aurora.gen.JobUpdateStatus.ERROR;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLED_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLED_FORWARD;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_FORWARD;
import static org.apache.aurora.gen.JobUpdateStatus.ROLL_BACK_PAUSED;
import static org.apache.aurora.gen.JobUpdateStatus.ROLL_FORWARD_PAUSED;
import static org.apache.aurora.gen.ScheduleStatus.ASSIGNED;
import static org.apache.aurora.gen.ScheduleStatus.FAILED;
import static org.apache.aurora.gen.ScheduleStatus.KILLED;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.gen.ScheduleStatus.STARTING;
import static org.apache.aurora.scheduler.events.PubsubEvent.SchedulerActive;
import static org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import static org.apache.aurora.scheduler.updater.JobUpdateControllerImpl.queryByUpdateId;
import static org.apache.aurora.scheduler.updater.UpdateFactory.UpdateFactoryImpl.expandInstanceIds;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JobUpdaterIT extends EasyMockTest {

  private static final String UPDATE_ID = "update_id";
  private static final String USER = "user";
  private static final IJobKey JOB = JobKeys.from("role", "env", "job1");
  private static final Amount<Long, Time> RUNNING_TIMEOUT = Amount.of(1000L, Time.MILLISECONDS);
  private static final Amount<Long, Time> WATCH_TIMEOUT = Amount.of(2000L, Time.MILLISECONDS);
  private static final Amount<Long, Time> FLAPPING_THRESHOLD = Amount.of(1L, Time.MILLISECONDS);
  private static final Amount<Long, Time> ONE_DAY = Amount.of(1L, Time.DAYS);
  private static final ITaskConfig OLD_CONFIG =
      ITaskConfig.build(makeTaskConfig().setExecutorConfig(new ExecutorConfig().setName("new")));
  private static final ITaskConfig NEW_CONFIG = ITaskConfig.build(makeTaskConfig());

  private FakeScheduledExecutor clock;
  private JobUpdateController updater;
  private Driver driver;
  private EventBus eventBus;
  private Storage storage;
  private LockManager lockManager;
  private StateManager stateManager;
  private JobUpdateEventSubscriber subscriber;

  @Before
  public void setUp() {
    // Avoid console spam due to stats registered multiple times.
    Stats.flush();
    final ScheduledExecutorService executor = createMock(ScheduledExecutorService.class);
    clock = new FakeScheduledExecutor(executor);
    driver = createMock(Driver.class);
    eventBus = new EventBus();

    Injector injector = Guice.createInjector(
        new UpdaterModule(executor),
        DbModule.testModule(Bindings.annotatedKeyFactory(Delegated.class)),
        new MemStorageModule(KeyFactory.PLAIN),
        new AbstractModule() {
          @Override
          protected void configure() {
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
                    Amount.of(1, Time.MINUTES)));
            bind(EventSink.class).toInstance(new EventSink() {
              @Override
              public void post(PubsubEvent event) {
                eventBus.post(event);
              }
            });
            bind(LockManager.class).to(LockManagerImpl.class);
            bind(UUIDGenerator.class).to(UUIDGeneratorImpl.class);
          }
        });
    updater = injector.getInstance(JobUpdateController.class);
    storage = injector.getInstance(Storage.class);
    storage.prepare();
    lockManager = injector.getInstance(LockManager.class);
    stateManager = injector.getInstance(StateManager.class);
    eventBus.register(injector.getInstance(JobUpdateEventSubscriber.class));
    subscriber = injector.getInstance(JobUpdateEventSubscriber.class);
  }

  @After
  public void validateExitState() {
    clock.assertEmpty();
    assertEquals(ImmutableList.<ILock>of(), ImmutableList.copyOf(lockManager.getLocks()));
  }

  @Test(expected = UpdateStateException.class)
  public void testJobLocked() throws Exception {
    control.replay();

    ILock lock = lockManager.acquireLock(ILockKey.build(LockKey.job(JOB.newBuilder())), USER);
    try {
      updater.start(makeJobUpdate(makeInstanceConfig(0, 0, NEW_CONFIG)), USER);
    } finally {
      lockManager.releaseLock(lock);
    }
  }

  private String getTaskId(IJobKey job, int instanceId) {
    return Tasks.id(Iterables.getOnlyElement(
        Storage.Util.consistentFetchTasks(
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

      assertTrue(stateManager.changeState(
          getTaskId(job, instanceId),
          Optional.<ScheduleStatus>absent(),
          s,
          Optional.<String>absent()));
    }
  }

  private void assertUpdateStatus(JobUpdateStatus expected) {
    JobUpdateStatus status = storage.consistentRead(new Work.Quiet<JobUpdateStatus>() {
      @Override
      public JobUpdateStatus apply(StoreProvider storeProvider) {
        IJobUpdateSummary summary = Iterables.getOnlyElement(
            storeProvider.getJobUpdateStore().fetchJobUpdateSummaries(queryByUpdateId(UPDATE_ID)));
        return summary.getState().getStatus();
      }
    });
    assertEquals(expected, status);
  }

  private IExpectationSetters<String> expectTaskKilled() {
    driver.killTask(EasyMock.<String>anyObject());
    return expectLastCall();
  }

  private void insertInitialTasks(IJobUpdate update) {
    for (IInstanceTaskConfig config : update.getInstructions().getInitialState()) {
      stateManager.insertPendingTasks(config.getTask(), expandInstanceIds(ImmutableSet.of(config)));
    }
  }

  private void assertJobState(IJobKey job, Map<Integer, ITaskConfig> expected) {
    Iterable<IScheduledTask> tasks =
        Storage.Util.consistentFetchTasks(storage, Query.jobScoped(job).active());

    Map<Integer, IScheduledTask> tasksByInstance =
        Maps.uniqueIndex(tasks, Tasks.SCHEDULED_TO_INSTANCE_ID);
    assertEquals(
        expected,
        ImmutableMap.copyOf(Maps.transformValues(tasksByInstance, Tasks.SCHEDULED_TO_INFO)));
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

    // Instance 1 is added
    updater.start(update, USER);
    assertUpdateStatus(ROLLING_FORWARD);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);

    // Updates may be paused for arbitrarily-long amounts of time, and the updater should not
    // take action while paused.
    updater.pause(JOB);
    updater.pause(JOB);  // Pausing again is a no-op.
    assertUpdateStatus(ROLL_FORWARD_PAUSED);
    clock.advance(ONE_DAY);
    changeState(JOB, 1, FAILED, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 2, FAILED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);
    updater.resume(JOB);
    assertUpdateStatus(ROLLING_FORWARD);

    // A task outside the scope of the update should be ignored by the updater.
    stateManager.insertPendingTasks(NEW_CONFIG, ImmutableSet.of(100));

    // Instance 2 is updated
    changeState(JOB, 2, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    assertUpdateStatus(ROLLED_FORWARD);
    assertJobState(
        JOB,
        ImmutableMap.of(0, NEW_CONFIG, 1, NEW_CONFIG, 2, NEW_CONFIG, 100, NEW_CONFIG));
  }

  @Test
  public void testUpdateSpecificInstances() throws Exception {
    expectTaskKilled();

    control.replay();

    JobUpdate builder =
        setInstanceCount(makeJobUpdate(makeInstanceConfig(0, 1, OLD_CONFIG)), 1).newBuilder();
    builder.getInstructions().getSettings().setUpdateOnlyTheseInstances(
        ImmutableSet.of(new Range(0, 0)));
    IJobUpdate update = IJobUpdate.build(builder);
    insertInitialTasks(update);

    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    // Instance 0 is updated
    updater.start(update, USER);
    changeState(JOB, 0, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    assertUpdateStatus(ROLLED_FORWARD);
    assertJobState(
        JOB,
        ImmutableMap.of(0, NEW_CONFIG, 1, OLD_CONFIG));
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

    // Instance 0 is updated.
    updater.start(update, USER);
    assertUpdateStatus(ROLLING_FORWARD);
    changeState(JOB, 0, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    // Instance 1 is added.
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    // Instance 2 is updated, but fails.
    changeState(JOB, 2, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(FLAPPING_THRESHOLD);
    changeState(JOB, 2, FAILED);

    // Instance 2 is rolled back.
    assertUpdateStatus(ROLLING_BACK);
    changeState(JOB, 2, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    // A rollback may be paused.
    updater.pause(JOB);
    assertUpdateStatus(ROLL_BACK_PAUSED);
    clock.advance(ONE_DAY);
    updater.resume(JOB);
    assertUpdateStatus(ROLLING_BACK);

    // Instance 1 is removed.
    changeState(JOB, 1, KILLED);
    clock.advance(WATCH_TIMEOUT);

    // Instance 0 is rolled back.
    changeState(JOB, 0, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    assertUpdateStatus(ROLLED_BACK);
    assertJobState(JOB, ImmutableMap.of(0, OLD_CONFIG, 2, OLD_CONFIG, 3, OLD_CONFIG));
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
    updater.start(update, USER);
    changeState(JOB, 0, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    updater.abort(JOB);
    assertUpdateStatus(ABORTED);
    clock.advance(WATCH_TIMEOUT);
    assertJobState(JOB, ImmutableMap.of(0, NEW_CONFIG, 1, NEW_CONFIG, 2, OLD_CONFIG));
  }

  @Test
  public void testRollbackFailed() throws Exception {
    expectTaskKilled().times(2);

    control.replay();

    IJobUpdate update = makeJobUpdate(
        makeInstanceConfig(0, 1, OLD_CONFIG));
    insertInitialTasks(update);

    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    // Instance 0 is updated.
    updater.start(update, USER);
    assertUpdateStatus(ROLLING_FORWARD);
    changeState(JOB, 0, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    // Instance 1 is updated, but fails.
    changeState(JOB, 1, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(FLAPPING_THRESHOLD);
    changeState(JOB, 1, FAILED);

    // Instance 1 is rolled back, but fails.
    assertUpdateStatus(ROLLING_BACK);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    clock.advance(FLAPPING_THRESHOLD);
    changeState(JOB, 1, FAILED);

    assertUpdateStatus(JobUpdateStatus.FAILED);
    clock.advance(WATCH_TIMEOUT);
    assertJobState(JOB, ImmutableMap.of(0, NEW_CONFIG, 1, OLD_CONFIG));
  }

  @Test
  public void testLostLock() throws Exception {
    expectTaskKilled();

    control.replay();

    IJobUpdate update = makeJobUpdate(
        makeInstanceConfig(0, 1, OLD_CONFIG));
    insertInitialTasks(update);

    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    // Instance 0 is updated.
    updater.start(update, USER);
    for (ILock lock : lockManager.getLocks()) {
      lockManager.releaseLock(lock);
    }
    clock.advance(RUNNING_TIMEOUT);
    assertUpdateStatus(ERROR);
  }

  private void expectInvalid(JobUpdate update)
      throws UpdateStateException, UpdateConfigurationException {

    try {
      updater.start(IJobUpdate.build(update), USER);
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
    update.getInstructions().getSettings().setMaxWaitToInstanceRunningMs(0);
    expectInvalid(update);

    update = makeJobUpdate().newBuilder();
    update.getInstructions().getSettings().setMinWaitInInstanceRunningMs(0);
    expectInvalid(update);

    update = makeJobUpdate().newBuilder();
    update.getInstructions().setDesiredState(
        new InstanceTaskConfig().setInstances(ImmutableSet.<Range>of()));
    expectInvalid(update);

    update = makeJobUpdate().newBuilder();
    update.getInstructions().getSettings().addToUpdateOnlyTheseInstances(new Range(0, 100));
    try {
      updater.start(IJobUpdate.build(update), USER);
      fail();
    } catch (UpdateConfigurationException e) {
      // Expected.
    }
  }

  @Test
  public void testConfigurationPolicyChange() throws Exception {
    // Simulates a change in input validation after a job update has been persisted.

    expectTaskKilled().times(2);

    control.replay();

    final IJobUpdate update =
        setInstanceCount(makeJobUpdate(makeInstanceConfig(0, 1, OLD_CONFIG)), 2);
    insertInitialTasks(update);

    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    // Instance 0 is updated
    updater.start(update, USER);
    assertUpdateStatus(ROLLING_FORWARD);

    storage.write(new NoResult.Quiet() {
      @Override
      protected void execute(Storage.MutableStoreProvider storeProvider) {
        JobUpdateStore.Mutable store = storeProvider.getJobUpdateStore();
        store.deleteAllUpdatesAndEvents();

        JobUpdate builder = update.newBuilder();
        builder.getInstructions().getSettings().setUpdateGroupSize(0);
        for (ILock lock : lockManager.getLocks()) {
          lockManager.releaseLock(lock);
        }
        saveJobUpdate(store, IJobUpdate.build(builder), ROLLING_FORWARD);
      }
    });

    changeState(JOB, 0, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    // Instance 1 is updated, but fails.
    changeState(JOB, 1, KILLED, ASSIGNED, STARTING, RUNNING, FAILED);
    clock.advance(WATCH_TIMEOUT);

    assertUpdateStatus(ERROR);
  }

  private ILock saveJobUpdate(
      JobUpdateStore.Mutable store,
      IJobUpdate update,
      JobUpdateStatus status) {

    ILock lock;
    try {
      lock = lockManager.acquireLock(
          ILockKey.build(LockKey.job(update.getSummary().getJobKey().newBuilder())), USER);
    } catch (LockManager.LockException e) {
      throw Throwables.propagate(e);
    }

    store.saveJobUpdate(update, lock.getToken());
    store.saveJobUpdateEvent(
        IJobUpdateEvent.build(
            new JobUpdateEvent()
                .setStatus(status)
                .setTimestampMs(clock.nowMillis())),
        update.getSummary().getUpdateId());
    return lock;
  }

  @Test
  public void testRecoverFromStorage() throws Exception {
    expectTaskKilled().times(2);

    control.replay();

    final IJobUpdate update =
        setInstanceCount(makeJobUpdate(makeInstanceConfig(0, 1, OLD_CONFIG)), 2);
    insertInitialTasks(update);
    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    clock.advance(ONE_DAY);

    storage.write(new NoResult.Quiet() {
      @Override
      protected void execute(Storage.MutableStoreProvider storeProvider) {
        saveJobUpdate(storeProvider.getJobUpdateStore(), update, ROLLING_FORWARD);
      }
    });

    eventBus.post(new SchedulerActive());

    // Instance 0 is updated.
    changeState(JOB, 0, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    // Instance 1 is updated.
    changeState(JOB, 1, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    assertUpdateStatus(ROLLED_FORWARD);
  }

  @Test
  public void testSystemResumeNoLock() throws Exception {
    control.replay();

    final IJobUpdate update =
        setInstanceCount(makeJobUpdate(makeInstanceConfig(0, 1, OLD_CONFIG)), 0);

    storage.write(new NoResult.Quiet() {
      @Override
      protected void execute(Storage.MutableStoreProvider storeProvider) {
        ILock lock = saveJobUpdate(storeProvider.getJobUpdateStore(), update, ROLLING_FORWARD);
        lockManager.releaseLock(lock);
      }
    });

    eventBus.post(new SchedulerActive());
    assertUpdateStatus(ERROR);
  }

  @Test
  public void testNoopUpdate() throws Exception {
    control.replay();

    final IJobUpdate update = makeJobUpdate(makeInstanceConfig(0, 2, NEW_CONFIG));
    insertInitialTasks(update);
    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 2, ASSIGNED, STARTING, RUNNING);
    clock.advance(ONE_DAY);
    updater.start(update, USER);
    assertUpdateStatus(ROLLED_FORWARD);
  }

  @Test
  public void testImmediatelyFailedUpdate() throws Exception {
    // An update where the new configuration is running, but entirely stuck in pending.
    control.replay();

    final IJobUpdate update = makeJobUpdate(makeInstanceConfig(0, 2, NEW_CONFIG));
    insertInitialTasks(update);
    clock.advance(ONE_DAY);
    updater.start(update, USER);
    assertUpdateStatus(JobUpdateStatus.FAILED);
  }

  @Test
  public void testStuckTask() throws Exception {
    expectTaskKilled().times(3);

    control.replay();

    IJobUpdate update = setInstanceCount(makeJobUpdate(makeInstanceConfig(0, 1, OLD_CONFIG)), 2);
    insertInitialTasks(update);
    changeState(JOB, 0, ASSIGNED, STARTING, RUNNING);
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);

    // Instance 0 is updated.
    updater.start(update, USER);
    assertUpdateStatus(ROLLING_FORWARD);
    changeState(JOB, 0, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    // Instance 1 is stuck in PENDING.
    changeState(JOB, 1, KILLED);
    clock.advance(RUNNING_TIMEOUT);
    assertUpdateStatus(ROLLING_BACK);

    // Instance 1 is reverted.
    changeState(JOB, 1, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    // Instance 0 is reverted.
    changeState(JOB, 0, KILLED, ASSIGNED, STARTING, RUNNING);
    clock.advance(WATCH_TIMEOUT);

    assertUpdateStatus(ROLLED_BACK);
    assertJobState(JOB, ImmutableMap.of(0, OLD_CONFIG, 1, OLD_CONFIG));
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

    updater.pause(JOB);
  }

  private static IJobUpdateSummary makeUpdateSummary() {
    return IJobUpdateSummary.build(new JobUpdateSummary()
        .setUser("user")
        .setJobKey(JOB.newBuilder())
        .setUpdateId(UPDATE_ID));
  }

  private static IJobUpdate makeJobUpdate(IInstanceTaskConfig... configs) {
    JobUpdate builder = new JobUpdate()
        .setSummary(makeUpdateSummary().newBuilder())
        .setInstructions(new JobUpdateInstructions()
            .setDesiredState(new InstanceTaskConfig()
                .setTask(NEW_CONFIG.newBuilder())
                .setInstances(ImmutableSet.of(new Range(0, 2))))
            .setSettings(new JobUpdateSettings()
                .setUpdateGroupSize(1)
                .setMaxWaitToInstanceRunningMs(RUNNING_TIMEOUT.as(Time.MILLISECONDS).intValue())
                .setMinWaitInInstanceRunningMs(WATCH_TIMEOUT.as(Time.MILLISECONDS).intValue())
                .setUpdateOnlyTheseInstances(ImmutableSet.<Range>of())));

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

  private static TaskConfig makeTaskConfig() {
    return new TaskConfig()
        .setJobName(JOB.getName())
        .setEnvironment(JOB.getEnvironment())
        .setOwner(new Identity(JOB.getRole(), "user"))
        .setIsService(true)
        .setExecutorConfig(new ExecutorConfig().setName("old"))
        .setNumCpus(1);
  }
}

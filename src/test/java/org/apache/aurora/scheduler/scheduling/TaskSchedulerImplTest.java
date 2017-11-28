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
package org.apache.aurora.scheduler.scheduling;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;

import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.scheduler.async.AsyncModule.AsyncExecutor;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.executor.ExecutorSettings;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.preemptor.BiCache;
import org.apache.aurora.scheduler.preemptor.Preemptor;
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.resources.ResourceManager;
import org.apache.aurora.scheduler.state.PubsubTestUtil;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.mem.MemStorageModule;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.gen.ScheduleStatus.THROTTLED;
import static org.apache.aurora.scheduler.filter.AttributeAggregate.empty;
import static org.apache.aurora.scheduler.mesos.TestExecutorSettings.THERMOS_EXECUTOR;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class TaskSchedulerImplTest extends EasyMockTest {
  private static final String TASK_ID = "a";
  private static final IScheduledTask TASK_A =
      TaskTestUtil.makeTask(TASK_ID, JobKeys.from("a", "a", "a"));
  private static final TaskGroupKey GROUP_KEY =
      TaskGroupKey.from(TASK_A.getAssignedTask().getTask());
  private static final String SLAVE_ID = "HOST_A";
  private static final Map<String, TaskGroupKey> NO_RESERVATION = ImmutableMap.of();
  private static final ImmutableSet<String> SINGLE_TASK = ImmutableSet.of(TASK_ID);
  private static final Set<String> SCHEDULED_RESULT = ImmutableSet.of(TASK_ID);
  private static final Set<String> NOT_SCHEDULED_RESULT = ImmutableSet.of();

  private StorageTestUtil storageUtil;
  private TaskAssigner assigner;
  private TaskScheduler scheduler;
  private Preemptor preemptor;
  private BiCache<String, TaskGroupKey> reservations;
  private EventSink eventSink;

  @Before
  public void setUp() throws Exception {
    storageUtil = new StorageTestUtil(this);
    assigner = createMock(TaskAssigner.class);
    preemptor = createMock(Preemptor.class);
    reservations = createMock(new Clazz<BiCache<String, TaskGroupKey>>() { });

    Injector injector = getInjector(storageUtil.storage);
    scheduler = injector.getInstance(TaskScheduler.class);
    eventSink = PubsubTestUtil.startPubsub(injector);
  }

  private Injector getInjector(Storage storageImpl) {
    return Guice.createInjector(
        new PubsubEventModule(),
        new AbstractModule() {
          @Override
          protected void configure() {

            bind(Executor.class).annotatedWith(AsyncExecutor.class)
                .toInstance(MoreExecutors.directExecutor());
            bind(new TypeLiteral<BiCache<String, TaskGroupKey>>() { }).toInstance(reservations);
            bind(TaskScheduler.class).to(TaskSchedulerImpl.class);
            bind(Preemptor.class).toInstance(preemptor);
            bind(TaskAssigner.class).toInstance(assigner);
            bind(Clock.class).toInstance(createMock(Clock.class));
            bind(StatsProvider.class).toInstance(new FakeStatsProvider());
            bind(Storage.class).toInstance(storageImpl);
            bind(ExecutorSettings.class).toInstance(THERMOS_EXECUTOR);
            PubsubEventModule.bindSubscriber(binder(), TaskScheduler.class);
          }
        });
  }

  private void expectTaskStillPendingQuery(IScheduledTask task) {
    storageUtil.expectTaskFetch(
        Query.taskScoped(Tasks.id(task)).byStatus(PENDING),
        ImmutableSet.of(task));
  }

  private ResourceBag bag(IScheduledTask task) {
    return ResourceManager.bagFromResources(task.getAssignedTask().getTask().getResources())
        .add(THERMOS_EXECUTOR.getExecutorOverhead(task.getAssignedTask()
            .getTask()
            .getExecutorConfig()
            .getName()).get());
  }

  private IExpectationSetters<Set<String>> expectAssigned(
      IScheduledTask task,
      Map<String, TaskGroupKey> reservationMap) {

    return expect(assigner.maybeAssign(
        storageUtil.mutableStoreProvider,
        new ResourceRequest(task.getAssignedTask().getTask(), bag(task), empty()),
        TaskGroupKey.from(task.getAssignedTask().getTask()),
        ImmutableSet.of(task.getAssignedTask()),
        reservationMap));
  }

  @Test
  public void testSchedule() throws Exception {
    storageUtil.expectOperations();

    expectAsMap(NO_RESERVATION);
    expectTaskStillPendingQuery(TASK_A);
    expectActiveJobFetch(TASK_A);
    expectAssigned(TASK_A, NO_RESERVATION).andReturn(SCHEDULED_RESULT);

    control.replay();

    assertEquals(
        SCHEDULED_RESULT,
        scheduler.schedule(storageUtil.mutableStoreProvider, SINGLE_TASK));
  }

  @Test
  public void testScheduleNoTask() throws Exception {
    storageUtil.expectOperations();
    storageUtil.expectTaskFetch(
        Query.taskScoped(Tasks.id(TASK_A)).byStatus(PENDING),
        ImmutableSet.of());

    control.replay();

    assertEquals(
        SCHEDULED_RESULT,
        scheduler.schedule(storageUtil.mutableStoreProvider, SINGLE_TASK));
  }

  @Test
  public void testSchedulePartial() throws Exception {
    storageUtil.expectOperations();

    String taskB = "b";
    expectAsMap(NO_RESERVATION);
    storageUtil.expectTaskFetch(
        Query.taskScoped(Tasks.id(TASK_A), taskB).byStatus(PENDING),
        ImmutableSet.of(TASK_A));
    expectActiveJobFetch(TASK_A);
    expectAssigned(TASK_A, NO_RESERVATION).andReturn(SCHEDULED_RESULT);

    control.replay();

    // Task b should be returned as well to be purged from its TaskGroup.
    assertEquals(
        ImmutableSet.of(TASK_ID, taskB),
        scheduler.schedule(storageUtil.mutableStoreProvider, ImmutableSet.of(TASK_ID, taskB)));
  }

  @Test
  public void testMultipleGroupsRejected() {
    storageUtil.expectOperations();

    String taskB = "b";
    storageUtil.expectTaskFetch(
        Query.taskScoped(Tasks.id(TASK_A), taskB).byStatus(PENDING),
        ImmutableSet.of(TASK_A, TaskTestUtil.makeTask(taskB, JobKeys.from("b", "b", "b"))));

    control.replay();

    assertEquals(
        NOT_SCHEDULED_RESULT,
        scheduler.schedule(storageUtil.mutableStoreProvider, ImmutableSet.of(TASK_ID, taskB)));
  }

  @Test
  public void testReservation() throws Exception {
    storageUtil.expectOperations();

    // No reservation available in preemptor
    expectTaskStillPendingQuery(TASK_A);
    expectActiveJobFetch(TASK_A);
    expectAssigned(TASK_A, NO_RESERVATION).andReturn(NOT_SCHEDULED_RESULT);
    expectAsMap(NO_RESERVATION);
    expectNoReservation(TASK_A);
    expectPreemptorCall(TASK_A, Optional.absent());

    // Slave is reserved.
    expectTaskStillPendingQuery(TASK_A);
    expectActiveJobFetch(TASK_A);
    expectAssigned(TASK_A, NO_RESERVATION).andReturn(NOT_SCHEDULED_RESULT);
    expectAsMap(NO_RESERVATION);
    expectNoReservation(TASK_A);
    expectPreemptorCall(TASK_A, Optional.of(SLAVE_ID));
    expectAddReservation(TASK_A, SLAVE_ID);

    // Use previously created reservation.
    expectTaskStillPendingQuery(TASK_A);
    expectActiveJobFetch(TASK_A);
    expectAsMap(ImmutableMap.of(SLAVE_ID, GROUP_KEY));
    expectAssigned(TASK_A, ImmutableMap.of(SLAVE_ID, GROUP_KEY)).andReturn(SCHEDULED_RESULT);

    control.replay();

    assertEquals(
        NOT_SCHEDULED_RESULT,
        scheduler.schedule(storageUtil.mutableStoreProvider, SINGLE_TASK));
    assertEquals(
        NOT_SCHEDULED_RESULT,
        scheduler.schedule(storageUtil.mutableStoreProvider, SINGLE_TASK));
    assertEquals(
        SCHEDULED_RESULT,
        scheduler.schedule(storageUtil.mutableStoreProvider, SINGLE_TASK));
  }

  @Test
  public void testReservationUnusable() throws Exception {
    storageUtil.expectOperations();

    expectTaskStillPendingQuery(TASK_A);
    expectActiveJobFetch(TASK_A);
    expectAsMap(NO_RESERVATION);
    expectAssigned(TASK_A, NO_RESERVATION).andReturn(NOT_SCHEDULED_RESULT);
    expectGetReservation(TASK_A, SLAVE_ID);

    control.replay();

    assertEquals(
        NOT_SCHEDULED_RESULT,
        scheduler.schedule(storageUtil.mutableStoreProvider, SINGLE_TASK));
  }

  @Test
  public void testReservationRemoved() throws Exception {
    storageUtil.expectOperations();

    expectTaskStillPendingQuery(TASK_A);
    expectActiveJobFetch(TASK_A);
    expectAsMap(NO_RESERVATION);
    expectAssigned(TASK_A, NO_RESERVATION).andReturn(NOT_SCHEDULED_RESULT);
    expectGetReservation(TASK_A, SLAVE_ID);

    control.replay();

    assertEquals(
        NOT_SCHEDULED_RESULT,
        scheduler.schedule(storageUtil.mutableStoreProvider, SINGLE_TASK));
  }

  @Test
  public void testNonPendingIgnored() throws Exception {
    control.replay();

    eventSink.post(TaskStateChange.transition(TASK_A, RUNNING));
  }

  @Test
  public void testPendingDeletedHandled() throws Exception {
    reservations.remove(SLAVE_ID, TaskGroupKey.from(TASK_A.getAssignedTask().getTask()));

    control.replay();

    ScheduledTask taskBuilder = TASK_A.newBuilder().setStatus(PENDING);
    taskBuilder.getAssignedTask().setSlaveId(SLAVE_ID);
    eventSink.post(TaskStateChange.transition(IScheduledTask.build(taskBuilder), PENDING));
  }

  @Test
  public void testIgnoresThrottledTasks() throws Exception {
    // Ensures that tasks in THROTTLED state are not considered part of the active job state.
    Storage memStorage = MemStorageModule.newEmptyStorage();

    Injector injector = getInjector(memStorage);
    scheduler = injector.getInstance(TaskScheduler.class);
    eventSink = PubsubTestUtil.startPubsub(injector);

    ScheduledTask builder = TASK_A.newBuilder();
    IScheduledTask taskA = IScheduledTask.build(builder.setStatus(PENDING));
    builder.getAssignedTask().setTaskId("b");
    IScheduledTask taskB = IScheduledTask.build(builder.setStatus(THROTTLED));

    memStorage.write((NoResult.Quiet)
        store -> store.getUnsafeTaskStore().saveTasks(ImmutableSet.of(taskA, taskB)));

    expectAsMap(NO_RESERVATION);
    expect(assigner.maybeAssign(
        EasyMock.anyObject(),
        eq(new ResourceRequest(taskA.getAssignedTask().getTask(), bag(taskA), empty())),
        eq(TaskGroupKey.from(taskA.getAssignedTask().getTask())),
        eq(ImmutableSet.of(taskA.getAssignedTask())),
        eq(NO_RESERVATION))).andReturn(SCHEDULED_RESULT);

    control.replay();

    memStorage.write((NoResult.Quiet)
        store -> assertEquals(SCHEDULED_RESULT, scheduler.schedule(store, SINGLE_TASK)));
  }

  @Test
  public void testScheduleThrows() throws Exception {
    storageUtil.expectOperations();

    expectAsMap(NO_RESERVATION);
    expectTaskStillPendingQuery(TASK_A);
    expectActiveJobFetch(TASK_A);
    expectAssigned(TASK_A, NO_RESERVATION).andThrow(new IllegalArgumentException("expected"));

    control.replay();

    assertEquals(
        NOT_SCHEDULED_RESULT,
        scheduler.schedule(storageUtil.mutableStoreProvider, SINGLE_TASK));
  }

  private void expectPreemptorCall(IScheduledTask task, Optional<String> result) {
    expect(preemptor.attemptPreemptionFor(
        task.getAssignedTask(),
        empty(),
        storageUtil.mutableStoreProvider)).andReturn(result);
  }

  private void expectActiveJobFetch(IScheduledTask task) {
    storageUtil.expectTaskFetch(
        Query.jobScoped(((Function<IScheduledTask, IJobKey>) Tasks::getJob).apply(task))
            .byStatus(Tasks.SLAVE_ASSIGNED_STATES),
        ImmutableSet.of());
  }

  private void expectAddReservation(IScheduledTask task, String slaveId) {
    reservations.put(slaveId, TaskGroupKey.from(task.getAssignedTask().getTask()));
  }

  private IExpectationSetters<?> expectGetReservation(IScheduledTask task, String slaveId) {
    return expect(reservations.getByValue(TaskGroupKey.from(task.getAssignedTask().getTask())))
        .andReturn(ImmutableSet.of(slaveId));
  }

  private IExpectationSetters<?> expectNoReservation(IScheduledTask task) {
    return expect(reservations.getByValue(TaskGroupKey.from(task.getAssignedTask().getTask())))
        .andReturn(ImmutableSet.of());
  }

  private IExpectationSetters<?> expectAsMap(Map<String, TaskGroupKey> map) {
    return expect(reservations.asMap()).andReturn(map);
  }
}

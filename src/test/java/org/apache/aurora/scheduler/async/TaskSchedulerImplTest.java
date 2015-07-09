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
package org.apache.aurora.scheduler.async;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import com.twitter.common.stats.StatsProvider;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.Clock;

import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.async.TaskScheduler.TaskSchedulerImpl;
import org.apache.aurora.scheduler.async.preemptor.BiCache;
import org.apache.aurora.scheduler.async.preemptor.Preemptor;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.state.PubsubTestUtil;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.state.TaskAssigner;
import org.apache.aurora.scheduler.state.TaskAssigner.Assignment;
import org.apache.aurora.scheduler.state.TaskAssigner.Assignment.Result;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.db.DbUtil;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.mesos.Protos.TaskInfo;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.gen.ScheduleStatus.THROTTLED;
import static org.apache.aurora.scheduler.filter.AttributeAggregate.EMPTY;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TaskSchedulerImplTest extends EasyMockTest {

  private static final IScheduledTask TASK_A =
      TaskTestUtil.makeTask("a", JobKeys.from("a", "a", "a"));
  private static final IScheduledTask TASK_B =
      TaskTestUtil.makeTask("b", JobKeys.from("b", "b", "b"));
  private static final HostOffer OFFER = new HostOffer(
      Offers.makeOffer("OFFER_A", "HOST_A"),
      IHostAttributes.build(new HostAttributes().setMode(MaintenanceMode.NONE)));

  private static final String SLAVE_ID = OFFER.getOffer().getSlaveId().getValue();

  private static final TaskGroupKey GROUP_A = TaskGroupKey.from(TASK_A.getAssignedTask().getTask());
  private static final TaskGroupKey GROUP_B = TaskGroupKey.from(TASK_B.getAssignedTask().getTask());

  private StorageTestUtil storageUtil;
  private StateManager stateManager;
  private TaskAssigner assigner;
  private OfferManager offerManager;
  private TaskScheduler scheduler;
  private Preemptor preemptor;
  private BiCache<String, TaskGroupKey> reservations;
  private EventSink eventSink;

  @Before
  public void setUp() throws Exception {
    storageUtil = new StorageTestUtil(this);
    stateManager = createMock(StateManager.class);
    assigner = createMock(TaskAssigner.class);
    offerManager = createMock(OfferManager.class);
    preemptor = createMock(Preemptor.class);
    reservations = createMock(new Clazz<BiCache<String, TaskGroupKey>>() { });

    Injector injector = getInjector(storageUtil.storage);
    scheduler = injector.getInstance(TaskScheduler.class);
    eventSink = PubsubTestUtil.startPubsub(injector);
  }

  private Injector getInjector(final Storage storageImpl) {
    return Guice.createInjector(
        new PubsubEventModule(false),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(new TypeLiteral<BiCache<String, TaskGroupKey>>() { }).toInstance(reservations);
            bind(TaskScheduler.class).to(TaskSchedulerImpl.class);
            bind(Preemptor.class).toInstance(preemptor);
            bind(OfferManager.class).toInstance(offerManager);
            bind(StateManager.class).toInstance(stateManager);
            bind(TaskAssigner.class).toInstance(assigner);
            bind(Clock.class).toInstance(createMock(Clock.class));
            bind(StatsProvider.class).toInstance(new FakeStatsProvider());
            bind(Storage.class).toInstance(storageImpl);
            PubsubEventModule.bindSubscriber(binder(), TaskScheduler.class);
          }
        });
  }

  private void expectTaskStillPendingQuery(IScheduledTask task) {
    storageUtil.expectTaskFetch(
        Query.taskScoped(Tasks.id(task)).byStatus(PENDING),
        ImmutableSet.of(task));
  }

  private void expectAssigned(IScheduledTask task) {
    expect(assigner.maybeAssign(
        storageUtil.mutableStoreProvider,
        OFFER,
        new ResourceRequest(task.getAssignedTask().getTask(), EMPTY),
        Tasks.id(task))).andReturn(Assignment.success(TaskInfo.getDefaultInstance()));
  }

  @Test
  public void testReservation() throws Exception {
    storageUtil.expectOperations();

    expectTaskStillPendingQuery(TASK_A);
    expectActiveJobFetch(TASK_A);
    expectLaunchAttempt(false);
    // Reserve "a" with offerA
    expectReservationCheck(TASK_A);
    expectPreemptorCall(TASK_A, Optional.of(SLAVE_ID));
    expectAddReservation(SLAVE_ID, TASK_A);

    // Use previously created reservation.
    expectTaskStillPendingQuery(TASK_A);
    expectActiveJobFetch(TASK_A);
    expectGetReservation(SLAVE_ID, TASK_A);
    expectAssigned(TASK_A);
    AssignmentCapture assignment = expectLaunchAttempt(true);

    control.replay();

    assertFalse(scheduler.schedule("a"));
    assertTrue(scheduler.schedule("a"));
    assignAndAssert(Result.SUCCESS, GROUP_A, OFFER, assignment);
  }

  @Test
  public void testReservationExpires() throws Exception {
    storageUtil.expectOperations();

    expectTaskStillPendingQuery(TASK_A);
    expectActiveJobFetch(TASK_A);
    expectLaunchAttempt(false);
    // Reserve "a" with offerA
    expectReservationCheck(TASK_A);
    expectPreemptorCall(TASK_A, Optional.of(SLAVE_ID));
    expectAddReservation(SLAVE_ID, TASK_A);

    // First attempt -> reservation is active.
    expectTaskStillPendingQuery(TASK_B);
    expectActiveJobFetch(TASK_B);
    AssignmentCapture firstAssignment = expectLaunchAttempt(false);
    expectGetReservation(SLAVE_ID, TASK_A);
    expectReservationCheck(TASK_B);
    expectPreemptorCall(TASK_B, Optional.absent());

    // Status changed -> reservation removed.
    reservations.remove(SLAVE_ID, TaskGroupKey.from(TASK_A.getAssignedTask().getTask()));

    // Second attempt -> reservation expires.
    expectGetNoReservation(SLAVE_ID);
    expectTaskStillPendingQuery(TASK_B);
    expectActiveJobFetch(TASK_B);
    AssignmentCapture secondAssignment = expectLaunchAttempt(true);
    expectAssigned(TASK_B);

    control.replay();

    assertFalse(scheduler.schedule("a"));
    assertFalse(scheduler.schedule("b"));
    assignAndAssert(Result.FAILURE, GROUP_B, OFFER, firstAssignment);

    eventSink.post(TaskStateChange.transition(assign(TASK_A, SLAVE_ID), PENDING));
    assertTrue(scheduler.schedule("b"));
    assignAndAssert(Result.SUCCESS, GROUP_B, OFFER, secondAssignment);
  }

  @Test
  public void testReservationUnusable() throws Exception {
    storageUtil.expectOperations();

    expectTaskStillPendingQuery(TASK_A);
    expectLaunchAttempt(false);
    expect(reservations.getByValue(TaskGroupKey.from(TASK_A.getAssignedTask().getTask())))
        .andReturn(ImmutableSet.of(SLAVE_ID));

    control.replay();

    assertFalse(scheduler.schedule("a"));
  }

  @Test
  public void testNonPendingIgnored() throws Exception {
    control.replay();

    eventSink.post(TaskStateChange.transition(TASK_A, RUNNING));
  }

  @Test
  public void testPendingDeletedHandled() throws Exception {
    control.replay();

    IScheduledTask task = IScheduledTask.build(TASK_A.newBuilder().setStatus(PENDING));
    eventSink.post(TaskStateChange.transition(task, PENDING));
  }

  @Test
  public void testIgnoresThrottledTasks() throws Exception {
    // Ensures that tasks in THROTTLED state are not considered part of the active job state passed
    // to the assigner function.

    Storage memStorage = DbUtil.createStorage();

    Injector injector = getInjector(memStorage);
    scheduler = injector.getInstance(TaskScheduler.class);
    eventSink = PubsubTestUtil.startPubsub(injector);

    ScheduledTask builder = TASK_A.newBuilder();
    final IScheduledTask taskA = IScheduledTask.build(builder.setStatus(PENDING));
    builder.getAssignedTask().setTaskId("b");
    final IScheduledTask taskB = IScheduledTask.build(builder.setStatus(THROTTLED));

    memStorage.write(new MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(MutableStoreProvider store) {
        store.getUnsafeTaskStore().saveTasks(ImmutableSet.of(taskA, taskB));
      }
    });

    expectGetNoReservation(SLAVE_ID);
    AssignmentCapture assignment = expectLaunchAttempt(true);
    expect(assigner.maybeAssign(
        EasyMock.anyObject(),
        eq(OFFER),
        eq(new ResourceRequest(taskA.getAssignedTask().getTask(), EMPTY)),
        eq(Tasks.id(taskA)))).andReturn(Assignment.success(TaskInfo.getDefaultInstance()));

    control.replay();

    assertTrue(scheduler.schedule(Tasks.id(taskA)));
    assignAndAssert(Result.SUCCESS, GROUP_A, OFFER, assignment);
  }

  private static class AssignmentCapture {
    public Capture<Function<HostOffer, Assignment>> assigner = createCapture();
    public Capture<TaskGroupKey> groupKey = createCapture();
  }

  private void expectPreemptorCall(IScheduledTask task, Optional<String> result) {
    expect(preemptor.attemptPreemptionFor(
        task.getAssignedTask(),
        EMPTY,
        storageUtil.mutableStoreProvider)).andReturn(result);
  }

  private AssignmentCapture expectLaunchAttempt(boolean taskLaunched)
      throws OfferManager.LaunchException {

    AssignmentCapture capture = new AssignmentCapture();
    expect(offerManager.launchFirst(capture(capture.assigner), capture(capture.groupKey)))
        .andReturn(taskLaunched);
    return capture;
  }

  private IScheduledTask assign(IScheduledTask task, String slaveId) {
    ScheduledTask result = task.newBuilder();
    result.getAssignedTask().setSlaveId(slaveId);
    return IScheduledTask.build(result);
  }

  private void assignAndAssert(
      Result result,
      TaskGroupKey groupKey,
      HostOffer offer,
      AssignmentCapture capture) {

    assertEquals(result, capture.assigner.getValue().apply(offer).getResult());
    assertEquals(groupKey, capture.groupKey.getValue());
  }

  private void expectActiveJobFetch(IScheduledTask task) {
    storageUtil.expectTaskFetch(
        Query.jobScoped(Tasks.SCHEDULED_TO_JOB_KEY.apply(task))
            .byStatus(Tasks.SLAVE_ASSIGNED_STATES),
        ImmutableSet.of());
  }

  private void expectAddReservation(String slaveId, IScheduledTask task) {
    reservations.put(slaveId, TaskGroupKey.from(task.getAssignedTask().getTask()));
  }

  private IExpectationSetters<?> expectGetReservation(String slaveId, IScheduledTask task) {
    return expect(reservations.get(slaveId))
        .andReturn(Optional.of(TaskGroupKey.from(task.getAssignedTask().getTask())));
  }

  private IExpectationSetters<?> expectGetNoReservation(String slaveId) {
    return expect(reservations.get(slaveId)).andReturn(Optional.absent());
  }

  private IExpectationSetters<?> expectReservationCheck(IScheduledTask task) {
    return expect(reservations.getByValue(TaskGroupKey.from(task.getAssignedTask().getTask())))
        .andReturn(ImmutableSet.of());
  }
}

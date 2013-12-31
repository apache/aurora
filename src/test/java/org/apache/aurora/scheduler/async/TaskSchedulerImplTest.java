/*
 * Copyright 2013 Twitter, Inc.
 *
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

import com.twitter.common.base.Closure;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.Clock;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.state.PubsubTestUtil;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.state.TaskAssigner;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;

import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.TaskInfo;
import org.easymock.Capture;

import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.PENDING;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;

import static org.junit.Assert.assertEquals;

public class TaskSchedulerImplTest extends EasyMockTest {

  private StorageTestUtil storageUtil;
  private StateManager stateManager;
  private TaskAssigner assigner;
  private OfferQueue offerQueue;
  private TaskScheduler scheduler;
  private FakeClock clock;
  private Preemptor preemptor;
  private Amount<Long, Time> reservationDuration;
  private Amount<Long, Time> halfReservationDuration;
  private Closure<PubsubEvent> eventSink;

  @Before
  public void setUp() throws Exception {
    storageUtil = new StorageTestUtil(this);
    stateManager = createMock(StateManager.class);
    assigner = createMock(TaskAssigner.class);
    offerQueue = createMock(OfferQueue.class);
    reservationDuration = Amount.of(2L, Time.MINUTES);
    halfReservationDuration = Amount.of(1L, Time.MINUTES);
    clock = new FakeClock();
    clock.setNowMillis(0);
    preemptor = createMock(Preemptor.class);

    Injector injector = Guice.createInjector(new AbstractModule() {
      @Override protected void configure() {
        PubsubTestUtil.installPubsub(binder());
        bind(AsyncModule.PREEMPTOR_KEY).toInstance(preemptor);
        AsyncModule.bindTaskScheduler(binder(), AsyncModule.PREEMPTOR_KEY, reservationDuration);
        bind(OfferQueue.class).toInstance(offerQueue);
        bind(StateManager.class).toInstance(stateManager);
        bind(TaskAssigner.class).toInstance(assigner);
        bind(Clock.class).toInstance(clock);
        bind(Storage.class).toInstance(storageUtil.storage);
      }
    });

    scheduler = injector.getInstance(TaskScheduler.class);
    eventSink = PubsubTestUtil.startPubsub(injector);
  }

  @Test
  public void testReservationsDeniesTasksForTimePeriod() throws OfferQueue.LaunchException {
    IScheduledTask taskA = makeTask("a");
    IScheduledTask taskB = makeTask("b");
    Offer offerA = Offers.makeOffer("OFFER_A", "HOST_A");

    storageUtil.expectOperations();

    storageUtil.expectTaskFetch(Query.taskScoped("a").byStatus(PENDING), ImmutableSet.of(taskA));
    expectLaunchAttempt(false);
    // Reserve "a" with offerA
    expect(preemptor.findPreemptionSlotFor("a"))
        .andReturn(Optional.of(offerA.getSlaveId().getValue()));

    storageUtil.expectTaskFetch(Query.taskScoped("b").byStatus(PENDING), ImmutableSet.of(taskB));
    Capture<Function<Offer, Optional<TaskInfo>>> firstAssignment = expectLaunchAttempt(false);
    expect(preemptor.findPreemptionSlotFor("b")).andReturn(Optional.<String>absent());

    storageUtil.expectTaskFetch(Query.taskScoped("b").byStatus(PENDING), ImmutableSet.of(taskB));
    Capture<Function<Offer, Optional<TaskInfo>>> secondAssignment = expectLaunchAttempt(true);

    expect(assigner.maybeAssign(offerA, taskB))
        .andReturn(Optional.<TaskInfo>of(TaskInfo.getDefaultInstance()));

    control.replay();

    assertEquals(scheduler.schedule("a"), TaskScheduler.TaskSchedulerResult.TRY_AGAIN);
    assertEquals(scheduler.schedule("b"), TaskScheduler.TaskSchedulerResult.TRY_AGAIN);

    assertEquals(Optional.<TaskInfo>absent(), firstAssignment.getValue().apply(offerA));

    clock.advance(reservationDuration);

    assertEquals(scheduler.schedule("b"), TaskScheduler.TaskSchedulerResult.SUCCESS);

    assertEquals(secondAssignment.getValue().apply(offerA).isPresent(), true);
  }

  @Test
  public void testReservationsExpireAfterAccepted() throws OfferQueue.LaunchException {
    IScheduledTask taskA = makeTask("a");
    IScheduledTask taskB = makeTask("b");
    Offer offerA = Offers.makeOffer("OFFER_A", "HOST_A");

    storageUtil.expectOperations();

    storageUtil.expectTaskFetch(Query.taskScoped("a").byStatus(PENDING), ImmutableSet.of(taskA));
    expectLaunchAttempt(false);
    // Reserve "a" with offerA
    expect(preemptor.findPreemptionSlotFor("a"))
        .andReturn(Optional.of(offerA.getSlaveId().getValue()));

    storageUtil.expectTaskFetch(Query.taskScoped("a").byStatus(PENDING), ImmutableSet.of(taskA));

    Capture<Function<Offer, Optional<TaskInfo>>> firstAssignment = expectLaunchAttempt(true);

    expect(assigner.maybeAssign(offerA, taskA))
        .andReturn(Optional.<TaskInfo>of(TaskInfo.getDefaultInstance()));

    storageUtil.expectTaskFetch(Query.taskScoped("b").byStatus(PENDING), ImmutableSet.of(taskB));

    Capture<Function<Offer, Optional<TaskInfo>>> secondAssignment = expectLaunchAttempt(true);

    expect(assigner.maybeAssign(offerA, taskB))
        .andReturn(Optional.<TaskInfo>of(TaskInfo.getDefaultInstance()));

    control.replay();
    assertEquals(scheduler.schedule("a"), TaskScheduler.TaskSchedulerResult.TRY_AGAIN);
    assertEquals(scheduler.schedule("a"), TaskScheduler.TaskSchedulerResult.SUCCESS);
    firstAssignment.getValue().apply(offerA);
    eventSink.execute(new PubsubEvent.TaskStateChange(taskA, PENDING));
    clock.advance(halfReservationDuration);
    assertEquals(scheduler.schedule("b"), TaskScheduler.TaskSchedulerResult.SUCCESS);
    secondAssignment.getValue().apply(offerA);
  }

  @Test
  public void testReservationsAcceptsWithInTimePeriod() throws OfferQueue.LaunchException {
    IScheduledTask taskA = makeTask("a");
    Offer offerA = Offers.makeOffer("OFFER_A", "HOST_A");

    storageUtil.expectOperations();
    storageUtil.expectTaskFetch(Query.taskScoped("a").byStatus(PENDING), ImmutableSet.of(taskA));
    expectLaunchAttempt(false);
    // Reserve "a" with offerA
    expect(preemptor.findPreemptionSlotFor("a"))
        .andReturn(Optional.of(offerA.getSlaveId().getValue()));

    storageUtil.expectTaskFetch(Query.taskScoped("a").byStatus(PENDING), ImmutableSet.of(taskA));

    Capture<Function<Offer, Optional<TaskInfo>>> firstAssignment = expectLaunchAttempt(true);

    expect(assigner.maybeAssign(offerA, taskA))
        .andReturn(Optional.<TaskInfo>of(TaskInfo.getDefaultInstance()));

    control.replay();
    assertEquals(scheduler.schedule("a"), TaskScheduler.TaskSchedulerResult.TRY_AGAIN);
    clock.advance(halfReservationDuration);
    assertEquals(scheduler.schedule("a"), TaskScheduler.TaskSchedulerResult.SUCCESS);

    firstAssignment.getValue().apply(offerA);
  }

  @Test
  public void testReservationsCancellation() throws OfferQueue.LaunchException {
    IScheduledTask taskA = makeTask("a");
    IScheduledTask taskB = makeTask("b");
    Offer offerA = Offers.makeOffer("OFFER_A", "HOST_A");

    storageUtil.expectOperations();

    storageUtil.expectTaskFetch(Query.taskScoped("a").byStatus(PENDING), ImmutableSet.of(taskA));
    expectLaunchAttempt(false);

    // Reserve "a" with offerA
    expect(preemptor.findPreemptionSlotFor("a"))
        .andReturn(Optional.of(offerA.getSlaveId().getValue()));

    storageUtil.expectTaskFetch(Query.taskScoped("b").byStatus(PENDING), ImmutableSet.of(taskB));

    Capture<Function<Offer, Optional<TaskInfo>>> assignment = expectLaunchAttempt(true);

    expect(assigner.maybeAssign(offerA, taskB))
        .andReturn(Optional.<TaskInfo>of(TaskInfo.getDefaultInstance()));

    control.replay();
    assertEquals(scheduler.schedule("a"), TaskScheduler.TaskSchedulerResult.TRY_AGAIN);
    clock.advance(halfReservationDuration);
    // Task is killed by user before it is scheduled
    eventSink.execute(new PubsubEvent.TaskStateChange(taskA, PENDING));
    assertEquals(scheduler.schedule("b"), TaskScheduler.TaskSchedulerResult.SUCCESS);
    assignment.getValue().apply(offerA);
  }

  @Test
  public void testReservationsExpire() throws OfferQueue.LaunchException {
    IScheduledTask taskA = makeTask("a");
    IScheduledTask taskB = makeTask("b");
    Offer offer1 = Offers.makeOffer("OFFER_1", "HOST_A");

    storageUtil.expectOperations();

    storageUtil.expectTaskFetch(Query.taskScoped("b").byStatus(PENDING), ImmutableSet.of(taskB));
    expectLaunchAttempt(false);
    // Reserve "b" with offer1
    expect(preemptor.findPreemptionSlotFor("b"))
        .andReturn(Optional.of(offer1.getSlaveId().getValue()));

    storageUtil.expectTaskFetch(Query.taskScoped("a").byStatus(PENDING), ImmutableSet.of(taskA));
    Capture<Function<Offer, Optional<TaskInfo>>> firstAssignment = expectLaunchAttempt(true);

    expect(assigner.maybeAssign(offer1, taskA))
        .andReturn(Optional.<TaskInfo>of(TaskInfo.getDefaultInstance()));

    control.replay();
    assertEquals(scheduler.schedule("b"), TaskScheduler.TaskSchedulerResult.TRY_AGAIN);
    // We don't act on the reservation made by b because we want to see timeout behaviour.
    clock.advance(reservationDuration);
    assertEquals(scheduler.schedule("a"), TaskScheduler.TaskSchedulerResult.SUCCESS);
    firstAssignment.getValue().apply(offer1);
  }

  private IScheduledTask makeTask(String taskId) {
    return IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setInstanceId(0)
            .setTaskId(taskId)
            .setTask(new TaskConfig()
                .setJobName("job-" + taskId)
                .setOwner(new Identity().setRole("role-" + taskId).setUser("user-" + taskId))
                .setEnvironment("env-" + taskId))));
  }

  private Capture<Function<Offer, Optional<TaskInfo>>> expectLaunchAttempt(boolean taskLaunched)
      throws OfferQueue.LaunchException {
        Capture<Function<Offer, Optional<TaskInfo>>> assignment = createCapture();
        expect(offerQueue.launchFirst(capture(assignment))).andReturn(taskLaunched);
        return assignment;
  }
}

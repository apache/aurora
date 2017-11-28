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
package org.apache.aurora.scheduler.mesos;

import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.Executor;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.InetAddresses;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.aurora.common.application.Lifecycle;
import org.apache.aurora.common.base.Command;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.TaskStatusHandler;
import org.apache.aurora.scheduler.base.Conversions;
import org.apache.aurora.scheduler.base.SchedulerException;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.state.MaintenanceController;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.mesos.v1.Protos;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.aurora.gen.MaintenanceMode.DRAINING;
import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.strictMock;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MesosCallbackHandlerTest extends EasyMockTest {
  private static final Protos.AgentID AGENT_ID =
      Protos.AgentID.newBuilder().setValue("agent-id").build();

  private static final String MASTER_ID = "master-id";
  private static final Protos.MasterInfo MASTER = Protos.MasterInfo.newBuilder()
      .setId(MASTER_ID)
      .setIp(InetAddresses.coerceToInteger(InetAddresses.forString("1.2.3.4"))) //NOPMD
      .setPort(5050).build();

  private static final Protos.ExecutorID EXECUTOR_ID =
      Protos.ExecutorID.newBuilder().setValue("executor-id").build();

  private static final String FRAMEWORK_ID = "framework-id";
  private static final Protos.FrameworkID FRAMEWORK =
      Protos.FrameworkID.newBuilder().setValue(FRAMEWORK_ID).build();

  private static final String AGENT_HOST = "agent-hostname";

  private static final Protos.OfferID OFFER_ID =
      Protos.OfferID.newBuilder().setValue("offer-id").build();
  private static final Protos.Offer OFFER = Protos.Offer.newBuilder()
      .setFrameworkId(FRAMEWORK)
      .setAgentId(AGENT_ID)
      .setHostname(AGENT_HOST)
      .setId(OFFER_ID)
      .build();

  private static final HostOffer HOST_OFFER = new HostOffer(
      OFFER,
      IHostAttributes.build(
          new HostAttributes()
              .setHost(AGENT_HOST)
              .setSlaveId(AGENT_ID.getValue())
              .setMode(NONE)
              .setAttributes(ImmutableSet.of())));

  private static final Protos.AgentID AGENT_ID_2 =
      Protos.AgentID.newBuilder().setValue("agent-id2").build();
  private static final String AGENT_HOST_2 = "agent2-hostname";
  private static final Protos.OfferID OFFER_ID_2 =
      Protos.OfferID.newBuilder().setValue("offer-id2").build();
  private static final Protos.Offer OFFER_2 = Protos.Offer.newBuilder()
      .setFrameworkId(FRAMEWORK)
      .setAgentId(AGENT_ID_2)
      .setHostname(AGENT_HOST_2)
      .setId(OFFER_ID_2)
      .build();

  private static final HostOffer HOST_OFFER_2 = new HostOffer(
      OFFER_2,
      IHostAttributes.build(
          new HostAttributes()
              .setHost(AGENT_HOST_2)
              .setSlaveId(AGENT_ID_2.getValue())
              .setMode(NONE)
              .setAttributes(ImmutableSet.of())));

  private static final HostOffer DRAINING_HOST_OFFER = new HostOffer(
      OFFER,
      IHostAttributes.build(new HostAttributes()
          .setHost(AGENT_HOST)
          .setSlaveId(AGENT_ID.getValue())
          .setMode(DRAINING)
          .setAttributes(ImmutableSet.of())));

  private static final Protos.TaskStatus STATUS_NO_REASON = Protos.TaskStatus.newBuilder()
      .setState(Protos.TaskState.TASK_RUNNING)
      .setSource(Protos.TaskStatus.Source.SOURCE_AGENT)
      .setMessage("message")
      .setTimestamp(1D)
      .setTaskId(Protos.TaskID.newBuilder().setValue("task-id").build())
      .build();

  private static final Protos.TaskStatus STATUS = STATUS_NO_REASON
      .toBuilder()
      // Only testing data plumbing, this field with TASK_RUNNING would not normally happen,
      .setReason(Protos.TaskStatus.Reason.REASON_COMMAND_EXECUTOR_FAILED)
      .build();

  private static final Protos.TaskStatus STATUS_RECONCILIATION = STATUS_NO_REASON
      .toBuilder()
      .setReason(Protos.TaskStatus.Reason.REASON_RECONCILIATION)
      .build();

  private static final Amount<Long, Time> DRAIN_THRESHOLD = Amount.of(2L, Time.MINUTES);

  private static final Protos.InverseOffer INVERSE_OFFER = Protos.InverseOffer.newBuilder()
      .setAgentId(AGENT_ID)
      .setFrameworkId(FRAMEWORK)
      .setId(OFFER_ID)
      .setUnavailability(Protos.Unavailability.newBuilder()
          .setStart(Protos.TimeInfo.newBuilder()
              .setNanoseconds(300000000000L)
          ))
      .build();

  private static final Protos.Filters FILTER = Protos.Filters.newBuilder().build();

  private StorageTestUtil storageUtil;
  private Command shutdownCommand;
  private TaskStatusHandler statusHandler;
  private OfferManager offerManager;
  private EventSink eventSink;
  private FakeStatsProvider statsProvider;
  private Driver driver;
  private Logger injectedLog;
  private FakeClock clock;
  private MaintenanceController controller;
  private EventSink registeredEventSink;

  private MesosCallbackHandler handler;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    shutdownCommand = createMock(Command.class);
    statusHandler = createMock(TaskStatusHandler.class);
    offerManager = createMock(OfferManager.class);
    eventSink = createMock(EventSink.class);
    statsProvider = new FakeStatsProvider();
    driver = createMock(Driver.class);
    clock = new FakeClock();
    controller = createMock(MaintenanceController.class);
    registeredEventSink = createMock(EventSink.class);
    createHandler(false);
  }

  private void createHandler(boolean mockLogger) {
    createHandler(mockLogger, MoreExecutors.directExecutor());
  }

  private void createHandler(boolean mockLogger, Executor customExecutor) {
    if (mockLogger) {
      injectedLog = createMock(Logger.class);
    } else {
      injectedLog = LoggerFactory.getLogger("MesosCallbackHandlerTestLogger");
    }

    handler = new MesosCallbackHandler.MesosCallbackHandlerImpl(
        storageUtil.storage,
        new Lifecycle(shutdownCommand), // Cannot mock lifecycle
        statusHandler,
        offerManager,
        eventSink,
        customExecutor,
        injectedLog,
        statsProvider,
        driver,
        clock,
        controller,
        DRAIN_THRESHOLD,
        registeredEventSink);
  }

  @Test
  public void testRegistration() {

    storageUtil.expectOperations();

    storageUtil.schedulerStore.saveFrameworkId(FRAMEWORK_ID);

    registeredEventSink.post(new PubsubEvent.DriverRegistered());

    control.replay();

    assertEquals(0L, statsProvider.getLongValue("framework_registered"));
    handler.handleRegistration(FRAMEWORK, MASTER);
    assertEquals(1L, statsProvider.getLongValue("framework_registered"));
  }

  @Test
  public void testReRegistration() {
    control.replay();

    handler.handleReregistration(MASTER);
    assertEquals(1L, statsProvider.getLongValue("scheduler_framework_reregisters"));
    assertEquals(1L, statsProvider.getLongValue("framework_registered"));
  }

  @Test
  public void testGetEmptyOfferList() {
    control.replay();

    handler.handleOffers(ImmutableList.of());
  }

  private void expectOfferAttributesSaved(HostOffer offer) {
    expect(storageUtil.attributeStore.getHostAttributes(offer.getOffer().getHostname()))
        .andReturn(Optional.absent());
    IHostAttributes defaultMode = IHostAttributes.build(
        Conversions.getAttributes(offer.getOffer()).newBuilder().setMode(NONE));
    expect(storageUtil.attributeStore.saveHostAttributes(defaultMode)).andReturn(true);
  }

  @Test
  public void testOffers() {
    storageUtil.expectOperations();
    expectOfferAttributesSaved(HOST_OFFER);
    offerManager.add(HOST_OFFER);

    control.replay();

    handler.handleOffers(ImmutableList.of(HOST_OFFER.getOffer()));
    assertEquals(1L, statsProvider.getLongValue("scheduler_resource_offers"));
  }

  @Test
  public void testMultipleOffers() {
    storageUtil.expectOperations();
    expectOfferAttributesSaved(HOST_OFFER);
    expectOfferAttributesSaved(HOST_OFFER_2);
    offerManager.add(HOST_OFFER);
    offerManager.add(HOST_OFFER_2);

    control.replay();

    handler.handleOffers(ImmutableList.of(HOST_OFFER.getOffer(), HOST_OFFER_2.getOffer()));
    assertEquals(2L, statsProvider.getLongValue("scheduler_resource_offers"));
  }

  @Test
  public void testModePreservedWhenOfferAdded() {
    storageUtil.expectOperations();

    IHostAttributes draining =
        IHostAttributes.build(HOST_OFFER.getAttributes().newBuilder().setMode(DRAINING));
    expect(storageUtil.attributeStore.getHostAttributes(AGENT_HOST))
        .andReturn(Optional.of(draining));

    IHostAttributes saved = IHostAttributes.build(
        Conversions.getAttributes(HOST_OFFER.getOffer()).newBuilder().setMode(DRAINING));

    expect(storageUtil.attributeStore.saveHostAttributes(saved)).andReturn(true);

    // If the host is in draining, then the offer manager should get an offer with that attribute
    offerManager.add(DRAINING_HOST_OFFER);

    control.replay();
    handler.handleOffers(ImmutableList.of(HOST_OFFER.getOffer()));
    assertEquals(1L, statsProvider.getLongValue("scheduler_resource_offers"));
  }

  @Test
  public void testDisconnection() {
    eventSink.post(new PubsubEvent.DriverDisconnected());

    control.replay();

    handler.handleDisconnection();
    assertEquals(1L, statsProvider.getLongValue("scheduler_framework_disconnects"));
    assertEquals(0L, statsProvider.getLongValue("framework_registered"));
  }

  @Test
  public void testRescind() {
    expect(offerManager.cancel(OFFER_ID)).andReturn(true);

    control.replay();

    handler.handleRescind(OFFER_ID);
    assertEquals(1L, statsProvider.getLongValue("offers_rescinded"));
  }

  /**
   * Ensure that if we get a rescind and the offer has not been added yet, we will ban it and
   * eventually unban.
   */
  @Test
  public void testRescindBeforeAdd() throws InterruptedException {
    // We want to observe the order of the offerManager calls to we create a strict mock.
    offerManager = strictMock(OfferManager.class);

    FakeScheduledThreadPoolExecutor fakeExecutor = new FakeScheduledThreadPoolExecutor();
    createHandler(false, fakeExecutor);

    expect(offerManager.cancel(OFFER_ID)).andReturn(false);
    offerManager.ban(OFFER_ID);
    storageUtil.expectOperations();
    expectOfferAttributesSaved(HOST_OFFER);
    offerManager.add(HOST_OFFER);
    expect(offerManager.cancel(OFFER_ID)).andReturn(true);

    control.replay();
    replay(offerManager);

    // Offer comes in, it will be put on the executor queue to add.
    handler.handleOffers(ImmutableList.of(HOST_OFFER.getOffer()));

    // Rescind comes in asynchronously, and we do not see HOST_OFFER in available list so we will
    // temporarily ban it and add a command to the executor to unban it later. As the executor
    // processes commands, we will try to add HOST_OFFER but it should be already banned.
    // Eventually, we unban the offer.
    handler.handleRescind(OFFER_ID);

    // 2 commands executed (add and unbanOffer).
    fakeExecutor.advance();
    fakeExecutor.advance();

    assertEquals(1L, statsProvider.getLongValue("offers_rescinded"));
    assertTrue(fakeExecutor.isEmpty());
    verify(offerManager);
  }

  @Test
  public void testError() {
    shutdownCommand.execute();
    expectLastCall();

    control.replay();

    handler.handleError("Something bad happened!");
  }

  @Test
  public void testUpdate() {
    eventSink.post(new PubsubEvent.TaskStatusReceived(
        STATUS.getState(),
        Optional.fromNullable(STATUS.getSource()),
        Optional.fromNullable(STATUS.getReason()),
        Optional.of(1000000L)
    ));
    statusHandler.statusUpdate(STATUS);

    control.replay();

    handler.handleUpdate(STATUS);
    assertEquals(1L, statsProvider.getLongValue("scheduler_status_update"));
  }

  @Test
  public void testUpdateNoSource() {
    Protos.TaskStatus status = STATUS.toBuilder().clearSource().build();

    eventSink.post(new PubsubEvent.TaskStatusReceived(
        status.getState(),
        Optional.absent(),
        Optional.fromNullable(status.getReason()),
        Optional.of(1000000L)
    ));
    statusHandler.statusUpdate(status);

    control.replay();

    handler.handleUpdate(status);
    assertEquals(1L, statsProvider.getLongValue("scheduler_status_update"));
  }

  @Test
  public void testUpdateNoReason() {
    Protos.TaskStatus status = STATUS.toBuilder().clearReason().build();

    eventSink.post(new PubsubEvent.TaskStatusReceived(
        status.getState(),
        Optional.fromNullable(status.getSource()),
        Optional.absent(),
        Optional.of(1000000L)
    ));
    statusHandler.statusUpdate(status);

    control.replay();

    handler.handleUpdate(status);
    assertEquals(1L, statsProvider.getLongValue("scheduler_status_update"));
  }

  @Test
  public void testUpdateNoMessage() {
    Protos.TaskStatus status = STATUS.toBuilder().clearMessage().build();

    eventSink.post(new PubsubEvent.TaskStatusReceived(
        status.getState(),
        Optional.fromNullable(status.getSource()),
        Optional.fromNullable(status.getReason()),
        Optional.of(1000000L)
    ));
    statusHandler.statusUpdate(status);

    control.replay();

    handler.handleUpdate(status);
    assertEquals(1L, statsProvider.getLongValue("scheduler_status_update"));
  }

  @Test(expected = SchedulerException.class)
  public void testUpdateWithException() {
    eventSink.post(new PubsubEvent.TaskStatusReceived(
        STATUS.getState(),
        Optional.fromNullable(STATUS.getSource()),
        Optional.fromNullable(STATUS.getReason()),
        Optional.of(1000000L)
    ));
    statusHandler.statusUpdate(STATUS);
    expectLastCall().andThrow(new Storage.StorageException("Storage Failure"));

    control.replay();

    handler.handleUpdate(STATUS);
    assertEquals(0L, statsProvider.getLongValue("scheduler_status_update"));
  }

  @Test
  public void testReconciliationUpdateLogging() {
    // Mock the logger so we can test that it is logged at debug
    createHandler(true);
    String expectedMsg = "Received status update for task task-id in state TASK_RUNNING from "
        + "SOURCE_AGENT with REASON_RECONCILIATION: message";

    injectedLog.debug(expectedMsg);

    eventSink.post(new PubsubEvent.TaskStatusReceived(
        STATUS_RECONCILIATION.getState(),
        Optional.fromNullable(STATUS_RECONCILIATION.getSource()),
        Optional.fromNullable(STATUS_RECONCILIATION.getReason()),
        Optional.of(1000000L)
    ));

    statusHandler.statusUpdate(STATUS_RECONCILIATION);

    control.replay();

    handler.handleUpdate(STATUS_RECONCILIATION);
    assertEquals(1L, statsProvider.getLongValue("scheduler_status_update"));
  }

  @Test
  public void testNoMultilineLogging() {
    createHandler(true);

    injectedLog.info("Received status update for task task-id in state "
        + "TASK_RUNNING from SOURCE_AGENT: A (truncated)");
    injectedLog.info("Received status update for task task-id in state "
        + "TASK_RUNNING from SOURCE_AGENT: A string with one line");
    eventSink.post(anyObject());
    expectLastCall().anyTimes();
    statusHandler.statusUpdate(anyObject());
    expectLastCall().anyTimes();

    control.replay();

    handler.handleUpdate(STATUS_NO_REASON
        .toBuilder()
        .setMessage("A\nstring\nwith\nmany\nlines")
        .build());
    handler.handleUpdate(STATUS_NO_REASON
        .toBuilder()
        .setMessage("A string with one line")
        .build());
  }

  @Test
  public void testLostAgent() {
    control.replay();

    handler.handleLostAgent(AGENT_ID);
    assertEquals(1L, statsProvider.getLongValue("slaves_lost"));
  }

  @Test
  public void testLostExecutorIgnoresOkStatus() {
    control.replay();

    handler.handleLostExecutor(EXECUTOR_ID, AGENT_ID, 0);
    assertEquals(0L, statsProvider.getLongValue("scheduler_lost_executors"));
  }

  @Test
  public void testLostExecutor() {
    control.replay();

    handler.handleLostExecutor(EXECUTOR_ID, AGENT_ID, 1);
    assertEquals(1L, statsProvider.getLongValue("scheduler_lost_executors"));
  }

  @Test
  public void testMessage() {
    // Framework messages should be ignored.
    control.replay();

    handler.handleMessage(EXECUTOR_ID, AGENT_ID);
    assertEquals(1L, statsProvider.getLongValue("scheduler_framework_message"));
  }

  @Test
  public void testInverseOfferInTheFuture() {
    driver.acceptInverseOffer(OFFER_ID, FILTER);

    control.replay();

    handler.handleInverseOffer(ImmutableList.of(INVERSE_OFFER));
    assertEquals(1L, statsProvider.getLongValue("scheduler_inverse_offers"));
  }

  @Test
  public void testInverseOfferWithinThreshold() {
    clock.advance(Amount.of(4L, Time.MINUTES));

    driver.acceptInverseOffer(OFFER_ID, FILTER);
    controller.drainForInverseOffer(INVERSE_OFFER);

    control.replay();

    handler.handleInverseOffer(ImmutableList.of(INVERSE_OFFER));
    assertEquals(1L, statsProvider.getLongValue("scheduler_inverse_offers"));
  }

  /**
   * Test executor that will execute commands when {@code advance} is called.
   */
  private static class FakeScheduledThreadPoolExecutor implements Executor {
    private final Queue<Runnable> workQueue = new LinkedList<>();

    @Override
    public void execute(Runnable command) {
      workQueue.add(command);
    }

    /**
     * Returns whether or not the work queue is empty.
     */
    public boolean isEmpty() {
      return workQueue.isEmpty();
    }

    /**
     * Execute a single item from the work queue.
     */
    public void advance() {
      if (workQueue.isEmpty()) {
        throw new NoSuchElementException("No commands to execute");
      }

      workQueue.poll().run();
    }
  }
}

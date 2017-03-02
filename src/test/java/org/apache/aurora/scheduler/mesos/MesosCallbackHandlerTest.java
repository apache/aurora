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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.InetAddresses;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.aurora.common.application.Lifecycle;
import org.apache.aurora.common.base.Command;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.TaskStatusHandler;
import org.apache.aurora.scheduler.base.Conversions;
import org.apache.aurora.scheduler.base.SchedulerException;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.offers.OfferManager;
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
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;

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

  private StorageTestUtil storageUtil;
  private Command shutdownCommand;
  private TaskStatusHandler statusHandler;
  private OfferManager offerManager;
  private EventSink eventSink;
  private FakeStatsProvider statsProvider;
  private Logger injectedLog;

  private MesosCallbackHandler handler;

  @Before
  public void setUp() {

    storageUtil = new StorageTestUtil(this);
    shutdownCommand = createMock(Command.class);
    statusHandler = createMock(TaskStatusHandler.class);
    offerManager = createMock(OfferManager.class);
    eventSink = createMock(EventSink.class);
    statsProvider = new FakeStatsProvider();

    createHandler(false);
  }

  private void createHandler(boolean mockLogger) {
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
        MoreExecutors.directExecutor(),
        injectedLog,
        statsProvider);

  }

  @Test
  public void testRegistration() {

    storageUtil.expectOperations();

    storageUtil.schedulerStore.saveFrameworkId(FRAMEWORK_ID);
    expectLastCall();

    eventSink.post(new PubsubEvent.DriverRegistered());

    control.replay();

    handler.handleRegistration(FRAMEWORK, MASTER);
  }

  @Test
  public void testReRegistration() {
    control.replay();

    handler.handleReregistration(MASTER);
    assertEquals(1L, statsProvider.getLongValue("scheduler_framework_reregisters"));
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
    offerManager.addOffer(HOST_OFFER);

    control.replay();

    handler.handleOffers(ImmutableList.of(HOST_OFFER.getOffer()));
    assertEquals(1L, statsProvider.getLongValue("scheduler_resource_offers"));
  }

  @Test
  public void testMultipleOffers() {
    storageUtil.expectOperations();
    expectOfferAttributesSaved(HOST_OFFER);
    expectOfferAttributesSaved(HOST_OFFER_2);
    offerManager.addOffer(HOST_OFFER);
    offerManager.addOffer(HOST_OFFER_2);

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
    offerManager.addOffer(DRAINING_HOST_OFFER);

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
  }

  @Test
  public void testRescind() {
    offerManager.cancelOffer(OFFER_ID);

    control.replay();

    handler.handleRescind(OFFER_ID);
    assertEquals(1L, statsProvider.getLongValue("offers_rescinded"));
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
  }

  @Test
  public void testReconciliationUpdateLogging() {
    // Mock the logger so we can test that it is logged at debug
    createHandler(true);
    String expectedMsg = "Received status update for task task-id in state TASK_RUNNING from "
        + "SOURCE_AGENT with REASON_RECONCILIATION: message";

    injectedLog.debug(expectedMsg);
    expectLastCall().once();

    eventSink.post(new PubsubEvent.TaskStatusReceived(
        STATUS_RECONCILIATION.getState(),
        Optional.fromNullable(STATUS_RECONCILIATION.getSource()),
        Optional.fromNullable(STATUS_RECONCILIATION.getReason()),
        Optional.of(1000000L)
    ));

    statusHandler.statusUpdate(STATUS_RECONCILIATION);

    control.replay();

    handler.handleUpdate(STATUS_RECONCILIATION);
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
  }
}

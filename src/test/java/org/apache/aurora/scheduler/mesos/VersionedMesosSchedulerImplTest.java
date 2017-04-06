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

import java.util.concurrent.Executors;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
import com.google.protobuf.ByteString;

import org.apache.aurora.common.base.ExceptionalSupplier;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.BackoffHelper;
import org.apache.aurora.scheduler.stats.CachedCounters;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.mesos.v1.Protos.AgentID;
import org.apache.mesos.v1.Protos.ExecutorID;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.Protos.FrameworkInfo;
import org.apache.mesos.v1.Protos.MasterInfo;
import org.apache.mesos.v1.Protos.Offer;
import org.apache.mesos.v1.Protos.OfferID;
import org.apache.mesos.v1.Protos.TaskID;
import org.apache.mesos.v1.Protos.TaskState;
import org.apache.mesos.v1.Protos.TaskStatus;
import org.apache.mesos.v1.Protos.TaskStatus.Source;
import org.apache.mesos.v1.scheduler.Mesos;
import org.apache.mesos.v1.scheduler.Protos.Call;
import org.apache.mesos.v1.scheduler.Protos.Event;
import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VersionedMesosSchedulerImplTest extends EasyMockTest {

  private MesosCallbackHandler handler;
  private StorageTestUtil storageUtil;
  private Mesos driver;
  private FakeStatsProvider statsProvider;
  private FrameworkInfoFactory infoFactory;
  private BackoffHelper backoffHelper;

  private VersionedMesosSchedulerImpl scheduler;

  private static final String AGENT_HOST = "slave-hostname";
  private static final AgentID AGENT_ID =
      AgentID.newBuilder().setValue("slave-id").build();

  private static final String FRAMEWORK_ID = "framework-id";
  private static final FrameworkID FRAMEWORK =
      FrameworkID.newBuilder().setValue(FRAMEWORK_ID).build();
  private static final FrameworkInfo FRAMEWORK_INFO = FrameworkInfo.newBuilder()
      .setName("name")
      .setUser("user")
      .setCheckpoint(true)
      .setFailoverTimeout(1000)
      .build();

  private static final String MASTER_ID = "master-id";
  private static final MasterInfo MASTER = MasterInfo.newBuilder()
      .setId(MASTER_ID)
      .setIp(InetAddresses.coerceToInteger(InetAddresses.forString("1.2.3.4"))) //NOPMD
      .setPort(5050).build();

  private static final OfferID OFFER_ID = OfferID.newBuilder().setValue("offer-id").build();
  private static final Offer OFFER = Offer.newBuilder()
      .setFrameworkId(FRAMEWORK)
      .setAgentId(AGENT_ID)
      .setHostname(AGENT_HOST)
      .setId(OFFER_ID)
      .build();

  private static final TaskStatus STATUS = TaskStatus.newBuilder()
      .setState(TaskState.TASK_RUNNING)
      .setSource(Source.SOURCE_AGENT)
      .setMessage("message")
      .setTimestamp(1D)
      .setTaskId(TaskID.newBuilder().setValue("task-id").build())
      .build();

  private static final ExecutorID EXECUTOR_ID =
      ExecutorID.newBuilder().setValue("executor-id").build();

  private static final Event OFFER_EVENT = Event.newBuilder()
      .setType(Event.Type.OFFERS)
      .setOffers(Event.Offers.newBuilder()
          .addOffers(OFFER))
      .build();

  private static final Event SUBSCRIBED_EVENT = Event.newBuilder()
      .setType(Event.Type.SUBSCRIBED)
      .setSubscribed(Event.Subscribed.newBuilder()
          .setFrameworkId(FRAMEWORK)
          .setHeartbeatIntervalSeconds(15)
          .setMasterInfo(MASTER))
      .build();

  private static final Event UPDATE_EVENT = Event.newBuilder()
      .setType(Event.Type.UPDATE)
      .setUpdate(Event.Update.newBuilder()
          .setStatus(STATUS))
      .build();

  private static final Event RESCIND_EVENT = Event.newBuilder()
      .setType(Event.Type.RESCIND)
      .setRescind(Event.Rescind.newBuilder().setOfferId(OFFER_ID))
      .build();

  private static final Event MESSAGE_EVENT = Event.newBuilder()
      .setType(Event.Type.MESSAGE)
      .setMessage(Event.Message.newBuilder()
          .setAgentId(AGENT_ID)
          .setExecutorId(EXECUTOR_ID)
          .setData(ByteString.copyFromUtf8("message")))
      .build();

  private static final Event ERROR_EVENT = Event.newBuilder()
      .setType(Event.Type.ERROR)
      .setError(Event.Error.newBuilder().setMessage("Oh no!"))
      .build();

  private static final Event FAILED_AGENT_EVENT = Event.newBuilder()
      .setType(Event.Type.FAILURE)
      .setFailure(Event.Failure.newBuilder().setAgentId(AGENT_ID))
      .build();

  @Before
  public void setUp() {
    handler = createMock(MesosCallbackHandler.class);
    storageUtil = new StorageTestUtil(this);
    driver = createMock(Mesos.class);
    statsProvider = new FakeStatsProvider();
    infoFactory = createMock(FrameworkInfoFactory.class);
    backoffHelper = createMock(BackoffHelper.class);

    scheduler = new VersionedMesosSchedulerImpl(
        handler,
        new CachedCounters(statsProvider),
        statsProvider,
        storageUtil.storage,
        Executors.newSingleThreadExecutor(),
        backoffHelper,
        infoFactory);
  }

  @Test(timeout = 300000)
  public void testConnected() throws Exception {
    // Once the V1 driver has connected, we need to establish a subscription to get events
    expectFrameworkInfoRead();

    Capture<Call> subscribeCapture = createCapture();

    driver.send(capture(subscribeCapture));
    expectLastCall().once();

    Capture<ExceptionalSupplier<Boolean, RuntimeException>> supplierCapture = createCapture();
    backoffHelper.doUntilSuccess(capture(supplierCapture));
    expectLastCall().once();

    control.replay();

    scheduler.connected(driver);

    waitUntilCaptured(supplierCapture);

    assertTrue(supplierCapture.hasCaptured());
    ExceptionalSupplier<Boolean, RuntimeException> supplier = supplierCapture.getValue();

    // Make one connection attempt
    supplier.get();

    assertTrue(subscribeCapture.hasCaptured());

    Call subscribe = subscribeCapture.getValue();

    assertEquals(subscribe.getType(), Call.Type.SUBSCRIBE);
    assertEquals(subscribe.getFrameworkId(), FRAMEWORK);
    assertEquals(
        subscribe.getSubscribe().getFrameworkInfo(),
        FRAMEWORK_INFO.toBuilder().setId(FRAMEWORK).build());
  }

  @Test(timeout = 300000)
  public void testAttemptSubscriptionSuccessful() throws Exception {
    expectFrameworkInfoRead();

    // Other tests already check if what we send is correct.
    driver.send(anyObject());
    expectLastCall().once();
    driver.send(anyObject());
    expectLastCall().once();

    Capture<ExceptionalSupplier<Boolean, RuntimeException>> supplierCapture = createCapture();
    backoffHelper.doUntilSuccess(capture(supplierCapture));
    expectLastCall().once();

    handler.handleRegistration(FRAMEWORK, MASTER);

    control.replay();

    scheduler.connected(driver);

    waitUntilCaptured(supplierCapture);
    assertTrue(supplierCapture.hasCaptured());
    ExceptionalSupplier<Boolean, RuntimeException> supplier = supplierCapture.getValue();

    // Each attempt should return false.
    assertFalse(supplier.get());
    assertFalse(supplier.get());

    // After the callback we should return true because it was successful.
    scheduler.received(driver, SUBSCRIBED_EVENT);

    assertTrue(supplier.get());
  }

  @Test(timeout = 300000)
  public void testAttemptSubscriptionHaltsAfterDisconnection() throws Exception {
    storageUtil.expectOperations();
    expect(storageUtil.schedulerStore.fetchFrameworkId()).andReturn(Optional.of(FRAMEWORK_ID));
    expect(infoFactory.getFrameworkInfo()).andReturn(FRAMEWORK_INFO);

    // Other tests already check if what we send is correct.
    driver.send(anyObject());
    expectLastCall().once();

    Capture<ExceptionalSupplier<Boolean, RuntimeException>> supplierCapture = createCapture();
    backoffHelper.doUntilSuccess(capture(supplierCapture));
    expectLastCall().once();

    handler.handleDisconnection();

    control.replay();

    scheduler.connected(driver);

    waitUntilCaptured(supplierCapture);
    assertTrue(supplierCapture.hasCaptured());
    ExceptionalSupplier<Boolean, RuntimeException> supplier = supplierCapture.getValue();

    assertFalse(supplier.get());

    // After disconnection we should stop.
    scheduler.disconnected(driver);

    assertTrue(supplier.get());
  }

  private static void waitUntilCaptured(Capture<?> capture) throws Exception {
    while (!capture.hasCaptured()) {
      Thread.sleep(1000);
    }
  }

  @Test
  public void testDisconnected() {
    handler.handleDisconnection();

    control.replay();

    scheduler.disconnected(driver);
  }

  @Test
  public void testSubscription() {
    handler.handleRegistration(FRAMEWORK, MASTER);

    control.replay();

    scheduler.received(driver, SUBSCRIBED_EVENT);

    assertEquals(1L, statsProvider.getLongValue("mesos_scheduler_event_SUBSCRIBED"));
  }

  @Test
  public void testOffers() {
    handler.handleRegistration(FRAMEWORK, MASTER);
    handler.handleOffers(ImmutableList.of(OFFER));

    control.replay();

    scheduler.received(driver, SUBSCRIBED_EVENT);
    scheduler.received(driver, OFFER_EVENT);
    assertEquals(1L, statsProvider.getLongValue("mesos_scheduler_event_OFFERS"));
  }

  @Test
  public void testRescind() {
    handler.handleRescind(OFFER_ID);

    control.replay();

    scheduler.received(driver, RESCIND_EVENT);
    assertEquals(1L, statsProvider.getLongValue("mesos_scheduler_event_RESCIND"));
  }

  @Test
  public void testUpdate() {
    handler.handleUpdate(STATUS);

    control.replay();

    scheduler.received(driver, UPDATE_EVENT);

    assertEquals(1L, statsProvider.getLongValue("mesos_scheduler_event_UPDATE"));
  }

  @Test(expected = IllegalStateException.class)
  public void testBadOrdering() {
    // get an offer before the driver has registered

    control.replay();

    scheduler.received(driver, OFFER_EVENT);
  }

  @Test
  public void testMessage() {
    handler.handleMessage(EXECUTOR_ID, AGENT_ID);

    control.replay();

    scheduler.received(driver, MESSAGE_EVENT);
    assertEquals(1L, statsProvider.getLongValue("mesos_scheduler_event_MESSAGE"));
  }

  @Test
  public void testError() {
    handler.handleError("Oh no!");

    control.replay();

    scheduler.received(driver, ERROR_EVENT);
    assertEquals(1L, statsProvider.getLongValue("mesos_scheduler_event_ERROR"));
  }

  @Test
  public void testFailedAgent() {
    handler.handleLostAgent(AGENT_ID);

    control.replay();

    scheduler.received(driver, FAILED_AGENT_EVENT);
    assertEquals(1L, statsProvider.getLongValue("mesos_scheduler_event_FAILURE"));
  }

  @Test(timeout = 300000)
  public void testSubscribeDisconnectSubscribeCycle() throws Exception {
    expectFrameworkInfoRead();

    Capture<ExceptionalSupplier<Boolean, RuntimeException>> firstSubscribeAttempt = createCapture();
    backoffHelper.doUntilSuccess(capture(firstSubscribeAttempt));
    expectLastCall().once();

    handler.handleRegistration(FRAMEWORK, MASTER);
    handler.handleDisconnection();

    Capture<ExceptionalSupplier<Boolean, RuntimeException>> secondSubscribeAttempt =
        createCapture();
    backoffHelper.doUntilSuccess(capture(secondSubscribeAttempt));
    expectLastCall().once();

    // Second subscribe should call the reregistration handler.
    handler.handleReregistration(MASTER);

    control.replay();

    scheduler.connected(driver);

    waitUntilCaptured(firstSubscribeAttempt);
    assertTrue(firstSubscribeAttempt.hasCaptured());

    scheduler.received(driver, SUBSCRIBED_EVENT);
    scheduler.disconnected(driver);
    scheduler.connected(driver);

    waitUntilCaptured(secondSubscribeAttempt);
    assertTrue(secondSubscribeAttempt.hasCaptured());

    scheduler.received(driver, SUBSCRIBED_EVENT);
  }

  private void expectFrameworkInfoRead() {
    storageUtil.expectOperations();
    expect(storageUtil.schedulerStore.fetchFrameworkId())
        .andReturn(Optional.of(FRAMEWORK_ID))
        .anyTimes();
    expect(infoFactory.getFrameworkInfo()).andReturn(FRAMEWORK_INFO).anyTimes();
  }
}

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

import java.nio.charset.StandardCharsets;

import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.v1.Protos.ExecutorID;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.Protos.OfferID;
import org.apache.mesos.v1.Protos.TaskID;
import org.apache.mesos.v1.Protos.TaskState;
import org.apache.mesos.v1.Protos.TaskStatus;
import org.apache.mesos.v1.Protos.TaskStatus.Reason;
import org.apache.mesos.v1.Protos.TaskStatus.Source;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.mesos.ProtosConversion.convert;
import static org.easymock.EasyMock.expectLastCall;

public class MesosSchedulerImplTest extends EasyMockTest {

  private static final String FRAMEWORK_ID = "framework-id";
  private static final FrameworkID FRAMEWORK =
      FrameworkID.newBuilder().setValue(FRAMEWORK_ID).build();
  private static final String MASTER_ID = "master-id";
  private static final Protos.MasterInfo MASTER = Protos.MasterInfo.newBuilder()
      .setId(MASTER_ID)
      .setIp(InetAddresses.coerceToInteger(InetAddresses.forString("1.2.3.4"))) //NOPMD
      .setPort(5050).build();
  private static final String SLAVE_HOST = "slave-hostname";
  private static final Protos.SlaveID SLAVE_ID =
      Protos.SlaveID.newBuilder().setValue("slave-id").build();

  private static final OfferID OFFER_ID = OfferID.newBuilder().setValue("offer-id").build();
  private static final Protos.Offer OFFER = Protos.Offer.newBuilder()
          .setFrameworkId(convert(FRAMEWORK))
          .setSlaveId(SLAVE_ID)
          .setHostname(SLAVE_HOST)
          .setId(convert(OFFER_ID))
          .build();

  private static final ExecutorID EXECUTOR_ID =
      ExecutorID.newBuilder().setValue("executor-id").build();

  private static final TaskStatus STATUS_NO_REASON = TaskStatus.newBuilder()
      .setState(TaskState.TASK_RUNNING)
      .setSource(Source.SOURCE_AGENT)
      .setMessage("message")
      .setTimestamp(1D)
      .setTaskId(TaskID.newBuilder().setValue("task-id").build())
      .build();

  private static final TaskStatus STATUS = STATUS_NO_REASON
      .toBuilder()
      // Only testing data plumbing, this field with TASK_RUNNING would not normally happen,
      .setReason(Reason.REASON_COMMAND_EXECUTOR_FAILED)
      .build();

  private MesosCallbackHandler handler;
  private SchedulerDriver driver;

  private MesosSchedulerImpl scheduler;

  @Before
  public void setUp() {
    handler = createMock(MesosCallbackHandler.class);
    driver = createMock(SchedulerDriver.class);
    scheduler = new MesosSchedulerImpl(handler);
  }

  @Test
  public void testSlaveLost() {
    handler.handleLostAgent(convert(SLAVE_ID));
    expectLastCall().once();

    control.replay();

    scheduler.slaveLost(driver, SLAVE_ID);
  }

  @Test
  public void testRegistered() {
    handler.handleRegistration(FRAMEWORK, convert(MASTER));
    expectLastCall().once();

    control.replay();

    scheduler.registered(driver, convert(FRAMEWORK), MASTER);
  }

  @Test
  public void testDisconnected() {
    handler.handleDisconnection();
    expectLastCall().once();

    control.replay();

    scheduler.disconnected(driver);
  }

  @Test
  public void testReRegistered() {
    handler.handleReregistration(convert(MASTER));
    expectLastCall().once();

    control.replay();

    scheduler.reregistered(driver, MASTER);
  }

  @Test
  public void testResourceOffers() {
    handler.handleRegistration(FRAMEWORK, convert(MASTER));
    expectLastCall().once();
    handler.handleOffers(ImmutableList.of(convert(OFFER)));
    expectLastCall().once();

    control.replay();

    scheduler.registered(driver, convert(FRAMEWORK), MASTER);
    scheduler.resourceOffers(driver, ImmutableList.of(OFFER));
  }

  @Test(expected = IllegalStateException.class)
  public void testBadOrdering() {
    control.replay();

    // Should fail since the scheduler is not yet registered.
    scheduler.resourceOffers(driver, ImmutableList.of());
  }

  @Test
  public void testOfferRescinded() {
    handler.handleRescind(OFFER_ID);
    expectLastCall().once();

    control.replay();

    scheduler.offerRescinded(driver, convert(OFFER_ID));
  }

  @Test
  public void testStatusUpdate() {
    handler.handleUpdate(STATUS);
    expectLastCall().once();

    control.replay();

    scheduler.statusUpdate(driver, convert(STATUS));
  }

  @Test
  public void testError() {
    handler.handleError("Oh No!");
    expectLastCall().once();

    control.replay();

    scheduler.error(driver, "Oh No!");
  }

  @Test
  public void testFrameworkMessage() {
    handler.handleMessage(EXECUTOR_ID, convert(SLAVE_ID));
    expectLastCall().once();

    control.replay();

    scheduler.frameworkMessage(
        driver,
        convert(EXECUTOR_ID),
        SLAVE_ID,
        "message".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testExecutorLost() {
    handler.handleLostExecutor(EXECUTOR_ID, convert(SLAVE_ID), 1);

    control.replay();

    scheduler.executorLost(driver, convert(EXECUTOR_ID), SLAVE_ID, 1);
  }
}

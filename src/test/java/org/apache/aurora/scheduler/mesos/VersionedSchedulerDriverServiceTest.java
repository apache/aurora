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

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.scheduler.Mesos;
import org.apache.mesos.v1.scheduler.Protos.Call;
import org.apache.mesos.v1.scheduler.Scheduler;
import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VersionedSchedulerDriverServiceTest extends EasyMockTest {

  private static final String FRAMEWORK_ID = "framework_id";
  private static final Protos.OfferID OFFER_ID =
      Protos.OfferID.newBuilder().setValue("offer-id").build();
  private static final Protos.Filters FILTER =
      Protos.Filters.newBuilder().setRefuseSeconds(10).build();
  private static final DriverSettings SETTINGS = new DriverSettings(
      "fakemaster",
      Optional.absent(),
      Protos.FrameworkInfo.newBuilder()
          .setUser("framework user")
          .setName("test framework")
          .build());

  private StorageTestUtil storage;
  private Scheduler scheduler;
  private VersionedSchedulerDriverService driverService;
  private VersionedDriverFactory driverFactory;
  private Mesos mesos;

  @Before
  public void setUp() {
    scheduler = createMock(Scheduler.class);
    storage = new StorageTestUtil(this);
    driverFactory = createMock(VersionedDriverFactory.class);
    mesos = createMock(Mesos.class);
    driverService = new VersionedSchedulerDriverService(
        storage.storage,
        SETTINGS,
        scheduler,
        driverFactory);
  }

  @Test
  public void testNoopStop() {
    control.replay();

    driverService.stopAsync().awaitTerminated();
  }

  @Test
  public void testStop() {
    expectStart();
    control.replay();

    driverService.startAsync().awaitRunning();
    driverService.stopAsync().awaitTerminated();
  }

  @Test(expected = IllegalStateException.class)
  public void testExceptionBeforeStart() {
    control.replay();
    driverService.killTask("task-id");
  }

  @Test
  public void testBlockingBeforeRegistered() throws InterruptedException {
    expectStart();
    control.replay();
    driverService.startAsync().awaitRunning();

    Thread killRunner = new Thread(() -> {
      driverService.killTask("task-id");
    });

    killRunner.start();

    // A hack to ensure the thread actually executes the method
    Thread.sleep(1000L);
    assertEquals(Thread.State.WAITING, killRunner.getState());

    killRunner.interrupt();
  }

  @Test
  public void testKill() {
    expectStart();
    expect(storage.schedulerStore.fetchFrameworkId()).andReturn(Optional.of(FRAMEWORK_ID));

    Capture<Call> killCapture = createCapture();
    mesos.send(capture(killCapture));
    expectLastCall().once();

    control.replay();
    driverService.startAsync().awaitRunning();
    driverService.registered(new PubsubEvent.DriverRegistered());

    driverService.killTask("task-id");

    assertTrue(killCapture.hasCaptured());
    Call kill = killCapture.getValue();
    assertEquals(kill.getFrameworkId().getValue(), FRAMEWORK_ID);
    assertEquals(kill.getType(), Call.Type.KILL);
    assertEquals(kill.getKill().getTaskId().getValue(), "task-id");
  }

  @Test
  public void testDecline() {
    expectStart();
    expect(storage.schedulerStore.fetchFrameworkId()).andReturn(Optional.of(FRAMEWORK_ID));

    Capture<Call> declineCapture = createCapture();
    mesos.send(capture(declineCapture));
    expectLastCall().once();

    control.replay();
    driverService.startAsync().awaitRunning();
    driverService.registered(new PubsubEvent.DriverRegistered());

    driverService.declineOffer(OFFER_ID, FILTER);

    assertTrue(declineCapture.hasCaptured());
    Call decline = declineCapture.getValue();
    assertEquals(decline.getFrameworkId().getValue(), FRAMEWORK_ID);
    assertEquals(decline.getType(), Call.Type.DECLINE);
    assertEquals(decline.getDecline().getOfferIdsList(), ImmutableList.of(OFFER_ID));
    assertEquals(decline.getDecline().getFilters(), FILTER);
  }

  @Test
  public void testAccept() {
    expectStart();
    expect(storage.schedulerStore.fetchFrameworkId()).andReturn(Optional.of(FRAMEWORK_ID));

    Capture<Call> acceptCapture = createCapture();
    mesos.send(capture(acceptCapture));
    expectLastCall().once();

    control.replay();
    driverService.startAsync().awaitRunning();
    driverService.registered(new PubsubEvent.DriverRegistered());

    driverService.acceptOffers(OFFER_ID, ImmutableList.of(), FILTER);

    assertTrue(acceptCapture.hasCaptured());
    Call accept = acceptCapture.getValue();
    assertEquals(accept.getFrameworkId().getValue(), FRAMEWORK_ID);
    assertEquals(accept.getType(), Call.Type.ACCEPT);
    assertEquals(accept.getAccept().getFilters(), FILTER);
    assertEquals(accept.getAccept().getOfferIdsList(), ImmutableList.of(OFFER_ID));
    assertEquals(accept.getAccept().getOperationsList(), ImmutableList.of());
  }

  private void expectStart() {
    storage.expectOperations();
    expect(storage.schedulerStore.fetchFrameworkId()).andReturn(Optional.of(FRAMEWORK_ID));

    Protos.FrameworkInfo.Builder builder = SETTINGS.getFrameworkInfo().toBuilder();
    builder.setId(Protos.FrameworkID.newBuilder().setValue(FRAMEWORK_ID));

    expect(driverFactory.create(
        scheduler,
        builder.build(),
        SETTINGS.getMasterUri(),
        SETTINGS.getCredentials()
    )).andReturn(mesos);
  }
}

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
package org.apache.aurora.scheduler;

import java.util.concurrent.TimeUnit;

import org.apache.aurora.GuavaUtils.ServiceManagerIface;
import org.apache.aurora.common.application.Lifecycle;
import org.apache.aurora.common.application.ShutdownRegistry;
import org.apache.aurora.common.base.Command;
import org.apache.aurora.common.base.ExceptionalCommand;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.zookeeper.SingletonService.LeaderControl;
import org.apache.aurora.common.zookeeper.SingletonService.LeadershipListener;
import org.apache.aurora.scheduler.SchedulerLifecycle.DelayedActions;
import org.apache.aurora.scheduler.events.PubsubEvent.DriverRegistered;
import org.apache.aurora.scheduler.mesos.Driver;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.SchedulerLifecycle.State;
import static org.apache.aurora.scheduler.SchedulerLifecycle.stateGaugeName;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SchedulerLifecycleTest extends EasyMockTest {
  private StorageTestUtil storageUtil;
  private ShutdownSystem shutdownRegistry;
  private Driver driver;
  private LeaderControl leaderControl;
  private DelayedActions delayedActions;
  private FakeStatsProvider statsProvider;
  private ServiceManagerIface serviceManager;

  private SchedulerLifecycle schedulerLifecycle;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    shutdownRegistry = createMock(ShutdownSystem.class);
    driver = createMock(Driver.class);
    leaderControl = createMock(LeaderControl.class);
    delayedActions = createMock(DelayedActions.class);
    statsProvider = new FakeStatsProvider();
    serviceManager = createMock(ServiceManagerIface.class);
  }

  /**
   * Composite interface to mimic a ShutdownRegistry implementation that can be triggered.
   */
  private interface ShutdownSystem extends ShutdownRegistry, Command {
  }

  private Capture<ExceptionalCommand<?>> replayAndCreateLifecycle() {
    Capture<ExceptionalCommand<?>> shutdownCommand = createCapture();
    shutdownRegistry.addAction(EasyMock.<ExceptionalCommand<?>>capture(shutdownCommand));

    control.replay();

    schedulerLifecycle = new SchedulerLifecycle(
        storageUtil.storage,
        new Lifecycle(shutdownRegistry),
        driver,
        delayedActions,
        shutdownRegistry,
        statsProvider,
        serviceManager);
    assertEquals(0, statsProvider.getValue(SchedulerLifecycle.REGISTERED_GAUGE));
    assertEquals(1, statsProvider.getValue(stateGaugeName(State.IDLE)));
    return shutdownCommand;
  }

  private void expectLoadStorage() {
    storageUtil.storage.start(EasyMock.anyObject());
    storageUtil.expectOperations();
  }

  private void expectInitializeDriver() {
    expect(driver.startAsync()).andReturn(driver);
    driver.awaitRunning();
    delayedActions.blockingDriverJoin(EasyMock.anyObject());
  }

  private void expectFullStartup() throws Exception {
    leaderControl.advertise();

    expect(serviceManager.startAsync()).andReturn(serviceManager);
    serviceManager.awaitHealthy();
  }

  private void expectShutdown() throws Exception {
    leaderControl.leave();
    expect(driver.stopAsync()).andReturn(driver);
    driver.awaitTerminated();
    storageUtil.storage.stop();
    shutdownRegistry.execute();
  }

  @Test
  public void testAutoFailover() throws Exception {
    // Test that when timed failover is initiated, cleanup is done in a way that should allow the
    // application to tear down cleanly.  Specifically, neglecting to call leaderControl.leave()
    // can result in a lame duck scheduler process.

    storageUtil.storage.prepare();
    expectLoadStorage();
    Capture<Runnable> triggerFailover = createCapture();
    delayedActions.onAutoFailover(capture(triggerFailover));
    delayedActions.onRegistrationTimeout(EasyMock.anyObject());
    expectInitializeDriver();

    expectFullStartup();
    expectShutdown();

    replayAndCreateLifecycle();

    LeadershipListener leaderListener = schedulerLifecycle.prepare();
    assertEquals(1, statsProvider.getValue(stateGaugeName(State.STORAGE_PREPARED)));
    leaderListener.onLeading(leaderControl);
    assertEquals(1, statsProvider.getValue(stateGaugeName(State.LEADER_AWAITING_REGISTRATION)));
    assertEquals(0, statsProvider.getValue(SchedulerLifecycle.REGISTERED_GAUGE));
    schedulerLifecycle.registered(new DriverRegistered());
    assertEquals(1, statsProvider.getValue(stateGaugeName(State.ACTIVE)));
    assertEquals(1, statsProvider.getValue(SchedulerLifecycle.REGISTERED_GAUGE));
    triggerFailover.getValue().run();
  }

  @Test
  public void testRegistrationTimeout() throws Exception {
    storageUtil.storage.prepare();
    expectLoadStorage();
    delayedActions.onAutoFailover(EasyMock.anyObject());
    Capture<Runnable> registrationTimeout = createCapture();
    delayedActions.onRegistrationTimeout(capture(registrationTimeout));
    expect(driver.startAsync()).andReturn(driver);
    driver.awaitRunning();

    expectShutdown();

    replayAndCreateLifecycle();

    LeadershipListener leaderListener = schedulerLifecycle.prepare();
    leaderListener.onLeading(leaderControl);
    registrationTimeout.getValue().run();
  }

  @Test
  public void testDefeatedBeforeRegistered() throws Exception {
    storageUtil.storage.prepare();
    expectLoadStorage();
    delayedActions.onAutoFailover(EasyMock.anyObject());
    delayedActions.onRegistrationTimeout(EasyMock.anyObject());
    expect(driver.startAsync()).andReturn(driver);
    driver.awaitRunning();

    // Important piece here is what's absent - leader presence is not advertised.
    expectShutdown();

    replayAndCreateLifecycle();

    LeadershipListener leaderListener = schedulerLifecycle.prepare();
    leaderListener.onLeading(leaderControl);
    leaderListener.onDefeated();
  }

  @Test
  public void testStorageStartFails() throws Exception {
    storageUtil.storage.prepare();
    storageUtil.expectOperations();
    storageUtil.storage.start(EasyMock.anyObject());
    expectLastCall().andThrow(new StorageException("Recovery failed."));
    expectShutdown();

    replayAndCreateLifecycle();

    LeadershipListener leaderListener = schedulerLifecycle.prepare();

    try {
      leaderListener.onLeading(leaderControl);
      fail();
    } catch (StorageException e) {
      // Expected.
    }
  }

  @Test
  public void testExternalShutdown() throws Exception {
    storageUtil.storage.prepare();
    expectLoadStorage();
    Capture<Runnable> triggerFailover = createCapture();
    delayedActions.onAutoFailover(capture(triggerFailover));
    delayedActions.onRegistrationTimeout(EasyMock.anyObject());
    expectInitializeDriver();

    expectFullStartup();
    expectShutdown();
    expect(serviceManager.stopAsync()).andReturn(serviceManager);
    serviceManager.awaitStopped(5, TimeUnit.SECONDS);

    Capture<ExceptionalCommand<?>> shutdownCommand = replayAndCreateLifecycle();

    LeadershipListener leaderListener = schedulerLifecycle.prepare();
    leaderListener.onLeading(leaderControl);
    schedulerLifecycle.registered(new DriverRegistered());
    shutdownCommand.getValue().execute();
  }
}

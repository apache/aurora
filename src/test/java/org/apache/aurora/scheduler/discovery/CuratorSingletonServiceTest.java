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
package org.apache.aurora.scheduler.discovery;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.zookeeper.SingletonService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.easymock.Capture;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createControl;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.newCapture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CuratorSingletonServiceTest extends BaseCuratorDiscoveryTest {

  private IMocksControl control;
  // This test has a lot of blocking, this ensures we don't deadlock.
  private final Timeout timeout = new Timeout(1, TimeUnit.MINUTES);

  @Rule
  public Timeout getTimeout() {
    return timeout;
  }

  @Before
  public void setUpSingletonService() throws Exception {
    control = createControl();
    addTearDown(control::verify);
  }

  private SingletonService.LeadershipListener createMockLeadershipListener() {
    return control.createMock(SingletonService.LeadershipListener.class);
  }

  private void newLeader(
      CuratorFramework client,
      String hostName,
      SingletonService.LeadershipListener listener)
      throws Exception {

    CuratorSingletonService singletonService =
        new CuratorSingletonService(client, GROUP_PATH, MEMBER_TOKEN);
    InetSocketAddress leaderEndpoint = InetSocketAddress.createUnresolved(hostName, PRIMARY_PORT);
    singletonService.lead(leaderEndpoint, ImmutableMap.of(), listener);
  }

  @Test
  public void testLeadAdvertise() throws Exception {
    SingletonService.LeadershipListener listener = createMockLeadershipListener();
    Capture<SingletonService.LeaderControl> capture = newCapture();
    listener.onLeading(capture(capture));
    expectLastCall();

    control.replay();

    startGroupMonitor();

    // Can't be leader until we try to lead.
    assertFalse(capture.hasCaptured());

    newLeader(getClient(), "host1", listener);

    // Wait to become leader.
    expectGroupEvent(PathChildrenCacheEvent.Type.CHILD_ADDED);
    awaitCapture(capture);

    // Leadership nodes should not be seen as service group nodes.
    assertEquals(ImmutableSet.of(), getGroupMonitor().get());

    capture.getValue().advertise();

    // Verify we've advertised as leader.
    expectGroupEvent(PathChildrenCacheEvent.Type.CHILD_ADDED);
    assertEquals(ImmutableSet.of(serviceInstance("host1")), getGroupMonitor().get());
  }

  @Test
  public void testAbdicateTransition() throws Exception {
    SingletonService.LeadershipListener host1Listener = createMockLeadershipListener();
    Capture<SingletonService.LeaderControl> host1OnLeadingCapture = newCapture();
    host1Listener.onLeading(capture(host1OnLeadingCapture));
    expectLastCall();

    SingletonService.LeadershipListener host2Listener = createMockLeadershipListener();
    Capture<SingletonService.LeaderControl> host2OnLeadingCapture = newCapture();
    host2Listener.onLeading(capture(host2OnLeadingCapture));
    expectLastCall();

    control.replay();

    startGroupMonitor();

    // Have host1 become leader.
    newLeader(getClient(), "host1", host1Listener);
    expectGroupEvent(PathChildrenCacheEvent.Type.CHILD_ADDED);
    awaitCapture(host1OnLeadingCapture);

    host1OnLeadingCapture.getValue().advertise();
    expectGroupEvent(PathChildrenCacheEvent.Type.CHILD_ADDED);
    assertEquals(ImmutableSet.of(serviceInstance("host1")), getGroupMonitor().get());

    newLeader(getClient(), "host2", host2Listener);
    expectGroupEvent(PathChildrenCacheEvent.Type.CHILD_ADDED);
    assertFalse(host2OnLeadingCapture.hasCaptured());

    // Now have host1 abdicate.
    host1OnLeadingCapture.getValue().leave();

    // Should see both the leadership and service group member nodes get cleaned up by host1.
    expectGroupEvent(PathChildrenCacheEvent.Type.CHILD_REMOVED);
    expectGroupEvent(PathChildrenCacheEvent.Type.CHILD_REMOVED);

    awaitCapture(host2OnLeadingCapture);
  }

  @Test
  public void testDefeatTransition() throws Exception {
    SingletonService.LeadershipListener host1Listener = createMockLeadershipListener();
    Capture<SingletonService.LeaderControl> host1OnLeadingCapture = newCapture();
    host1Listener.onLeading(capture(host1OnLeadingCapture));
    expectLastCall();

    CountDownLatch host1Defeated = new CountDownLatch(1);
    host1Listener.onDefeated();
    expectLastCall().andAnswer(() -> {
      host1Defeated.countDown();
      return null;
    });

    SingletonService.LeadershipListener host2Listener = createMockLeadershipListener();
    Capture<SingletonService.LeaderControl> host2OnLeadingCapture = newCapture();
    host2Listener.onLeading(capture(host2OnLeadingCapture));
    expectLastCall();

    control.replay();

    startGroupMonitor();

    // Have host1 become leader.
    newLeader(getClient(), "host1", host1Listener);
    expectGroupEvent(PathChildrenCacheEvent.Type.CHILD_ADDED);
    awaitCapture(host1OnLeadingCapture);

    host1OnLeadingCapture.getValue().advertise();
    expectGroupEvent(PathChildrenCacheEvent.Type.CHILD_ADDED);
    assertEquals(ImmutableSet.of(serviceInstance("host1")), getGroupMonitor().get());

    newLeader(startNewClient(), "host2", host2Listener);
    expectGroupEvent(PathChildrenCacheEvent.Type.CHILD_ADDED);
    assertFalse(host2OnLeadingCapture.hasCaptured());

    // Simulate a session timeout - the ephemeral leader node goes away and host1 should be
    // defeated.
    expireSession(getClient());

    // Should see both the leadership and service group member nodes go away as part of session
    // expiration for ephemeral nodes.
    expectGroupEvent(PathChildrenCacheEvent.Type.CHILD_REMOVED);
    expectGroupEvent(PathChildrenCacheEvent.Type.CHILD_REMOVED);

    awaitCapture(host2OnLeadingCapture);

    // No advertisement by host2 yet, even though it won leadership, but the host1 service group
    // node should have been cleaned up as tested above.
    assertEquals(ImmutableSet.of(), getGroupMonitor().get());

    // Eventually host1 should notice its been defeated.
    host1Defeated.await();
  }

  @Test
  public void testZKDisconnection() throws Exception {
    CountDownLatch leading = new CountDownLatch(1);
    AtomicBoolean leader = new AtomicBoolean(false);
    CountDownLatch defeated = new CountDownLatch(1);

    // The listener is executed in an internal thread of Curator, where it executes the leader
    // listener callbacks. The exceptions there are not propagated out, so we have our own
    // listener to validate behaviour.
    SingletonService.LeadershipListener listener = new SingletonService.LeadershipListener() {
      @Override
      public void onLeading(SingletonService.LeaderControl leaderControl) {
        leader.set(true);
        leading.countDown();
      }

      @Override
      public void onDefeated() {
        leader.set(false);
        defeated.countDown();
      }
    };

    control.replay();

    startGroupMonitor();

    CuratorFramework client = getClient();
    newLeader(client, "host1", listener);
    leading.await();

    causeDisconnection();
    assertTrue(leader.get());

    expireSession(client);
    defeated.await();

    assertFalse(leader.get());
  }

  private void awaitCapture(Capture<?> capture) throws InterruptedException {
    while (!capture.hasCaptured()) {
      Thread.sleep(1L);
    }
  }
}

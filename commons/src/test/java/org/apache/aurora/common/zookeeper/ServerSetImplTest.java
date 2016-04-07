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
package org.apache.aurora.common.zookeeper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.aurora.common.base.Command;
import org.apache.aurora.common.net.pool.DynamicHostSet;
import org.apache.aurora.common.thrift.Endpoint;
import org.apache.aurora.common.thrift.ServiceInstance;
import org.apache.aurora.common.thrift.Status;
import org.apache.aurora.common.zookeeper.Group.JoinException;
import org.apache.aurora.common.zookeeper.testing.BaseZooKeeperClientTest;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createControl;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 * TODO(William Farner): Change this to remove thrift dependency.
 */
public class ServerSetImplTest extends BaseZooKeeperClientTest {
  private static final List<ACL> ACL = ZooDefs.Ids.OPEN_ACL_UNSAFE;
  private static final String SERVICE = "/twitter/services/puffin_hosebird";

  private LinkedBlockingQueue<ImmutableSet<ServiceInstance>> serverSetBuffer;
  private DynamicHostSet.HostChangeMonitor<ServiceInstance> serverSetMonitor;

  @Before
  public void mySetUp() throws IOException {
    serverSetBuffer = new LinkedBlockingQueue<>();
    serverSetMonitor = serverSetBuffer::offer;
  }

  private ServerSetImpl createServerSet() throws IOException {
    return new ServerSetImpl(createZkClient(), ACL, SERVICE);
  }

  @Test
  public void testLifecycle() throws Exception {
    ServerSetImpl client = createServerSet();
    client.watch(serverSetMonitor);
    assertChangeFiredEmpty();

    ServerSetImpl server = createServerSet();
    ServerSet.EndpointStatus status = server.join(
        InetSocketAddress.createUnresolved("foo", 1234), makePortMap("http-admin", 8080));

    ServiceInstance serviceInstance = new ServiceInstance(
        new Endpoint("foo", 1234),
        ImmutableMap.of("http-admin", new Endpoint("foo", 8080)),
        Status.ALIVE);

    assertChangeFired(serviceInstance);

    status.leave();
    assertChangeFiredEmpty();
    assertTrue(serverSetBuffer.isEmpty());
  }

  @Test
  public void testMembershipChanges() throws Exception {
    ServerSetImpl client = createServerSet();
    client.watch(serverSetMonitor);
    assertChangeFiredEmpty();

    ServerSetImpl server = createServerSet();

    ServerSet.EndpointStatus foo = join(server, "foo");
    assertChangeFired("foo");

    expireSession(client.getZkClient());

    ServerSet.EndpointStatus bar = join(server, "bar");

    // We should've auto re-monitored membership, but not been notifed of "foo" since this was not a
    // change, just "foo", "bar" since this was an addition.
    assertChangeFired("foo", "bar");

    foo.leave();
    assertChangeFired("bar");

    ServerSet.EndpointStatus baz = join(server, "baz");
    assertChangeFired("bar", "baz");

    baz.leave();
    assertChangeFired("bar");

    bar.leave();
    assertChangeFiredEmpty();

    assertTrue(serverSetBuffer.isEmpty());
  }

  @Test
  public void testStopMonitoring() throws Exception {
    ServerSetImpl client = createServerSet();
    Command stopMonitoring = client.watch(serverSetMonitor);
    assertChangeFiredEmpty();

    ServerSetImpl server = createServerSet();

    ServerSet.EndpointStatus foo = join(server, "foo");
    assertChangeFired("foo");
    ServerSet.EndpointStatus bar = join(server, "bar");
    assertChangeFired("foo", "bar");

    stopMonitoring.execute();

    // No new updates should be received since monitoring has stopped.
    foo.leave();
    assertTrue(serverSetBuffer.isEmpty());

    // Expiration event.
    assertTrue(serverSetBuffer.isEmpty());
  }

  @Test
  public void testOrdering() throws Exception {
    ServerSetImpl client = createServerSet();
    client.watch(serverSetMonitor);
    assertChangeFiredEmpty();

    Map<String, InetSocketAddress> server1Ports = makePortMap("http-admin1", 8080);
    Map<String, InetSocketAddress> server2Ports = makePortMap("http-admin2", 8081);
    Map<String, InetSocketAddress> server3Ports = makePortMap("http-admin3", 8082);

    ServerSetImpl server1 = createServerSet();
    ServerSetImpl server2 = createServerSet();
    ServerSetImpl server3 = createServerSet();

    ServiceInstance instance1 = new ServiceInstance(
        new Endpoint("foo", 1000),
        ImmutableMap.of("http-admin1", new Endpoint("foo", 8080)),
        Status.ALIVE);
    ServiceInstance instance2 = new ServiceInstance(
        new Endpoint("foo", 1001),
        ImmutableMap.of("http-admin2", new Endpoint("foo", 8081)),
        Status.ALIVE);
    ServiceInstance instance3 = new ServiceInstance(
        new Endpoint("foo", 1002),
        ImmutableMap.of("http-admin3", new Endpoint("foo", 8082)),
        Status.ALIVE);

    server1.join(InetSocketAddress.createUnresolved("foo", 1000), server1Ports);
    assertEquals(ImmutableList.of(instance1), ImmutableList.copyOf(serverSetBuffer.take()));

    ServerSet.EndpointStatus status2 = server2.join(
        InetSocketAddress.createUnresolved("foo", 1001),
        server2Ports);
    assertEquals(ImmutableList.of(instance1, instance2),
        ImmutableList.copyOf(serverSetBuffer.take()));

    server3.join(InetSocketAddress.createUnresolved("foo", 1002), server3Ports);
    assertEquals(ImmutableList.of(instance1, instance2, instance3),
        ImmutableList.copyOf(serverSetBuffer.take()));

    status2.leave();
    assertEquals(ImmutableList.of(instance1, instance3),
        ImmutableList.copyOf(serverSetBuffer.take()));
  }

  @Test
  public void testUnwatchOnException() throws Exception {
    IMocksControl control = createControl();

    ZooKeeperClient zkClient = control.createMock(ZooKeeperClient.class);
    Watcher onExpirationWatcher = control.createMock(Watcher.class);

    expect(zkClient.registerExpirationHandler(anyObject(Command.class)))
        .andReturn(onExpirationWatcher);

    expect(zkClient.get()).andThrow(new InterruptedException());  // See interrupted() note below.
    expect(zkClient.unregister(onExpirationWatcher)).andReturn(true);
    control.replay();

    Group group = new Group(zkClient, ZooDefs.Ids.OPEN_ACL_UNSAFE, "/blabla");
    ServerSetImpl serverset = new ServerSetImpl(zkClient, group);

    try {
      serverset.watch(hostSet -> {});
      fail("Expected MonitorException");
    } catch (DynamicHostSet.MonitorException e) {
      // NB: The assert is not important to this test, but the call to `Thread.interrupted()` is.
      // That call both returns the current interrupted status as well as clearing it.  The clearing
      // is crucial depending on the order tests are run in this class.  If this test runs before
      // one of the tests above that uses a `ZooKeeperClient` for example, those tests will fail
      // executing `ZooKeeperClient.get` which internally blocks on s sync-point that takes part in
      // the interruption mechanism and so immediately throws `InterruptedException` based on the
      // un-cleared interrupted bit.
      assertTrue(Thread.interrupted());
    }
    control.verify();
  }

  private static Map<String, InetSocketAddress> makePortMap(String name, int port) {
    return ImmutableMap.of(name, InetSocketAddress.createUnresolved("foo", port));
  }

  private ServerSet.EndpointStatus join(ServerSet serverSet, String host)
      throws JoinException, InterruptedException {

    return serverSet.join(
        InetSocketAddress.createUnresolved(host, 42), ImmutableMap.<String, InetSocketAddress>of());
  }

  private void assertChangeFired(String... serviceHosts)
      throws InterruptedException {

    assertChangeFired(ImmutableSet.copyOf(Iterables.transform(ImmutableSet.copyOf(serviceHosts),
        serviceHost -> new ServiceInstance(new Endpoint(serviceHost, 42),
            ImmutableMap.<String, Endpoint>of(), Status.ALIVE))));
  }

  protected void assertChangeFiredEmpty() throws InterruptedException {
    assertChangeFired(ImmutableSet.<ServiceInstance>of());
  }

  protected void assertChangeFired(ServiceInstance... serviceInstances)
      throws InterruptedException {
    assertChangeFired(ImmutableSet.copyOf(serviceInstances));
  }

  protected void assertChangeFired(ImmutableSet<ServiceInstance> serviceInstances)
      throws InterruptedException {
    assertEquals(serviceInstances, serverSetBuffer.take());
  }
}

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
package com.twitter.common.zookeeper;

import java.net.InetSocketAddress;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.twitter.common.net.pool.DynamicHostSet.HostChangeMonitor;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.zookeeper.ServerSet.EndpointStatus;
import com.twitter.thrift.Endpoint;
import com.twitter.thrift.ServiceInstance;
import com.twitter.thrift.Status;

public class StaticServerSetTest extends EasyMockTest {

  private static final ServiceInstance BACKEND_1 = new ServiceInstance(
      new Endpoint("host_1", 12345),
      ImmutableMap.of("http", new Endpoint("host_1", 80)),
      Status.ALIVE);
  private static final ServiceInstance BACKEND_2 = new ServiceInstance(
      new Endpoint("host_2", 12346),
      ImmutableMap.of("http", new Endpoint("host_1", 80)),
      Status.ALIVE);

  private HostChangeMonitor<ServiceInstance> monitor;

  @Before
  public void setUp() {
    monitor = createMock(new Clazz<HostChangeMonitor<ServiceInstance>>() { });
  }

  @Test
  public void testMonitor() throws Exception {
    ImmutableSet<ServiceInstance> hosts = ImmutableSet.of(BACKEND_1, BACKEND_2);
    monitor.onChange(hosts);

    control.replay();

    ServerSet serverSet = new StaticServerSet(hosts);
    serverSet.monitor(monitor);
  }

  @Test
  public void testMonitorEmpty() throws Exception {
    ImmutableSet<ServiceInstance> hosts = ImmutableSet.of();
    monitor.onChange(hosts);

    control.replay();

    ServerSet serverSet = new StaticServerSet(hosts);
    serverSet.monitor(monitor);
  }

  @Test
  public void testJoin() throws Exception {
    // Ensure join/update calls don't break.
    ImmutableSet<ServiceInstance> hosts = ImmutableSet.of();

    control.replay();

    ServerSet serverSet = new StaticServerSet(hosts);
    EndpointStatus status = serverSet.join(
        InetSocketAddress.createUnresolved("host", 1000),
        ImmutableMap.<String, InetSocketAddress>of(),
        Status.ALIVE);
    status.update(Status.DEAD);
  }
}

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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.codec.ThriftBinaryCodec;
import org.apache.aurora.common.thrift.Endpoint;
import org.apache.aurora.common.thrift.ServiceInstance;
import org.apache.aurora.common.thrift.Status;
import org.apache.aurora.common.zookeeper.ServerSet;
import org.apache.aurora.common.zookeeper.testing.BaseZooKeeperTest;
import org.apache.aurora.scheduler.app.ServiceGroupMonitor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CuratorServiceGroupMonitorTest extends BaseZooKeeperTest {

  private static final String GROUP_PATH = "/group/root";
  private static final String MEMBER_TOKEN = "member_";

  private CuratorFramework client;
  private BlockingQueue<PathChildrenCacheEvent> groupEvents;
  private CuratorServiceGroupMonitor groupMonitor;

  @Before
  public void setUpCurator() {
    client = CuratorFrameworkFactory.builder()
        .dontUseContainerParents() // Container nodes are only available in ZK 3.5+.
        .retryPolicy((retryCount, elapsedTimeMs, sleeper) -> false) // Don't retry.
        .connectString(String.format("localhost:%d", getServer().getPort()))
        .build();
    client.start();
    addTearDown(client::close);

    PathChildrenCache groupCache =
        new PathChildrenCache(client, GROUP_PATH, true /* cacheData */);
    groupEvents = new LinkedBlockingQueue<>();
    groupCache.getListenable().addListener((c, event) -> groupEvents.put(event));

    Predicate<String> memberSelector = name -> name.contains(MEMBER_TOKEN);
    groupMonitor = new CuratorServiceGroupMonitor(groupCache, memberSelector, ServerSet.JSON_CODEC);
  }

  private void startGroupMonitor() throws ServiceGroupMonitor.MonitorException {
    groupMonitor.start();
    addTearDown(groupMonitor::close);
  }

  private void expectGroupEvent(PathChildrenCacheEvent.Type eventType) {
    while (true) {
      try {
        PathChildrenCacheEvent event = groupEvents.take();
        if (event.getType() == eventType) {
          break;
        }
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  @Test
  public void testNominalLifecycle() throws Exception {
    startGroupMonitor();
    groupMonitor.close();
  }

  @Test
  public void testExceptionalLifecycle() throws Exception {
    // Close on a non-started or failed-to-start monitor should be allowed.
    groupMonitor.close();
  }

  @Test
  public void testNoHosts() throws Exception {
    assertEquals(ImmutableSet.of(), groupMonitor.get());

    startGroupMonitor();
    assertEquals(ImmutableSet.of(), groupMonitor.get());
  }

  @Test
  public void testHostUpdates() throws Exception {
    startGroupMonitor();

    ServiceInstance one = serviceInstance("one");
    String onePath = createMember(one);
    ServiceInstance two = serviceInstance("two");
    String twoPath = createMember(two);
    assertEquals(ImmutableSet.of(one, two), groupMonitor.get());

    deleteChild(twoPath);
    assertEquals(ImmutableSet.of(one), groupMonitor.get());

    deleteChild(onePath);
    ServiceInstance three = serviceInstance("three");
    String threePath = createMember(three);
    assertEquals(ImmutableSet.of(three), groupMonitor.get());

    deleteChild(threePath);
    assertEquals(ImmutableSet.of(), groupMonitor.get());
  }

  @Test
  public void testMixedNodes() throws Exception {
    startGroupMonitor();

    String nonMemberPath = createNonMember();
    assertEquals(ImmutableSet.of(), groupMonitor.get());

    ServiceInstance member = serviceInstance("member");
    String memberPath = createMember(member);
    assertEquals(ImmutableSet.of(member), groupMonitor.get());

    deleteChild(memberPath);
    assertEquals(ImmutableSet.of(), groupMonitor.get());

    deleteChild(nonMemberPath);
    assertEquals(ImmutableSet.of(), groupMonitor.get());
  }

  @Test
  public void testInvalidMemberNode() throws Exception {
    startGroupMonitor();

    createMember(ThriftBinaryCodec.encode(serviceInstance("invalid")));

    ServiceInstance member = serviceInstance("member");
    createMember(member);

    // Invalid member should be ignored.
    assertEquals(ImmutableSet.of(member), groupMonitor.get());
  }

  @Test
  public void testStartBlocksOnInitialMembership() throws Exception {
    ServiceInstance one = serviceInstance("one");
    createMember(one, false /* waitForGroupEvent */);

    ServiceInstance two = serviceInstance("two");
    createMember(two, false /* waitForGroupEvent */);

    // Not started yet, should see no group members.
    assertEquals(ImmutableSet.of(), groupMonitor.get());

    startGroupMonitor();
    assertEquals(ImmutableSet.of(one, two), groupMonitor.get());
  }

  private void deleteChild(String twoPath) throws Exception {
    client.delete().forPath(twoPath);
    expectGroupEvent(PathChildrenCacheEvent.Type.CHILD_REMOVED);
  }

  private String createMember(ServiceInstance serviceInstance) throws Exception {
    return createMember(serviceInstance, true /* waitForGroupEvent */);
  }

  private String createMember(ServiceInstance serviceInstance, boolean waitForGroupEvent)
      throws Exception {

    return createMember(serialize(serviceInstance), waitForGroupEvent);
  }

  private String createMember(byte[] nodeData) throws Exception {
    return createMember(nodeData, true /* waitForGroupEvent */);
  }

  private String createMember(byte[] nodeData, boolean waitForGroupEvent) throws Exception {
    String path = client.create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
        .forPath(ZKPaths.makePath(GROUP_PATH, MEMBER_TOKEN), nodeData);
    if (waitForGroupEvent) {
      expectGroupEvent(PathChildrenCacheEvent.Type.CHILD_ADDED);
    }
    return path;
  }

  private String createNonMember() throws Exception {
    String path = client.create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
        .forPath(ZKPaths.makePath(GROUP_PATH, "not-a-member"));
    expectGroupEvent(PathChildrenCacheEvent.Type.CHILD_ADDED);
    return path;
  }

  private byte[] serialize(ServiceInstance serviceInstance) throws IOException {
    ByteArrayOutputStream sink = new ByteArrayOutputStream();
    ServerSet.JSON_CODEC.serialize(serviceInstance, sink);
    return sink.toByteArray();
  }

  private ServiceInstance serviceInstance(String hostName) {
    return new ServiceInstance(new Endpoint(hostName, 42), ImmutableMap.of(), Status.ALIVE);
  }
}

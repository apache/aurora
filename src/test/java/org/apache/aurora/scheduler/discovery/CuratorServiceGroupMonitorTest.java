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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;

import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CuratorServiceGroupMonitorTest extends BaseCuratorDiscoveryTest {

  @Test
  public void testNominalLifecycle() throws Exception {
    startGroupMonitor();
    getGroupMonitor().close();
  }

  @Test
  public void testNeverStarted() throws Exception {
    // Close on a non-started or failed-to-start monitor should be allowed.
    getGroupMonitor().close();
  }

  @Test
  public void testAlreadyStopped() throws Exception {
    startGroupMonitor();
    getGroupMonitor().close();
    // Multiple closes on a started monitor should be allowed.
    getGroupMonitor().close();
  }

  @Test
  public void testNoHosts() throws Exception {
    assertEquals(ImmutableSet.of(), getGroupMonitor().get());

    startGroupMonitor();
    assertEquals(ImmutableSet.of(), getGroupMonitor().get());
  }

  @Test
  public void testHostUpdates() throws Exception {
    startGroupMonitor();

    ServiceInstance one = serviceInstance("one");
    String onePath = createMember(one);
    ServiceInstance two = serviceInstance("two");
    String twoPath = createMember(two);
    assertEquals(ImmutableSet.of(one, two), getGroupMonitor().get());

    deleteChild(twoPath);
    assertEquals(ImmutableSet.of(one), getGroupMonitor().get());

    deleteChild(onePath);
    ServiceInstance three = serviceInstance("three");
    String threePath = createMember(three);
    assertEquals(ImmutableSet.of(three), getGroupMonitor().get());

    deleteChild(threePath);
    assertEquals(ImmutableSet.of(), getGroupMonitor().get());
  }

  @Test
  public void testMixedNodes() throws Exception {
    startGroupMonitor();

    String nonMemberPath = createNonMember();
    assertEquals(ImmutableSet.of(), getGroupMonitor().get());

    ServiceInstance member = serviceInstance("member");
    String memberPath = createMember(member);
    assertEquals(ImmutableSet.of(member), getGroupMonitor().get());

    deleteChild(memberPath);
    assertEquals(ImmutableSet.of(), getGroupMonitor().get());

    deleteChild(nonMemberPath);
    assertEquals(ImmutableSet.of(), getGroupMonitor().get());
  }

  @Test
  public void testInvalidMemberNode() throws Exception {
    startGroupMonitor();

    createMember("invalid".getBytes(Charsets.UTF_8));

    ServiceInstance member = serviceInstance("member");
    createMember(member);

    // Invalid member should be ignored.
    assertEquals(ImmutableSet.of(member), getGroupMonitor().get());
  }

  @Test
  public void testStartBlocksOnInitialMembership() throws Exception {
    ServiceInstance one = serviceInstance("one");
    createMember(one, false /* waitForGroupEvent */);

    ServiceInstance two = serviceInstance("two");
    createMember(two, false /* waitForGroupEvent */);

    // Not started yet, should see no group members.
    assertEquals(ImmutableSet.of(), getGroupMonitor().get());

    startGroupMonitor();
    assertEquals(ImmutableSet.of(one, two), getGroupMonitor().get());
  }

  private void deleteChild(String twoPath) throws Exception {
    getClient().delete().forPath(twoPath);
    expectGroupEvent(PathChildrenCacheEvent.Type.CHILD_REMOVED);
  }

  private String createMember(ServiceInstance serviceInstance) throws Exception {
    return createMember(serviceInstance, true /* waitForGroupEvent */);
  }

  private String createMember(ServiceInstance serviceInstance, boolean waitForGroupEvent)
      throws Exception {

    return createMember(Encoding.encode(serviceInstance), waitForGroupEvent);
  }

  private String createMember(byte[] nodeData) throws Exception {
    return createMember(nodeData, true /* waitForGroupEvent */);
  }

  private String createMember(byte[] nodeData, boolean waitForGroupEvent) throws Exception {
    String path = getClient().create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
        .forPath(ZKPaths.makePath(GROUP_PATH, MEMBER_TOKEN), nodeData);
    if (waitForGroupEvent) {
      expectGroupEvent(PathChildrenCacheEvent.Type.CHILD_ADDED);
    }
    return path;
  }

  private String createNonMember() throws Exception {
    String path = getClient().create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
        .forPath(ZKPaths.makePath(GROUP_PATH, "not-a-member"));
    expectGroupEvent(PathChildrenCacheEvent.Type.CHILD_ADDED);
    return path;
  }
}

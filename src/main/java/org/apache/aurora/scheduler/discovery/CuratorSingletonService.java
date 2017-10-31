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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Maps;
import com.google.common.io.Closer;

import org.apache.aurora.common.base.MorePreconditions;
import org.apache.aurora.common.zookeeper.SingletonService;
import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.discovery.ServiceInstance.Endpoint;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.CancelLeadershipException;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

class CuratorSingletonService implements SingletonService {

  private static final Logger LOG = LoggerFactory.getLogger(CuratorSingletonService.class);

  // This is the complement of the CuratorServiceGroupMonitor, it allows advertisement of a leader
  // in a service group.
  private static class Advertiser {

    private final String groupPath;
    private final String memberToken;
    private final CuratorFramework client;

    Advertiser(CuratorFramework client, String groupPath, String memberToken) {
      this.client = requireNonNull(client);
      this.groupPath = PathUtils.validatePath(groupPath);
      this.memberToken = MorePreconditions.checkNotBlank(memberToken);
    }

    void advertise(
        Closer closer,
        InetSocketAddress endpoint,
        Map<String, InetSocketAddress> additionalEndpoints)
        throws AdvertiseException, InterruptedException {

      byte[] nodeData = serializeAdvertisement(endpoint, additionalEndpoints);
      PersistentNode persistentNode =
          new PersistentNode(
              client,
              CreateMode.EPHEMERAL_SEQUENTIAL,

              // TODO(John Sirois): Enable GUID protection once clients are updated to support
              // its effects on group member node naming.  We get nodes like:
              //   4f5f98c4-8e71-41e3-8c8d-1c9a1f5f5df9-member_000000001
              // Clients expect member_ is the prefix and are not prepared for the GUID.
              false /* GUID protection */,

              ZKPaths.makePath(groupPath, memberToken),
              nodeData);
      persistentNode.start();
      closer.register(persistentNode);

      // NB: This blocks on initial server set node population to emulate legacy
      // SingletonService.LeaderControl.advertise (Group.join) behavior. Asynchronous
      // population is an option though, we simply need to remove this wait.
      if (!persistentNode.waitForInitialCreate(Long.MAX_VALUE, TimeUnit.DAYS)) {
        throw new AdvertiseException("Timed out waiting for leader advertisement.");
      }
    }

    private byte[] serializeAdvertisement(
        InetSocketAddress endpoint,
        Map<String, InetSocketAddress> additionalEndpoints)
        throws AdvertiseException {

      ServiceInstance serviceInstance =
          new ServiceInstance(
              asEndpoint(endpoint),
              Maps.transformValues(additionalEndpoints, Advertiser::asEndpoint));

      try {
        return Encoding.encode(serviceInstance);
      } catch (IOException e) {
        throw new AdvertiseException(
            "Problem serializing service instance data for " + serviceInstance, e);
      }
    }

    private static Endpoint asEndpoint(InetSocketAddress endpoint) {
      return new Endpoint(endpoint.getHostName(), endpoint.getPort());
    }
  }

  private final Advertiser advertiser;
  private final CuratorFramework client;
  private final String groupPath;

  /**
   * Creates a {@code SingletonService} backed by Curator.
   *
   * @param client A client to interact with a ZooKeeper ensemble.
   * @param groupPath The root ZooKeeper path service members advertise their presence under.
   * @param memberToken A token used to form service member node names.
   */
  CuratorSingletonService(CuratorFramework client, String groupPath, String memberToken) {
    advertiser = new Advertiser(client, groupPath, memberToken);
    this.client = client;
    this.groupPath = PathUtils.validatePath(groupPath);
  }

  @Override
  public synchronized void lead(
      InetSocketAddress endpoint,
      Map<String, InetSocketAddress> additionalEndpoints,
      LeadershipListener listener)
      throws LeadException, InterruptedException {

    requireNonNull(endpoint);
    requireNonNull(additionalEndpoints);
    requireNonNull(listener);

    Closer closer = Closer.create();

    CountDownLatch giveUpLeadership = new CountDownLatch(1);

    // We do not use the suggested `LeaderSelectorListenerAdapter` or the LeaderLatch class
    // because we want to have precise control over state changes. By default the listener and the
    // latch class treat `SUSPENDED` (connection loss) as fatal and a reason to lose leadership.
    // To make the scheduler resilient to connection blips and long GC pauses, we only treat
    // `LOST` (session loss) as fatal.

    ExecutorService executor =
        AsyncUtil.singleThreadLoggingScheduledExecutor("LeaderSelector-%d", LOG);

    LeaderSelectorListener leaderSelectorListener = new LeaderSelectorListener() {
      @Override
      public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
        listener.onLeading(new LeaderControl() {
          @Override
          public void advertise() throws AdvertiseException, InterruptedException {
            advertiser.advertise(closer, endpoint, additionalEndpoints);
          }

          @Override
          public void leave() throws LeaveException {
            try {
              giveUpLeadership.countDown();
              closer.close();
            } catch (IOException e) {
              throw new LeaveException("Failed to abdicate leadership of group at " + groupPath, e);
            }
          }
        });

        // The contract is to block as long as we want leadership. The leader never gives up
        // leadership voluntarily, only when asked to shutdown so we block until our shutdown
        // callback has been executed or we have lost our ZK connection.
        giveUpLeadership.await();
      }

      @Override
      public void stateChanged(CuratorFramework curatorFramework, ConnectionState newState) {
        if (newState == ConnectionState.LOST) {
          giveUpLeadership.countDown();
          listener.onDefeated();
          throw new CancelLeadershipException();
        }

      }
    };

    LeaderSelector leaderSelector =
        new LeaderSelector(client, groupPath, executor, leaderSelectorListener);

    leaderSelector.setId(endpoint.getHostName());

    try {
      leaderSelector.start();
    } catch (Exception e) {
      // NB: We failed to lead; so we never could have advertised and there is no need to close the
      // closer.
      throw new LeadException("Failed to begin awaiting leadership of group " + groupPath, e);
    }
    closer.register(leaderSelector);
  }
}

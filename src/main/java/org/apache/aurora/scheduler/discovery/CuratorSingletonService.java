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
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Maps;
import com.google.common.io.Closer;

import org.apache.aurora.common.base.MorePreconditions;
import org.apache.aurora.common.io.Codec;
import org.apache.aurora.common.thrift.Endpoint;
import org.apache.aurora.common.thrift.ServiceInstance;
import org.apache.aurora.common.thrift.Status;
import org.apache.aurora.common.zookeeper.SingletonService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;

import static java.util.Objects.requireNonNull;

class CuratorSingletonService implements SingletonService {

  // This is the complement of the CuratorServiceGroupMonitor, it allows advertisement of a leader
  // in a service group.
  private static class Advertiser {

    private final String groupPath;
    private final String memberToken;
    private final CuratorFramework client;
    private final Codec<ServiceInstance> codec;

    Advertiser(
        CuratorFramework client,
        String groupPath,
        String memberToken,
        Codec<ServiceInstance> codec) {

      this.client = requireNonNull(client);
      this.groupPath = PathUtils.validatePath(groupPath);
      this.memberToken = MorePreconditions.checkNotBlank(memberToken);
      this.codec = requireNonNull(codec);
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
              Maps.transformValues(additionalEndpoints, Advertiser::asEndpoint),
              Status.ALIVE);

      ByteArrayOutputStream sink = new ByteArrayOutputStream();
      try {
        codec.serialize(serviceInstance, sink);
      } catch (IOException e) {
        throw new AdvertiseException(
            "Problem serializing service instance data for " + serviceInstance, e);
      }
      return sink.toByteArray();
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
   * @param codec A codec that can be used to deserialize group member {@link ServiceInstance} data.
   */
  CuratorSingletonService(
      CuratorFramework client,
      String groupPath,
      String memberToken,
      Codec<ServiceInstance> codec) {
    advertiser = new Advertiser(client, groupPath, memberToken, codec);
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

    LeaderLatch leaderLatch = new LeaderLatch(client, groupPath, endpoint.getHostName());
    Closer closer = Closer.create();
    leaderLatch.addListener(new LeaderLatchListener() {
      @Override
      public void isLeader() {
        listener.onLeading(new LeaderControl() {
          @Override
          public void advertise() throws AdvertiseException, InterruptedException {
            advertiser.advertise(closer, endpoint, additionalEndpoints);
          }

          @Override
          public void leave() throws LeaveException {
            try {
              closer.close();
            } catch (IOException e) {
              throw new LeaveException("Failed to abdicate leadership of group at " + groupPath, e);
            }
          }
        });
      }

      @Override
      public void notLeader() {
        listener.onDefeated();
      }
    });

    try {
      leaderLatch.start();
    } catch (Exception e) {
      // NB: We failed to lead; so we never could have advertised and there is no need to close the
      // closer.
      throw new LeadException("Failed to begin awaiting leadership of group " + groupPath, e);
    }
    closer.register(leaderLatch);
  }
}

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
import java.util.List;

import javax.inject.Singleton;

import com.google.inject.Exposed;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;

import org.apache.aurora.common.net.pool.DynamicHostSet;
import org.apache.aurora.common.thrift.ServiceInstance;
import org.apache.aurora.common.zookeeper.ServerSetImpl;
import org.apache.aurora.common.zookeeper.SingletonService;
import org.apache.aurora.common.zookeeper.SingletonServiceImpl;
import org.apache.aurora.common.zookeeper.ZooKeeperClient;
import org.apache.aurora.common.zookeeper.ZooKeeperUtils;
import org.apache.aurora.scheduler.app.ServiceGroupMonitor;
import org.apache.zookeeper.data.ACL;

import static java.util.Objects.requireNonNull;

/**
 * Binding module for utilities to advertise the network presence of the scheduler.
 *
 * Uses a fork of Twitter commons/zookeeper.
 */
class CommonsServiceDiscoveryModule extends PrivateModule {

  private final String discoveryPath;
  private final ZooKeeperConfig zooKeeperConfig;

  CommonsServiceDiscoveryModule(String discoveryPath, ZooKeeperConfig zooKeeperConfig) {
    this.discoveryPath = ZooKeeperUtils.normalizePath(discoveryPath);
    this.zooKeeperConfig = requireNonNull(zooKeeperConfig);
  }

  @Override
  protected void configure() {
    requireBinding(ServiceDiscoveryBindings.ZOO_KEEPER_CLUSTER_KEY);
    requireBinding(ServiceDiscoveryBindings.ZOO_KEEPER_ACL_KEY);

    bind(ServiceGroupMonitor.class).to(CommonsServiceGroupMonitor.class).in(Singleton.class);
    expose(ServiceGroupMonitor.class);
  }

  @Provides
  @Singleton
  ZooKeeperClient provideZooKeeperClient(
      @ServiceDiscoveryBindings.ZooKeeper Iterable<InetSocketAddress> zooKeeperCluster) {

    return new ZooKeeperClient(
        zooKeeperConfig.getSessionTimeout(),
        zooKeeperConfig.getCredentials(),
        zooKeeperConfig.getChrootPath(),
        zooKeeperCluster);
  }

  @Provides
  @Singleton
  ServerSetImpl provideServerSet(
      ZooKeeperClient client,
      @ServiceDiscoveryBindings.ZooKeeper List<ACL> zooKeeperAcls) {

    return new ServerSetImpl(client, zooKeeperAcls, discoveryPath);
  }

  @Provides
  @Singleton
  DynamicHostSet<ServiceInstance> provideServerSet(ServerSetImpl serverSet) {
    // Used for a type re-binding of the server set.
    return serverSet;
  }

  // NB: We only take a ServerSetImpl instead of a ServerSet here to simplify binding.
  @Provides
  @Singleton
  @Exposed
  SingletonService provideSingletonService(
      ZooKeeperClient client,
      ServerSetImpl serverSet,
      @ServiceDiscoveryBindings.ZooKeeper List<ACL> zookeeperAcls) {

    return new SingletonServiceImpl(
        serverSet,
        SingletonServiceImpl.createSingletonCandidate(client, discoveryPath, zookeeperAcls));
  }
}

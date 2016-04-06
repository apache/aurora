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
package org.apache.aurora.scheduler.app;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;

import org.apache.aurora.common.base.Command;
import org.apache.aurora.common.net.pool.DynamicHostSet;
import org.apache.aurora.common.thrift.ServiceInstance;
import org.apache.aurora.common.zookeeper.ServerSetImpl;
import org.apache.aurora.common.zookeeper.SingletonService;
import org.apache.aurora.common.zookeeper.SingletonServiceImpl;
import org.apache.aurora.common.zookeeper.ZooKeeperClient;
import org.apache.aurora.common.zookeeper.ZooKeeperClient.Credentials;
import org.apache.aurora.common.zookeeper.ZooKeeperUtils;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * Binding module for utilities to advertise the network presence of the scheduler.
 */
class ServiceDiscoveryModule extends AbstractModule {

  private static class ServerSetMonitor implements ServiceGroupMonitor {
    private Optional<Command> closeCommand = Optional.empty();
    private final DynamicHostSet<ServiceInstance> serverSet;
    private final AtomicReference<ImmutableSet<ServiceInstance>> services =
        new AtomicReference<>(ImmutableSet.of());

    // NB: We only take a ServerSetImpl instead of a DynamicHostSet<ServiceInstance> here to
    // simplify binding.
    @Inject
    ServerSetMonitor(ServerSetImpl serverSet) {
      this.serverSet = requireNonNull(serverSet);
    }

    @Override
    public void start() throws MonitorException {
      try {
        closeCommand = Optional.of(serverSet.watch(services::set));
      } catch (DynamicHostSet.MonitorException e) {
        throw new MonitorException("Unable to watch scheduler host set.", e);
      }
    }

    @Override
    public void close() {
      closeCommand.ifPresent(Command::execute);
    }

    @Override
    public ImmutableSet<ServiceInstance> get() {
      return services.get();
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(ServiceDiscoveryModule.class);

  private final String serverSetPath;
  private final Credentials zkCredentials;

  ServiceDiscoveryModule(String serverSetPath, Credentials zkCredentials) {
    this.serverSetPath = requireNonNull(serverSetPath);
    this.zkCredentials = requireNonNull(zkCredentials);
  }

  @Override
  protected void configure() {
    bind(ServiceGroupMonitor.class).to(ServerSetMonitor.class).in(Singleton.class);
  }

  @Provides
  @Singleton
  List<ACL> provideAcls() {
    if (zkCredentials == Credentials.NONE) {
      LOG.warn("Running without ZooKeeper digest credentials. ZooKeeper ACLs are disabled.");
      return ZooKeeperUtils.OPEN_ACL_UNSAFE;
    } else {
      return ZooKeeperUtils.EVERYONE_READ_CREATOR_ALL;
    }
  }

  @Provides
  @Singleton
  ServerSetImpl provideServerSet(ZooKeeperClient client, List<ACL> zooKeeperAcls) {
    return new ServerSetImpl(client, zooKeeperAcls, serverSetPath);
  }

  // NB: We only take a ServerSetImpl instead of a ServerSet here to simplify binding.
  @Provides
  @Singleton
  SingletonService provideSingletonService(
      ZooKeeperClient client,
      ServerSetImpl serverSet,
      List<ACL> zookeeperAcls) {

    return new SingletonServiceImpl(
        serverSet,
        SingletonServiceImpl.createSingletonCandidate(client, serverSetPath, zookeeperAcls));
  }
}

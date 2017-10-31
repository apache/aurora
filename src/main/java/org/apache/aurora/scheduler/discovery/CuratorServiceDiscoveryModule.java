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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import javax.inject.Singleton;

import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.collect.FluentIterable;
import com.google.inject.Exposed;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;

import org.apache.aurora.common.application.ShutdownRegistry;
import org.apache.aurora.common.base.MorePreconditions;
import org.apache.aurora.common.net.InetSocketAddressHelper;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.zookeeper.Credentials;
import org.apache.aurora.common.zookeeper.SingletonService;
import org.apache.aurora.scheduler.app.ServiceGroupMonitor;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.utils.PathUtils;
import org.apache.zookeeper.data.ACL;

import static java.util.Objects.requireNonNull;

/**
 * Binding module for utilities to advertise the network presence of the scheduler.
 *
 * Uses Apache Curator.
 */
class CuratorServiceDiscoveryModule extends PrivateModule {

  private final String discoveryPath;
  private final ZooKeeperConfig zooKeeperConfig;

  private ConnectionState currentState;

  CuratorServiceDiscoveryModule(String discoveryPath, ZooKeeperConfig zooKeeperConfig) {
    this.discoveryPath = PathUtils.validatePath(discoveryPath);
    this.zooKeeperConfig = requireNonNull(zooKeeperConfig);
  }

  @Override
  protected void configure() {
    requireBinding(ServiceDiscoveryBindings.ZOO_KEEPER_CLUSTER_KEY);
    requireBinding(ServiceDiscoveryBindings.ZOO_KEEPER_ACL_KEY);
  }

  @Provides
  @Singleton
  CuratorFramework provideCuratorFramework(
      ShutdownRegistry shutdownRegistry,
      @ServiceDiscoveryBindings.ZooKeeper Iterable<InetSocketAddress> zooKeeperCluster,
      ACLProvider aclProvider,
      StatsProvider statsProvider) {

    String connectString =
        FluentIterable.from(zooKeeperCluster)
            .transform(InetSocketAddressHelper::toString)
            .join(Joiner.on(','));

    if (zooKeeperConfig.getChrootPath().isPresent()) {
      connectString = connectString + zooKeeperConfig.getChrootPath().get();
    }

    // export current connection state
    for (ConnectionState connectionState : ConnectionState.values()) {
      statsProvider.makeGauge(
          zkConnectionGaugeName(connectionState),
          new Supplier<Integer>() {
            @Override
            public Integer get() {
              return connectionState.equals(currentState) ? 1 : 0;
            }
          }
      );
    }

    // connection state counter
    AtomicLong zkConnectionConnectedCounter =
        statsProvider.makeCounter(zkConnectionStateCounterName(ConnectionState.CONNECTED));
    AtomicLong zkConnectionReadonlyCounter =
        statsProvider.makeCounter(zkConnectionStateCounterName(ConnectionState.READ_ONLY));
    AtomicLong zkConnectionSuspendedCounter =
        statsProvider.makeCounter(zkConnectionStateCounterName(ConnectionState.SUSPENDED));
    AtomicLong zkConnectionReconnectedCounter =
        statsProvider.makeCounter(zkConnectionStateCounterName(ConnectionState.RECONNECTED));
    AtomicLong zkConnectionLostCounter =
        statsProvider.makeCounter(zkConnectionStateCounterName(ConnectionState.LOST));

    // This emulates the default BackoffHelper configuration used by the legacy commons/zookeeper
    // stack. BackoffHelper is unbounded, this dies after around 5 minutes using the 10 retries.
    // NB: BoundedExponentialBackoffRetry caps max retries at 29 if you send it a larger value.
    RetryPolicy retryPolicy =
        new BoundedExponentialBackoffRetry(
            Amount.of(1, Time.SECONDS).as(Time.MILLISECONDS),
            Amount.of(1, Time.MINUTES).as(Time.MILLISECONDS),
            10);

    CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
        .dontUseContainerParents() // Container nodes are only available in ZK 3.5+.
        .connectString(connectString)
        .canBeReadOnly(false) // We must be able to write to perform leader election.
        .sessionTimeoutMs(zooKeeperConfig.getSessionTimeout().as(Time.MILLISECONDS))
        .connectionTimeoutMs(zooKeeperConfig.getConnectionTimeout().as(Time.MILLISECONDS))
        .retryPolicy(retryPolicy)
        .aclProvider(aclProvider);

    if (zooKeeperConfig.getCredentials().isPresent()) {
      Credentials credentials = zooKeeperConfig.getCredentials().get();
      builder.authorization(credentials.scheme(), credentials.authToken());
    }

    CuratorFramework curatorFramework = builder.build();
    Listenable<ConnectionStateListener> connectionStateListener = curatorFramework
        .getConnectionStateListenable();
    connectionStateListener.addListener((CuratorFramework client, ConnectionState newState) -> {
      currentState = newState;
      switch (newState) {
        case CONNECTED:
          zkConnectionConnectedCounter.getAndIncrement();
          break;
        case READ_ONLY:
          zkConnectionReadonlyCounter.getAndIncrement();
          break;
        case SUSPENDED:
          zkConnectionSuspendedCounter.getAndIncrement();
          break;
        case RECONNECTED:
          zkConnectionReconnectedCounter.getAndIncrement();
          break;
        case LOST:
          zkConnectionLostCounter.getAndIncrement();
          break;
        default:
          currentState = null;
          break;
      }
    });

    // TODO(John Sirois): It would be nice to use a Service to control the lifecycle here, but other
    // services (org.apache.aurora.scheduler.http.JettyServerModule.RedirectMonitor) rely on this
    // service being started 1st which is not deterministic as things stand.  Find a way to leverage
    // the Service system for services with Service dependencies.
    curatorFramework.start();
    shutdownRegistry.addAction(curatorFramework::close);

    return curatorFramework;
  }

  static class SingleACLProvider implements ACLProvider {
    private final List<ACL> acl;

    SingleACLProvider(List<ACL> acl) {
      this.acl = MorePreconditions.checkNotBlank(acl);
    }

    @Override
    public List<ACL> getDefaultAcl() {
      return acl;
    }

    @Override
    public List<ACL> getAclForPath(String path) {
      return acl;
    }
  }

  @Provides
  @Singleton
  ACLProvider provideACLProvider(@ServiceDiscoveryBindings.ZooKeeper List<ACL> acl) {
    return new SingleACLProvider(acl);
  }

  // These values are compatible with the Java and Python common/zookeeper service discovery
  // protocol. If GUID protection is enabled for Curator, the MEMBER_SELECTOR will need to be
  // modified to handle GUID prefixes of MEMBER_TOKEN.
  private static final String MEMBER_TOKEN = "member_";
  private static final Predicate<String> MEMBER_SELECTOR = name -> name.startsWith(MEMBER_TOKEN);

  @Provides
  @Singleton
  @Exposed
  ServiceGroupMonitor provideServiceGroupMonitor(
      ShutdownRegistry shutdownRegistry,
      CuratorFramework client) {

    PathChildrenCache groupCache =
        new PathChildrenCache(client, discoveryPath, true /* cacheData */);

    // NB: Even though we do not start the serviceGroupMonitor here, the registered close shutdown
    // action is safe since the underlying PathChildrenCache close is tolerant of an un-started
    // state. Its also crucial so that its underlying groupCache is closed prior to its
    // curatorFramework dependency in the case when the PathChildrenCache is in fact started (via
    // CuratorServiceGroupMonitor::start) since a CuratorFramework should have no active clients
    // when it is closed to avoid errors in those clients when attempting to use it.

    ServiceGroupMonitor serviceGroupMonitor =
        new CuratorServiceGroupMonitor(groupCache, MEMBER_SELECTOR);
    shutdownRegistry.addAction(groupCache::close);

    return serviceGroupMonitor;
  }

  @Provides
  @Singleton
  @Exposed
  SingletonService provideSingletonService(CuratorFramework client) {
    return new CuratorSingletonService(client, discoveryPath, MEMBER_TOKEN);
  }

  private String zkConnectionStateCounterName(ConnectionState state) {
    return zkConnectionGaugeName(state) + "_counter";
  }

  private String zkConnectionGaugeName(ConnectionState state) {
    return "zk_connection_state_" + state;
  }
}

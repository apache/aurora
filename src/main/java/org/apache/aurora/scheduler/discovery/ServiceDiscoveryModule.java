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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import javax.inject.Singleton;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.binder.LinkedBindingBuilder;

import org.apache.aurora.common.application.ShutdownRegistry;
import org.apache.aurora.common.base.MorePreconditions;
import org.apache.aurora.common.zookeeper.ZooKeeperUtils;
import org.apache.aurora.common.zookeeper.testing.ZooKeeperTestServer;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * Creates a Guice module that binds leader election and leader discovery components.
 */
public class ServiceDiscoveryModule extends AbstractModule {

  private static final Logger LOG = LoggerFactory.getLogger(CommonsServiceDiscoveryModule.class);

  private final ZooKeeperConfig zooKeeperConfig;
  private final String discoveryPath;

  /**
   * Creates a Guice module that will bind a
   * {@link org.apache.aurora.common.zookeeper.SingletonService} for scheduler leader election and a
   * {@link org.apache.aurora.scheduler.app.ServiceGroupMonitor} that can be used to find the
   * leading scheduler.
   *
   * @param zooKeeperConfig The ZooKeeper client configuration to use to interact with ZooKeeper.
   * @param discoveryPath The ZooKeeper path to use to host leader election and service discovery
   *                      nodes under.
   */
  public ServiceDiscoveryModule(ZooKeeperConfig zooKeeperConfig, String discoveryPath) {
    this.zooKeeperConfig = requireNonNull(zooKeeperConfig);
    this.discoveryPath = MorePreconditions.checkNotBlank(discoveryPath);
  }

  @Override
  protected void configure() {
    LinkedBindingBuilder<Iterable<InetSocketAddress>> clusterBinder =
        bind(ServiceDiscoveryBindings.ZOO_KEEPER_CLUSTER_KEY);

    if (zooKeeperConfig.isInProcess()) {
      requireBinding(ShutdownRegistry.class);
      File tempDir = Files.createTempDir();
      bind(ZooKeeperTestServer.class).toInstance(new ZooKeeperTestServer(tempDir, tempDir));
      SchedulerServicesModule.addAppStartupServiceBinding(binder()).to(TestServerService.class);

      clusterBinder.toProvider(LocalZooKeeperClusterProvider.class);
    } else {
      clusterBinder.toInstance(zooKeeperConfig.getServers());
    }

    install(discoveryModule());
  }

  private Module discoveryModule() {
    if (zooKeeperConfig.isUseCurator()) {
      return new CuratorServiceDiscoveryModule(discoveryPath, zooKeeperConfig);
    } else {
      return new CommonsServiceDiscoveryModule(discoveryPath, zooKeeperConfig);
    }
  }

  @Provides
  @Singleton
  @ServiceDiscoveryBindings.ZooKeeper
  List<ACL> provideAcls() {
    if (zooKeeperConfig.getCredentials().isPresent()) {
      return ZooKeeperUtils.EVERYONE_READ_CREATOR_ALL;
    } else {
      LOG.warn("Running without ZooKeeper digest credentials. ZooKeeper ACLs are disabled.");
      return ZooKeeperUtils.OPEN_ACL_UNSAFE;
    }
  }

  /**
   * A service to wrap ZooKeeperTestServer.  ZooKeeperTestServer is not a service itself because
   * some tests depend on stop/start routines that do not no-op, like startAsync and stopAsync may.
   */
  private static class TestServerService extends AbstractIdleService {
    private final ZooKeeperTestServer testServer;

    @Inject
    TestServerService(ZooKeeperTestServer testServer) {
      this.testServer = requireNonNull(testServer);
    }

    @Override
    protected void startUp() {
      // We actually start the test server on-demand rather than with the normal lifecycle.
      // This is because a ZooKeeperClient binding is needed before scheduler services are started.
    }

    @Override
    protected void shutDown() {
      testServer.stop();
    }
  }

  private static class LocalZooKeeperClusterProvider
      implements Provider<Iterable<InetSocketAddress>> {

    private final ZooKeeperTestServer testServer;

    @Inject
    LocalZooKeeperClusterProvider(ZooKeeperTestServer testServer) {
      this.testServer = requireNonNull(testServer);
    }

    @Override
    public Iterable<InetSocketAddress> get() {
      try {
        testServer.startNetwork();
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException(e);
      }
      return ImmutableList.of(
          InetSocketAddress.createUnresolved("localhost", testServer.getPort()));
    }
  }
}

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
package org.apache.aurora.common.zookeeper.guice.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Singleton;

import org.apache.aurora.common.application.ShutdownRegistry;
import org.apache.aurora.common.inject.Bindings.KeyFactory;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.zookeeper.ZooKeeperClient;
import org.apache.aurora.common.zookeeper.ZooKeeperClient.Credentials;
import org.apache.aurora.common.zookeeper.ZooKeeperUtils;
import org.apache.aurora.common.zookeeper.testing.ZooKeeperTestServer;

/**
 * A guice binding module that configures and binds a {@link ZooKeeperClient} instance.
 */
public class ZooKeeperClientModule extends PrivateModule {
  private final KeyFactory keyFactory;
  private final ClientConfig config;

  /**
   * Creates a new ZK client module from the provided configuration.
   *
   * @param config Configuration parameters for the client.
   */
  public ZooKeeperClientModule(ClientConfig config) {
    this(KeyFactory.PLAIN, config);
  }

  /**
   * Creates a new ZK client module from the provided configuration, using a key factory to
   * qualify any bindings.
   *
   * @param keyFactory Factory to use when creating any exposed bindings.
   * @param config Configuration parameters for the client.
   */
  public ZooKeeperClientModule(KeyFactory keyFactory, ClientConfig config) {
    this.keyFactory = Preconditions.checkNotNull(keyFactory);
    this.config = Preconditions.checkNotNull(config);
  }

  @Override
  protected void configure() {
    Key<ZooKeeperClient> clientKey = keyFactory.create(ZooKeeperClient.class);
    if (config.inProcess) {
      requireBinding(ShutdownRegistry.class);
      // Bound privately to give the local provider access to configuration settings.
      bind(ClientConfig.class).toInstance(config);
      bind(clientKey).toProvider(LocalClientProvider.class).in(Singleton.class);
    } else {
      ZooKeeperClient client =
          new ZooKeeperClient(config.sessionTimeout, config.credentials, config.chrootPath, config.servers);
      bind(clientKey).toInstance(client);
    }
    expose(clientKey);
  }

  private static class LocalClientProvider implements Provider<ZooKeeperClient> {
    private static final Logger LOG = Logger.getLogger(LocalClientProvider.class.getName());

    private final ClientConfig config;
    private final ShutdownRegistry shutdownRegistry;

    @Inject
    LocalClientProvider(ClientConfig config, ShutdownRegistry shutdownRegistry) {
      this.config = Preconditions.checkNotNull(config);
      this.shutdownRegistry = Preconditions.checkNotNull(shutdownRegistry);
    }

    @Override
    public ZooKeeperClient get() {
      ZooKeeperTestServer zooKeeperServer;
      try {
        zooKeeperServer = new ZooKeeperTestServer(0, shutdownRegistry);
        zooKeeperServer.startNetwork();
      } catch (IOException e) {
        throw Throwables.propagate(e);
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }

      LOG.info("Embedded zookeeper cluster started on port " + zooKeeperServer.getPort());
      return zooKeeperServer.createClient(config.sessionTimeout, config.credentials);
    }
  }

  /**
   * Composite type that contains configuration parameters used when creating a client.
   * <p>
   * Instances of this class are immutable, but builder-style chained calls are supported.
   */
  public static class ClientConfig {
    public final Iterable<InetSocketAddress> servers;
    public final boolean inProcess;
    public final Amount<Integer, Time> sessionTimeout;
    public final Optional<String> chrootPath;
    public final Credentials credentials;

    /**
     * Creates a new client configuration.
     *
     * @param servers ZooKeeper server addresses.
     * @param inProcess Whether to run and create clients for an in-process ZooKeeper server.
     * @param chrootPath an optional chroot path
     * @param sessionTimeout Timeout duration for established sessions.
     * @param credentials ZooKeeper authentication credentials.
     */
    public ClientConfig(
        Iterable<InetSocketAddress> servers,
        Optional<String> chrootPath,
        boolean inProcess,
        Amount<Integer, Time> sessionTimeout,
        Credentials credentials) {

      this.servers = servers;
      this.chrootPath = chrootPath;
      this.inProcess = inProcess;
      this.sessionTimeout = sessionTimeout;
      this.credentials = credentials;
    }

    /**
     * Creates a new client configuration with defaults for the session timeout and credentials.
     *
     * @param servers ZooKeeper server addresses.
     * @return A new configuration.
     */
    public static ClientConfig create(Iterable<InetSocketAddress> servers) {
      return new ClientConfig(
          servers,
          Optional.<String> absent(),
          false,
          ZooKeeperUtils.DEFAULT_ZK_SESSION_TIMEOUT,
          Credentials.NONE);
    }

    /**
     * Creates a new configuration identical to this configuration, but with the provided
     * credentials.
     *
     * @param credentials ZooKeeper authentication credentials.
     * @return A modified clone of this configuration.
     */
    public ClientConfig withCredentials(Credentials credentials) {
      return new ClientConfig(servers, chrootPath, inProcess, sessionTimeout, credentials);
    }
  }
}

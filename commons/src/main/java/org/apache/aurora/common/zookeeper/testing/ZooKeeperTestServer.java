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
package org.apache.aurora.common.zookeeper.testing;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.LinkedList;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperServer.BasicDataTreeBuilder;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

/**
 * A helper class for starting in-process ZooKeeper server and clients.
 *
 * <p>This is ONLY meant to be used for testing.
 */
public class ZooKeeperTestServer {

  public static final Amount<Integer, Time> DEFAULT_SESSION_TIMEOUT =
      Amount.of(100, Time.MILLISECONDS);

  protected final ZooKeeperServer zooKeeperServer;
  private NIOServerCnxn.Factory connectionFactory;
  private int port;
  private final Amount<Integer, Time> defaultSessionTimeout;
  private final LinkedList<Runnable> cleanupActions = Lists.newLinkedList();

  /**
   * @param defaultSessionTimeout the default session timeout for clients created with
   *     {@link #createClient()}.
   * @throws IOException if there was aproblem creating the server's database
   */
  public ZooKeeperTestServer(
      Amount<Integer, Time> defaultSessionTimeout,
      File dataDir,
      File snapDir) throws IOException {

    this.defaultSessionTimeout = Preconditions.checkNotNull(defaultSessionTimeout);

    zooKeeperServer =
        new ZooKeeperServer(
            new FileTxnSnapLog(dataDir, snapDir),
            new BasicDataTreeBuilder()) {

          // TODO(John Sirois): Introduce a builder to configure the in-process server if and when
          // some folks need JMX for in-process tests.
          @Override protected void registerJMX() {
            // noop
          }
        };
  }

  /**
   * Starts zookeeper up on an ephemeral port.
   */
  public void startNetwork() throws IOException, InterruptedException {
    connectionFactory = new NIOServerCnxn.Factory(new InetSocketAddress(port));
    connectionFactory.startup(zooKeeperServer);
    cleanupActions.addFirst((this::shutdownNetwork));
    port = zooKeeperServer.getClientPort();
  }

  /**
   * Stops the zookeeper server.
   */
  public void stop() {
    for (Runnable cleanup : cleanupActions) {
      cleanup.run();
    }
    cleanupActions.clear();
  }

  /**
   * Starts zookeeper back up on the last used port.
   */
  public final void restartNetwork() throws IOException, InterruptedException {
    checkEphemeralPortAssigned();
    Preconditions.checkState(!connectionFactory.isAlive());
    startNetwork();
  }

  /**
   * Shuts down the in-process zookeeper network server.
   */
  public final void shutdownNetwork() {
    if (connectionFactory != null && connectionFactory.isAlive()) {
      connectionFactory.shutdown();
    }
  }

  /**
   * Expires the active session for the given client.  The client should be one returned from
   * {@link #createClient}.
   *
   * @param zkClient the client to expire
   * @throws ZooKeeperClient.ZooKeeperConnectionException if a problem is encountered connecting to
   *    the local zk server while trying to expire the session
   * @throws InterruptedException if interrupted while requesting expiration
   */
  public final void expireClientSession(ZooKeeperClient zkClient)
      throws ZooKeeperClient.ZooKeeperConnectionException, InterruptedException {
    zooKeeperServer.closeSession(zkClient.get().getSessionId());
  }

  /**
   * Returns the current port to connect to the in-process zookeeper instance.
   */
  public final int getPort() {
    checkEphemeralPortAssigned();
    return port;
  }

  /**
   * Returns a new unauthenticated zookeeper client connected to the in-process zookeeper server
   * with the default session timeout.
   */
  public final ZooKeeperClient createClient() {
    return createClient(defaultSessionTimeout);
  }

  /**
   * Returns a new unauthenticated zookeeper client connected to the in-process zookeeper server
   * with the default session timeout and a custom {@code chrootPath}.
   */
  public final ZooKeeperClient createClient(String chrootPath) {
    return createClient(defaultSessionTimeout, ZooKeeperClient.Credentials.NONE, Optional.of(chrootPath));
  }

  /**
   * Returns a new authenticated zookeeper client connected to the in-process zookeeper server with
   * the default session timeout.
   */
  public final ZooKeeperClient createClient(ZooKeeperClient.Credentials credentials) {
    return createClient(defaultSessionTimeout, credentials, Optional.<String>absent());
  }

  /**
   * Returns a new unauthenticated zookeeper client connected to the in-process zookeeper server
   * with a custom {@code sessionTimeout}.
   */
  public final ZooKeeperClient createClient(Amount<Integer, Time> sessionTimeout) {
    return createClient(sessionTimeout, ZooKeeperClient.Credentials.NONE, Optional.<String>absent());
  }

  /**
   * Returns a new authenticated zookeeper client connected to the in-process zookeeper server with
   * a custom {@code sessionTimeout}.
   */
  public final ZooKeeperClient createClient(Amount<Integer, Time> sessionTimeout,
      ZooKeeperClient.Credentials credentials) {
        return createClient(sessionTimeout, credentials, Optional.<String>absent());
      }

  /**
   * Returns a new authenticated zookeeper client connected to the in-process zookeeper server with
   * a custom {@code sessionTimeout} and a custom {@code chrootPath}.
   */
  public final ZooKeeperClient createClient(Amount<Integer, Time> sessionTimeout,
      ZooKeeperClient.Credentials credentials, Optional<String> chrootPath) {

    final ZooKeeperClient client = new ZooKeeperClient(sessionTimeout, credentials,
        chrootPath, Arrays.asList(InetSocketAddress.createUnresolved("127.0.0.1", port)));
    cleanupActions.addFirst(client::close);
    return client;
  }

  private void checkEphemeralPortAssigned() {
    Preconditions.checkState(port > 0, "startNetwork must be called first");
  }
}

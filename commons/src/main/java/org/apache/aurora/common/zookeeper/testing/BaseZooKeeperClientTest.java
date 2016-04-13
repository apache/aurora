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

import java.io.IOException;
import java.net.InetSocketAddress;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.zookeeper.Credentials;
import org.apache.aurora.common.zookeeper.ZooKeeperClient;

/**
 * A base-class for tests that interact with ZooKeeper via the commons ZooKeeperClient.
 */
public abstract class BaseZooKeeperClientTest extends BaseZooKeeperTest {

  private final Amount<Integer, Time> defaultSessionTimeout;

  /**
   * Creates a test case where the test server uses its
   * {@link ZooKeeperTestServer#DEFAULT_SESSION_TIMEOUT} for clients created without an explicit
   * session timeout.
   */
  public BaseZooKeeperClientTest() {
    this(ZooKeeperTestServer.DEFAULT_SESSION_TIMEOUT);
  }

  /**
   * Creates a test case where the test server uses the given {@code defaultSessionTimeout} for
   * clients created without an explicit session timeout.
   */
  public BaseZooKeeperClientTest(Amount<Integer, Time> defaultSessionTimeout) {
    this.defaultSessionTimeout = Preconditions.checkNotNull(defaultSessionTimeout);
  }


  /**
   * Starts zookeeper back up on the last used port.
   */
  protected final void restartNetwork() throws IOException, InterruptedException {
    getServer().restartNetwork();
  }

  /**
   * Shuts down the in-process zookeeper network server.
   */
  protected final void shutdownNetwork() {
    getServer().shutdownNetwork();
  }

  /**
   * Expires the active session for the given client.  The client should be one returned from
   * {@link #createZkClient}.
   *
   * @param zkClient the client to expire
   * @throws ZooKeeperClient.ZooKeeperConnectionException if a problem is encountered connecting to
   *    the local zk server while trying to expire the session
   * @throws InterruptedException if interrupted while requesting expiration
   */
  protected final void expireSession(ZooKeeperClient zkClient)
      throws ZooKeeperClient.ZooKeeperConnectionException, InterruptedException {
    getServer().expireClientSession(zkClient.get().getSessionId());
  }

  /**
   * Returns the current port to connect to the in-process zookeeper instance.
   */
  protected final int getPort() {
    return getServer().getPort();
  }

  /**
   * Returns a new unauthenticated zookeeper client connected to the in-process zookeeper server
   * with the default session timeout.
   */
  protected final ZooKeeperClient createZkClient() {
    return createZkClient(defaultSessionTimeout, Optional.absent(), Optional.absent());
  }

  /**
   * Returns a new authenticated zookeeper client connected to the in-process zookeeper server with
   * the default session timeout.
   */
  protected final ZooKeeperClient createZkClient(Credentials credentials) {
    return createZkClient(defaultSessionTimeout, Optional.of(credentials), Optional.absent());
  }

  /**
   * Returns a new authenticated zookeeper client connected to the in-process zookeeper server with
   * the default session timeout.  The client is authenticated in the digest authentication scheme
   * with the given {@code username} and {@code password}.
   */
  protected final ZooKeeperClient createZkClient(String username, String password) {
    return createZkClient(Credentials.digestCredentials(username, password));
  }

  /**
   * Returns a new unauthenticated zookeeper client connected to the in-process zookeeper server
   * with a custom {@code sessionTimeout}.
   */
  protected final ZooKeeperClient createZkClient(Amount<Integer, Time> sessionTimeout) {
    return createZkClient(sessionTimeout, Optional.absent(), Optional.absent());
  }

  /**
   * Returns a new authenticated zookeeper client connected to the in-process zookeeper server with
   * the default session timeout and the custom chroot path.
   */
  protected final ZooKeeperClient createZkClient(String chrootPath) {
    return createZkClient(defaultSessionTimeout, Optional.absent(),
        Optional.of(chrootPath));
  }

  private ZooKeeperClient createZkClient(
      Amount<Integer, Time> sessionTimeout,
      Optional<Credentials> credentials,
      Optional<String> chrootPath) {

    ZooKeeperClient client = new ZooKeeperClient(sessionTimeout, credentials, chrootPath,
        ImmutableList.of(InetSocketAddress.createUnresolved("127.0.0.1", getPort())));
    addTearDown(client::close);
    return client;
  }
}

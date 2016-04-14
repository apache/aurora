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

import com.google.common.base.Preconditions;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperServer.BasicDataTreeBuilder;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

/**
 * A helper class for starting in-process ZooKeeper server and clients.
 *
 * <p>This is ONLY meant to be used for testing.
 */
public class ZooKeeperTestServer {

  static final Amount<Integer, Time> DEFAULT_SESSION_TIMEOUT = Amount.of(100, Time.MILLISECONDS);

  private final File dataDir;
  private final File snapDir;

  private ZooKeeperServer zooKeeperServer;
  private ServerCnxnFactory connectionFactory;
  private int port;

  public ZooKeeperTestServer(File dataDir, File snapDir) {
    this.dataDir = Preconditions.checkNotNull(dataDir);
    this.snapDir = Preconditions.checkNotNull(snapDir);
  }

  /**
   * Starts zookeeper up on an ephemeral port.
   */
  public void startNetwork() throws IOException, InterruptedException {
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

    connectionFactory = new NIOServerCnxnFactory();
    connectionFactory.configure(
        new InetSocketAddress(port),
        60 /* Semi-arbitrary, max 60 connections is the default used by NIOServerCnxnFactory */);
    connectionFactory.startup(zooKeeperServer);
    port = zooKeeperServer.getClientPort();
  }

  /**
   * Stops the zookeeper server.
   */
  public void stop() {
    shutdownNetwork();
  }

  /**
   * Starts zookeeper back up on the last used port.
   */
  final void restartNetwork() throws IOException, InterruptedException {
    checkEphemeralPortAssigned();
    Preconditions.checkState(connectionFactory == null);
    startNetwork();
  }

  /**
   * Shuts down the in-process zookeeper network server.
   */
  final void shutdownNetwork() {
    if (connectionFactory != null) {
      connectionFactory.shutdown(); // Also shuts down zooKeeperServer.
      connectionFactory = null;
    }
  }

  /**
   * Expires the client session with the given {@code sessionId}.
   *
   * @param sessionId The id of the client session to expire.
   */
  public final void expireClientSession(long sessionId) {
    zooKeeperServer.closeSession(sessionId);
  }

  /**
   * Returns the current port to connect to the in-process zookeeper instance.
   */
  public final int getPort() {
    checkEphemeralPortAssigned();
    return port;
  }

  private void checkEphemeralPortAssigned() {
    Preconditions.checkState(port > 0, "startNetwork must be called first");
  }
}

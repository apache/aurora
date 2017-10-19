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

import com.google.common.base.Optional;

import org.apache.aurora.common.base.MorePreconditions;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.zookeeper.Credentials;
import org.apache.aurora.common.zookeeper.ZooKeeperUtils;
import org.apache.aurora.scheduler.config.types.TimeAmount;

import static java.util.Objects.requireNonNull;

/**
 * Composite type that contains configuration parameters used when creating a ZooKeeper client.
 * <p>
 * Instances of this class are immutable, but builder-style chained calls are supported.
 */
public class ZooKeeperConfig {

  public static final TimeAmount DEFAULT_SESSION_TIMEOUT = new TimeAmount(
      ZooKeeperUtils.DEFAULT_ZK_SESSION_TIMEOUT.getValue(),
        ZooKeeperUtils.DEFAULT_ZK_SESSION_TIMEOUT.getUnit());

  public static final TimeAmount DEFAULT_CONNECTION_TIMEOUT = new TimeAmount(
      ZooKeeperUtils.DEFAULT_ZK_CONNECTION_TIMEOUT.getValue(),
        ZooKeeperUtils.DEFAULT_ZK_CONNECTION_TIMEOUT.getUnit());

  /**
   * Creates a new client configuration with defaults for the session timeout and credentials.
   *
   * @param servers ZooKeeper server addresses.
   * @return A new configuration.
   */
  public static ZooKeeperConfig create(Iterable<InetSocketAddress> servers) {
    return new ZooKeeperConfig(
        servers,
        Optional.absent(), // chrootPath
        false,
        ZooKeeperUtils.DEFAULT_ZK_SESSION_TIMEOUT,
        ZooKeeperUtils.DEFAULT_ZK_CONNECTION_TIMEOUT,
        Optional.absent()); // credentials
  }

  private final Iterable<InetSocketAddress> servers;
  private final boolean inProcess;
  private final Amount<Integer, Time> sessionTimeout;
  private final Amount<Integer, Time> connectionTimeout;
  private final Optional<String> chrootPath;
  private final Optional<Credentials> credentials;

  /**
   * Creates a new client configuration.
   *
   * @param servers ZooKeeper server addresses.
   * @param inProcess Whether to run and create clients for an in-process ZooKeeper server.
   * @param chrootPath an optional chroot path
   * @param sessionTimeout Timeout duration for established sessions.
   * @param credentials ZooKeeper authentication credentials.
   */
  ZooKeeperConfig(
      Iterable<InetSocketAddress> servers,
      Optional<String> chrootPath,
      boolean inProcess,
      Amount<Integer, Time> sessionTimeout,
      Amount<Integer, Time> connectionTimeout,
      Optional<Credentials> credentials) {

    this.servers = MorePreconditions.checkNotBlank(servers);
    this.chrootPath = requireNonNull(chrootPath);
    this.inProcess = inProcess;
    this.sessionTimeout = requireNonNull(sessionTimeout);
    this.connectionTimeout = requireNonNull(connectionTimeout);
    this.credentials = requireNonNull(credentials);
  }

  /**
   * Creates a new configuration identical to this configuration, but with the provided
   * credentials.
   *
   * @param newCredentials ZooKeeper authentication credentials.
   * @return A modified clone of this configuration.
   */
  public ZooKeeperConfig withCredentials(Credentials newCredentials) {
    return new ZooKeeperConfig(
        servers,
        chrootPath,
        inProcess,
        sessionTimeout,
        connectionTimeout,
        Optional.of(newCredentials));
  }

  Iterable<InetSocketAddress> getServers() {
    return servers;
  }

  boolean isInProcess() {
    return inProcess;
  }

  public Amount<Integer, Time> getSessionTimeout() {
    return sessionTimeout;
  }

  public Amount<Integer, Time> getConnectionTimeout() {
    return connectionTimeout;
  }

  Optional<String> getChrootPath() {
    return chrootPath;
  }

  public Optional<Credentials> getCredentials() {
    return credentials;
  }
}

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

import javax.annotation.Nullable;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.zookeeper.Credentials;
import org.apache.aurora.scheduler.config.splitters.CommaSplitter;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.config.validators.NotEmptyIterable;

import static org.apache.aurora.scheduler.discovery.ZooKeeperConfig.DEFAULT_CONNECTION_TIMEOUT;
import static org.apache.aurora.scheduler.discovery.ZooKeeperConfig.DEFAULT_SESSION_TIMEOUT;

/**
 * A factory that creates a {@link ZooKeeperConfig} instance based on command line argument
 * values.
 */
public final class FlaggedZooKeeperConfig {

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-zk_in_proc",
        description =
            "Launches an embedded zookeeper server for local testing causing -zk_endpoints "
                + "to be ignored if specified.",
        arity = 1)
    public boolean inProcess = false;

    @Parameter(
        names = "-zk_endpoints",
        required = true,
        validateValueWith = NotEmptyIterable.class,
        description = "Endpoint specification for the ZooKeeper servers.",
        splitter = CommaSplitter.class)
    public List<InetSocketAddress> zkEndpoints;

    @Parameter(names = "-zk_chroot_path",
        description = "chroot path to use for the ZooKeeper connections")
    public String chrootPath;

    @Parameter(names = "-zk_session_timeout", description = "The ZooKeeper session timeout.")
    public TimeAmount sessionTimeout = DEFAULT_SESSION_TIMEOUT;

    @Parameter(names = "-zk_connection_timeout", description = "The ZooKeeper connection timeout.")
    public TimeAmount connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;

    @Parameter(names = "-zk_digest_credentials",
        description = "user:password to use when authenticating with ZooKeeper.")
    public String digestCredentials;
  }

  private FlaggedZooKeeperConfig() {
    // Utility class.
  }

  /**
   * Creates a configuration from command line arguments.
   *
   * @return Configuration instance.
   */
  public static ZooKeeperConfig create(Options opts) {
    return new ZooKeeperConfig(
        opts.zkEndpoints,
        Optional.fromNullable(opts.chrootPath),
        opts.inProcess,
        Amount.of(opts.sessionTimeout.getValue().intValue(), opts.sessionTimeout.getUnit()),
        Amount.of(opts.connectionTimeout.getValue().intValue(), opts.connectionTimeout.getUnit()),
        getCredentials(opts.digestCredentials));
  }

  private static Optional<Credentials> getCredentials(@Nullable String userAndPass) {
    if (userAndPass == null) {
      return Optional.absent();
    }

    List<String> parts = ImmutableList.copyOf(Splitter.on(":").split(userAndPass));
    if (parts.size() != 2) {
      throw new IllegalArgumentException(
          "zk_digest_credentials must be formatted as user:pass");
    }
    return Optional.of(Credentials.digestCredentials(parts.get(0), parts.get(1)));
  }
}

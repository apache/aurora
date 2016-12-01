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

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.common.args.constraints.NotEmpty;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.zookeeper.Credentials;
import org.apache.aurora.common.zookeeper.ZooKeeperUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory that creates a {@link ZooKeeperConfig} instance based on command line argument
 * values.
 */
public final class FlaggedZooKeeperConfig {
  private static final Logger LOG = LoggerFactory.getLogger(FlaggedZooKeeperConfig.class);

  @CmdLine(name = "zk_use_curator",
      help = "DEPRECATED: Uses Apache Curator as the zookeeper client; otherwise a copy of Twitter "
          + "commons/zookeeper (the legacy library) is used.")
  private static final Arg<Boolean> USE_CURATOR = Arg.create(true);

  @CmdLine(name = "zk_in_proc",
      help = "Launches an embedded zookeeper server for local testing causing -zk_endpoints "
          + "to be ignored if specified.")
  private static final Arg<Boolean> IN_PROCESS = Arg.create(false);

  @NotEmpty
  @CmdLine(name = "zk_endpoints", help = "Endpoint specification for the ZooKeeper servers.")
  private static final Arg<List<InetSocketAddress>> ZK_ENDPOINTS = Arg.create();

  @CmdLine(name = "zk_chroot_path", help = "chroot path to use for the ZooKeeper connections")
  private static final Arg<String> CHROOT_PATH = Arg.create(null);

  @CmdLine(name = "zk_session_timeout", help = "The ZooKeeper session timeout.")
  private static final Arg<Amount<Integer, Time>> SESSION_TIMEOUT =
      Arg.create(ZooKeeperUtils.DEFAULT_ZK_SESSION_TIMEOUT);

  @CmdLine(name = "zk_digest_credentials",
           help = "user:password to use when authenticating with ZooKeeper.")
  private static final Arg<String> DIGEST_CREDENTIALS = Arg.create(null);

  private FlaggedZooKeeperConfig() {
    // Utility class.
  }

  /**
   * Creates a configuration from command line arguments.
   *
   * @return Configuration instance.
   */
  public static ZooKeeperConfig create() {
    if (USE_CURATOR.hasAppliedValue()) {
      LOG.warn("The -zk_use_curator flag is deprecated and will be removed in a future release.");
    }
    return new ZooKeeperConfig(
        USE_CURATOR.get(),
        ZK_ENDPOINTS.get(),
        Optional.fromNullable(CHROOT_PATH.get()),
        IN_PROCESS.get(),
        SESSION_TIMEOUT.get(),
        getCredentials(DIGEST_CREDENTIALS.get()));
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

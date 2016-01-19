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
package org.apache.aurora.scheduler.zookeeper.guice.client.flagged;

import java.net.InetSocketAddress;
import java.util.List;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.common.args.constraints.NotEmpty;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.zookeeper.ZooKeeperClient;
import org.apache.aurora.common.zookeeper.ZooKeeperClient.Credentials;
import org.apache.aurora.common.zookeeper.ZooKeeperUtils;
import org.apache.aurora.scheduler.zookeeper.guice.client.ZooKeeperClientModule.ClientConfig;

/**
 * A factory that creates a {@link ClientConfig} instance based on command line argument values.
 */
public final class FlaggedClientConfig {
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

  interface Params {
    boolean zkInProc();

    List<InetSocketAddress> zkEndpoints();

    Optional<String> zkChrootPath();

    Amount<Integer, Time> zkSessionTimeout();

    Optional<String> zkDigestCredentials();
  }

  private FlaggedClientConfig() {
    // Utility class.
  }

  /**
   * Creates a configuration from command line arguments.
   *
   * @return Configuration instance.
   */
  public static ClientConfig create() {
    Params params = new Params() {
      @Override
      public boolean zkInProc() {
        return IN_PROCESS.get();
      }

      @Override
      public List<InetSocketAddress> zkEndpoints() {
        return ZK_ENDPOINTS.get();
      }

      @Override
      public Optional<String> zkChrootPath() {
        return Optional.fromNullable(CHROOT_PATH.get());
      }

      @Override
      public Amount<Integer, Time> zkSessionTimeout() {
        return SESSION_TIMEOUT.get();
      }

      @Override
      public Optional<String> zkDigestCredentials() {
        return Optional.fromNullable(DIGEST_CREDENTIALS.get());
      }
    };

    return new ClientConfig(
        params.zkEndpoints(),
        params.zkChrootPath(),
        params.zkInProc(),
        params.zkSessionTimeout(),
        params.zkDigestCredentials()
            .transform(FlaggedClientConfig::getCredentials)
            .or(Credentials.NONE));
  }

  private static Credentials getCredentials(String userAndPass) {
    List<String> parts = ImmutableList.copyOf(Splitter.on(":").split(userAndPass));
    if (parts.size() != 2) {
      throw new IllegalArgumentException(
          "zk_digest_credentials must be formatted as user:pass");
    }
    return ZooKeeperClient.digestCredentials(parts.get(0), parts.get(1));
  }
}

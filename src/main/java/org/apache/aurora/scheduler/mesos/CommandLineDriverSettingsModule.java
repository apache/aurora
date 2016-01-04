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
package org.apache.aurora.scheduler.mesos;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.inject.AbstractModule;
import com.google.protobuf.ByteString;

import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.common.args.constraints.NotNull;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.mesos.Protos.FrameworkInfo;
import static org.apache.mesos.Protos.FrameworkInfo.Capability;
import static org.apache.mesos.Protos.FrameworkInfo.Capability.Type.REVOCABLE_RESOURCES;

/**
 * Creates and binds {@link DriverSettings} based on values found on the command line.
 */
public class CommandLineDriverSettingsModule extends AbstractModule {

  private static final Logger LOG =
      LoggerFactory.getLogger(CommandLineDriverSettingsModule.class);

  @NotNull
  @CmdLine(name = "mesos_master_address",
      help = "Address for the mesos master, can be a socket address or zookeeper path.")
  private static final Arg<String> MESOS_MASTER_ADDRESS = Arg.create();

  @VisibleForTesting
  static final String PRINCIPAL_KEY = "aurora_authentication_principal";
  @VisibleForTesting
  static final String SECRET_KEY = "aurora_authentication_secret";

  @CmdLine(name = "framework_authentication_file",
      help = "Properties file which contains framework credentials to authenticate with Mesos"
          + "master. Must contain the properties '" + PRINCIPAL_KEY + "' and "
          + "'" + SECRET_KEY + "'.")
  private static final Arg<File> FRAMEWORK_AUTHENTICATION_FILE = Arg.create();

  @CmdLine(name = "framework_failover_timeout",
      help = "Time after which a framework is considered deleted.  SHOULD BE VERY HIGH.")
  private static final Arg<Amount<Long, Time>> FRAMEWORK_FAILOVER_TIMEOUT =
      Arg.create(Amount.of(21L, Time.DAYS));

  @CmdLine(name = "framework_announce_principal",
      help = "When 'framework_authentication_file' flag is set, the FrameworkInfo "
          + "registered with the mesos master will also contain the principal. This is "
          + "necessary if you intend to use mesos authorization via mesos ACLs. "
          + "The default will change in a future release.")
  private static final Arg<Boolean> FRAMEWORK_ANNOUNCE_PRINCIPAL = Arg.create(false);

  @CmdLine(name = "executor_user",
      help = "User to start the executor. Defaults to \"root\". "
          + "Set this to an unprivileged user if the mesos master was started with "
          + "\"--no-root_submissions\". If set to anything other than \"root\", the executor "
          + "will ignore the \"role\" setting for jobs since it can't use setuid() anymore. "
          + "This means that all your jobs will run under the specified user and the user has "
          + "to exist on the mesos slaves.")
  private static final Arg<String> EXECUTOR_USER = Arg.create("root");

  @CmdLine(name = "receive_revocable_resources",
      help = "Allows receiving revocable resource offers from Mesos.")
  private static final Arg<Boolean> RECEIVE_REVOCABLE_RESOURCES = Arg.create(false);

  // TODO(wfarner): Figure out a way to change this without risk of fallout (MESOS-703).
  private static final String TWITTER_FRAMEWORK_NAME = "TwitterScheduler";

  @Override
  protected void configure() {
    Optional<Protos.Credential> credentials = getCredentials();
    Optional<String> principal = Optional.absent();
    if (FRAMEWORK_ANNOUNCE_PRINCIPAL.get() && credentials.isPresent()) {
      principal = Optional.of(credentials.get().getPrincipal());
    }
    DriverSettings settings = new DriverSettings(
        MESOS_MASTER_ADDRESS.get(),
        credentials,
        buildFrameworkInfo(
            EXECUTOR_USER.get(),
            principal,
            FRAMEWORK_FAILOVER_TIMEOUT.get(),
            RECEIVE_REVOCABLE_RESOURCES.get()));
    bind(DriverSettings.class).toInstance(settings);
  }

  private static Optional<Protos.Credential> getCredentials() {
    if (FRAMEWORK_AUTHENTICATION_FILE.hasAppliedValue()) {
      Properties properties;
      try {
        properties = parseCredentials(new FileInputStream(FRAMEWORK_AUTHENTICATION_FILE.get()));
      } catch (FileNotFoundException e) {
        LOG.error("Authentication File not Found");
        throw Throwables.propagate(e);
      }

      LOG.info(
          "Connecting to master using authentication (principal: {}).",
          properties.get(PRINCIPAL_KEY));

      return Optional.of(Protos.Credential.newBuilder()
          .setPrincipal(properties.getProperty(PRINCIPAL_KEY))
          .setSecret(ByteString.copyFromUtf8(properties.getProperty(SECRET_KEY)))
          .build());
    } else {
      return Optional.absent();
    }
  }

  @VisibleForTesting
  static FrameworkInfo buildFrameworkInfo(
      String executorUser,
      Optional<String> principal,
      Amount<Long, Time> failoverTimeout,
      boolean revocable) {

    FrameworkInfo.Builder infoBuilder = FrameworkInfo.newBuilder()
        .setUser(executorUser)
        .setName(TWITTER_FRAMEWORK_NAME)
            // Require slave checkpointing.  Assumes slaves have '--checkpoint=true' arg set.
        .setCheckpoint(true)
        .setFailoverTimeout(failoverTimeout.as(Time.SECONDS));
    if (principal.isPresent()) {
      infoBuilder.setPrincipal(principal.get());
    }

    if (revocable) {
      infoBuilder.addCapabilities(Capability.newBuilder().setType(REVOCABLE_RESOURCES));
    }
    return infoBuilder.build();
  }

  @VisibleForTesting
  static Properties parseCredentials(InputStream credentialsStream) {
    Properties properties = new Properties();
    try {
      properties.load(credentialsStream);
    } catch (IOException e) {
      LOG.error("Unable to load authentication file");
      throw Throwables.propagate(e);
    }
    Preconditions.checkState(properties.containsKey(PRINCIPAL_KEY),
        "The framework authentication file is missing the key: %s", PRINCIPAL_KEY);
    Preconditions.checkState(properties.containsKey(SECRET_KEY),
        "The framework authentication file is missing the key: %s", SECRET_KEY);
    return properties;
  }
}

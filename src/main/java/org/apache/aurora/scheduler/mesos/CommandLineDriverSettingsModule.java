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

import javax.inject.Singleton;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.mesos.v1.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.mesos.v1.Protos.FrameworkInfo;
import static org.apache.mesos.v1.Protos.FrameworkInfo.Capability;
import static org.apache.mesos.v1.Protos.FrameworkInfo.Capability.Type.GPU_RESOURCES;
import static org.apache.mesos.v1.Protos.FrameworkInfo.Capability.Type.REVOCABLE_RESOURCES;

/**
 * Creates and binds {@link DriverSettings} based on values found on the command line.
 */
public class CommandLineDriverSettingsModule extends AbstractModule {

  private static final Logger LOG =
      LoggerFactory.getLogger(CommandLineDriverSettingsModule.class);

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-mesos_master_address",
        required = true,
        description = "Address for the mesos master, can be a socket address or zookeeper path.")
    public String mesosMasterAddress;

    public static final String PRINCIPAL_KEY = "aurora_authentication_principal";
    public static final String SECRET_KEY = "aurora_authentication_secret";

    @Parameter(names = "-framework_authentication_file",
        description =
            "Properties file which contains framework credentials to authenticate with Mesos"
                + "master. Must contain the properties '" + PRINCIPAL_KEY + "' and "
                + "'" + SECRET_KEY + "'.")
    public File frameworkAuthenticationFile;

    @Parameter(names = "-framework_failover_timeout",
        description = "Time after which a framework is considered deleted.  SHOULD BE VERY HIGH.")
    public TimeAmount frameworkFailoverTimeout = new TimeAmount(21, Time.DAYS);

    @Parameter(names = "-framework_announce_principal",
        description = "When 'framework_authentication_file' flag is set, the FrameworkInfo "
            + "registered with the mesos master will also contain the principal. This is "
            + "necessary if you intend to use mesos authorization via mesos ACLs. "
            + "The default will change in a future release. Changing this value is backwards "
            + "incompatible. For details, see MESOS-703.",
        arity = 1)
    public boolean frameworkAnnouncePrincipal = false;

    @Parameter(names = "-framework_name",
        description = "Name used to register the Aurora framework with Mesos.")
    public String frameworkName = "Aurora";

    @Parameter(names = "-executor_user",
        description = "User to start the executor. Defaults to \"root\". "
            + "Set this to an unprivileged user if the mesos master was started with "
            + "\"--no-root_submissions\". If set to anything other than \"root\", the executor "
            + "will ignore the \"role\" setting for jobs since it can't use setuid() anymore. "
            + "This means that all your jobs will run under the specified user and the user has "
            + "to exist on the Mesos agents.")
    public String executorUser = "root";

    @Parameter(names = "-receive_revocable_resources",
        description = "Allows receiving revocable resource offers from Mesos.",
        arity = 1)
    public boolean receiveRevocableResources = false;

    @Parameter(names = "-partition_aware",
        description = "Enable paritition-aware status updates.",
        arity = 1)
    public boolean isPartitionAware = false;

    @Parameter(names = "-mesos_role",
        description =
            "The Mesos role this framework will register as. The default is to left this empty, "
                + "and the framework will register without any role and only receive unreserved "
                + "resources in offer.")
    public String mesosRole;
  }

  private final Options options;
  private final boolean allowGpuResource;

  public CommandLineDriverSettingsModule(Options options, boolean allowGpuResource) {
    this.options = options;
    this.allowGpuResource = allowGpuResource;
  }

  @Override
  protected void configure() {
    Optional<Protos.Credential> credentials = getCredentials(options);
    Optional<String> principal = Optional.absent();
    if (options.frameworkAnnouncePrincipal && credentials.isPresent()) {
      principal = Optional.of(credentials.get().getPrincipal());
    }
    Optional<String> role = Optional.fromNullable(options.mesosRole);
    DriverSettings settings = new DriverSettings(options.mesosMasterAddress, credentials);
    bind(DriverSettings.class).toInstance(settings);

    FrameworkInfo base =
        buildFrameworkInfo(
            options.frameworkName,
            options.executorUser,
            principal,
            options.frameworkFailoverTimeout,
            options.receiveRevocableResources,
            allowGpuResource,
            options.isPartitionAware,
            role);
    bind(FrameworkInfo.class)
        .annotatedWith(FrameworkInfoFactory.FrameworkInfoFactoryImpl.BaseFrameworkInfo.class)
        .toInstance(base);
    bind(FrameworkInfoFactory.class).to(FrameworkInfoFactory.FrameworkInfoFactoryImpl.class);
    bind(FrameworkInfoFactory.FrameworkInfoFactoryImpl.class).in(Singleton.class);

  }

  private static Optional<Protos.Credential> getCredentials(Options opts) {
    if (opts.frameworkAuthenticationFile == null) {
      return Optional.absent();
    } else {
      Properties properties;
      try {
        properties = parseCredentials(new FileInputStream(opts.frameworkAuthenticationFile));
      } catch (FileNotFoundException e) {
        LOG.error("Authentication File not Found");
        throw new RuntimeException(e);
      }

      LOG.info(
          "Connecting to master using authentication (principal: {}).",
          properties.get(Options.PRINCIPAL_KEY));

      return Optional.of(Protos.Credential.newBuilder()
          .setPrincipal(properties.getProperty(Options.PRINCIPAL_KEY))
          .setSecret(properties.getProperty(Options.SECRET_KEY))
          .build());
    }
  }

  @VisibleForTesting
  // See: https://github.com/apache/mesos/commit/d06d05c76eca13745ca73039b93ad684b9d07196
  // The role field has been deprecated but the replacement is not ready. We'll also have to
  // turn on MULTI_ROLES capability before we can use the roles field.
  @SuppressWarnings("deprecation")
  static FrameworkInfo buildFrameworkInfo(
      String frameworkName,
      String executorUser,
      Optional<String> principal,
      Amount<Long, Time> failoverTimeout,
      boolean revocable,
      boolean allowGpu,
      boolean enablePartitionAwareness,
      Optional<String> role) {

    FrameworkInfo.Builder infoBuilder = FrameworkInfo.newBuilder()
        .setName(frameworkName)
        .setUser(executorUser)
            // Require slave checkpointing.  Assumes slaves have '--checkpoint=true' arg set.
        .setCheckpoint(true)
        .setFailoverTimeout(failoverTimeout.as(Time.SECONDS));
    if (principal.isPresent()) {
      infoBuilder.setPrincipal(principal.get());
    }

    if (revocable) {
      infoBuilder.addCapabilities(Capability.newBuilder().setType(REVOCABLE_RESOURCES));
    }

    if (allowGpu) {
      infoBuilder.addCapabilities(Capability.newBuilder().setType(GPU_RESOURCES));
    }

    if (enablePartitionAwareness) {
      infoBuilder.addCapabilities(
          Capability.newBuilder().setType(Capability.Type.PARTITION_AWARE).build());
    }

    if (role.isPresent()) {
      infoBuilder.setRole(role.get());
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
      throw new RuntimeException(e);
    }
    Preconditions.checkState(properties.containsKey(Options.PRINCIPAL_KEY),
        "The framework authentication file is missing the key: %s", Options.PRINCIPAL_KEY);
    Preconditions.checkState(properties.containsKey(Options.SECRET_KEY),
        "The framework authentication file is missing the key: %s", Options.SECRET_KEY);
    return properties;
  }
}

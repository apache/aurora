/*
 * Copyright 2013 Twitter, Inc.
 *
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
package org.apache.aurora.scheduler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.Credential;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

/**
 * Factory to create scheduler driver instances.
 */
public interface DriverFactory extends Function<String, SchedulerDriver> {

  static class DriverFactoryImpl implements DriverFactory {
    private static final Logger LOG = Logger.getLogger(DriverFactory.class.getName());

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

    /**
     * Require Mesos slaves to have checkpointing enabled. Slaves with checkpointing enabled will
     * attempt to write checkpoints when required by a task's framework. These checkpoints allow
     * executors to be reattached rather than killed when a slave is restarted.
     *
     * This flag is dangerous! When enabled tasks will not launch on slaves without checkpointing
     * enabled.
     *
     * Behavior is as follows:
     * (Scheduler -require_slave_checkpoint=true,  Slave --checkpoint=true):
     *   Tasks will launch.        Checkpoints will be written.
     * (Scheduler -require_slave_checkpoint=true,   Slave --checkpoint=false):
     *   Tasks WILL NOT launch.
     * (Scheduler -require_slave_checkpoint=false,  Slave --checkpoint=true):
     *   Tasks will launch.        Checkpoints will not be written.
     * (Scheduler -require_slave_checkpoint=false,  Slave --checkpoint=false):
     *   Tasks will launch.        Checkpoints will not be written.
     *
     * TODO(ksweeney): Remove warning table after https://issues.apache.org/jira/browse/MESOS-444
     * is resolved.
     */
    @CmdLine(name = "require_slave_checkpoint",
        help = "DANGEROUS! Require Mesos slaves to have checkpointing enabled. When enabled a "
            + "slave restart should not kill executors, but the scheduler will not be able to "
            + "launch tasks on slaves without --checkpoint=true in their command lines. See "
            + "DriverFactory.java for more information.")
    private static final Arg<Boolean> REQUIRE_SLAVE_CHECKPOINT = Arg.create(false);

    private static final String EXECUTOR_USER = "root";

    private static final String TWITTER_FRAMEWORK_NAME = "TwitterScheduler";

    private final Provider<Scheduler> scheduler;

    @Inject
    DriverFactoryImpl(Provider<Scheduler> scheduler) {
      this.scheduler = Preconditions.checkNotNull(scheduler);
    }

    @VisibleForTesting
    static Properties parseCredentials(InputStream credentialsStream) {
      Properties properties = new Properties();
      try {
        properties.load(credentialsStream);
      } catch (IOException e) {
        LOG.severe("Unable to load authentication file");
        throw Throwables.propagate(e);
      }
      Preconditions.checkState(properties.containsKey(PRINCIPAL_KEY),
          "The framework authentication file is missing the key: %s", PRINCIPAL_KEY);
      Preconditions.checkState(properties.containsKey(SECRET_KEY),
          "The framework authentication file is missing the key: %s", SECRET_KEY);
      return properties;
    }

    @Override
    public SchedulerDriver apply(@Nullable String frameworkId) {
      LOG.info("Connecting to mesos master: " + MESOS_MASTER_ADDRESS.get());

      FrameworkInfo.Builder frameworkInfo = FrameworkInfo.newBuilder()
          .setUser(EXECUTOR_USER)
          .setName(TWITTER_FRAMEWORK_NAME)
          .setCheckpoint(REQUIRE_SLAVE_CHECKPOINT.get())
          .setFailoverTimeout(FRAMEWORK_FAILOVER_TIMEOUT.get().as(Time.SECONDS));

      if (frameworkId != null) {
        LOG.info("Found persisted framework ID: " + frameworkId);
        frameworkInfo.setId(FrameworkID.newBuilder().setValue(frameworkId));
      } else {
        LOG.warning("Did not find a persisted framework ID, connecting as a new framework.");
      }

      if (FRAMEWORK_AUTHENTICATION_FILE.hasAppliedValue()) {
        Properties properties;
        try {
          properties = parseCredentials(new FileInputStream(FRAMEWORK_AUTHENTICATION_FILE.get()));
        } catch (FileNotFoundException e) {
          LOG.severe("Authentication File not Found");
          throw Throwables.propagate(e);
        }

        LOG.info(String.format("Connecting to master using authentication (principal: %s).",
            properties.get(PRINCIPAL_KEY)));

        Credential credential = Credential.newBuilder()
            .setPrincipal(properties.getProperty(PRINCIPAL_KEY))
            .setSecret(ByteString.copyFromUtf8(properties.getProperty(SECRET_KEY)))
            .build();

        return new MesosSchedulerDriver(
            scheduler.get(),
            frameworkInfo.build(),
            MESOS_MASTER_ADDRESS.get(),
            credential);
      } else {
        LOG.warning("Connecting to master without authentication!");
        return new MesosSchedulerDriver(
            scheduler.get(),
            frameworkInfo.build(),
            MESOS_MASTER_ADDRESS.get());
      }
    }
  }
}

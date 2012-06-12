package com.twitter.mesos.scheduler;

import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.inject.Provider;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

/**
 * Factory to create scheduler driver instances.
 */
public interface DriverFactory extends Function<String, SchedulerDriver> {

  static class DriverFactoryImpl implements DriverFactory {
    private static final Logger LOG = Logger.getLogger(DriverFactory.class.getName());

    @NotNull
    @CmdLine(name = "mesos_master_address",
            help = "Mesos address for the master, can be a mesos address or zookeeper path.")
    private static final Arg<String> MESOS_MASTER_ADDRESS = Arg.create();

    @CmdLine(name = "framework_failover_timeout",
        help = "Time after which a framework is considered deleted.  SHOULD BE VERY HIGH.")
    private static final Arg<Amount<Long, Time>> FRAMEWORK_FAILOVER_TIMEOUT =
        Arg.create(Amount.of(21L, Time.DAYS));

    private static final String EXECUTOR_USER = "root";

    private static final String TWITTER_FRAMEWORK_NAME = "TwitterScheduler";

    private final Provider<Scheduler> scheduler;

    @Inject
    DriverFactoryImpl(Provider<Scheduler> scheduler) {
      this.scheduler = Preconditions.checkNotNull(scheduler);
    }

    @Override
    public SchedulerDriver apply(@Nullable String frameworkId) {
      LOG.info("Connecting to mesos master: " + MESOS_MASTER_ADDRESS.get());

      FrameworkInfo.Builder frameworkInfo = FrameworkInfo.newBuilder()
          .setUser(EXECUTOR_USER)
          .setName(TWITTER_FRAMEWORK_NAME)
          .setFailoverTimeout(FRAMEWORK_FAILOVER_TIMEOUT.get().as(Time.SECONDS));

      if (frameworkId != null) {
        LOG.info("Found persisted framework ID: " + frameworkId);
        frameworkInfo.setId(FrameworkID.newBuilder().setValue(frameworkId));
      } else {
        LOG.warning("Did not find a persisted framework ID, connecting as a new framework.");
      }

      return new MesosSchedulerDriver(scheduler.get(), frameworkInfo.build(),
          MESOS_MASTER_ADDRESS.get());
    }
  }
}

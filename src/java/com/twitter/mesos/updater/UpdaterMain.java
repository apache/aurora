package com.twitter.mesos.updater;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;

import com.twitter.common.application.AbstractApplication;
import com.twitter.common.application.modules.HttpModule;
import com.twitter.common.application.modules.LogModule;
import com.twitter.common.application.modules.StatsModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.thrift.ThriftFactory;
import com.twitter.mesos.gen.MesosSchedulerManager.Iface;

/**
 * Mesos updater, coordinates rolling restarts of tasks in a job.
 *
 * @author William Farner
 */
public class UpdaterMain extends AbstractApplication {

  private static final Logger LOG = Logger.getLogger(UpdaterMain.class.getName());

  @NotNull
  @CmdLine(name = "scheduler_address", help = "Thrift service address for the scheduler.")
  private static final Arg<InetSocketAddress> schedulerAddress = Arg.create();

  @NotNull
  @CmdLine(name = "update_token", help = "Unique update token.")
  private static final Arg<String> updateToken = Arg.create();

  @CmdLine(name = "thrift_timeout", help = "Scheduler thrift request timeout.")
  private static final Arg<Amount<Long, Time>> thriftTimeout =
      Arg.create(Amount.of(1L, Time.SECONDS));

  @Inject Coordinator updateCoordinator;

  @Override
  public void run() {
    try {
      updateCoordinator.run();
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Update failed.", e);
    }
  }

  @Override
  public Iterable<? extends Module> getModules() {
    return Arrays.asList(
        new LogModule(),
        new HttpModule(),
        new StatsModule(),
        new UpdaterModule()
    );
  }

  static class UpdaterModule extends AbstractModule {
    @Override protected void configure() {
      Iface scheduler = ThriftFactory.create(Iface.class)
          .withMaxConnectionsPerEndpoint(5)
          .withSslEnabled()
          .build(ImmutableSet.of(schedulerAddress.get()))
          .builder()
          .noRetries()
          .withRequestTimeout(thriftTimeout.get())
          .create();

      bind(Iface.class).toInstance(scheduler);
      bind(String.class).annotatedWith(UpdateToken.class).toInstance(updateToken.get());
    }
  }
}

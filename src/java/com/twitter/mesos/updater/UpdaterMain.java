package com.twitter.mesos.updater;

import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.twitter.common_internal.args.Option;
import com.twitter.common_internal.process.GuicedProcess;
import com.twitter.common_internal.process.GuicedProcessOptions;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.thrift.ThriftFactory;
import com.twitter.mesos.gen.MesosSchedulerManager.Iface;
import com.twitter.mesos.updater.UpdaterMain.Options;

/**
 * Mesos updater, coordinates rolling restarts of tasks in a job.
 *
 * @author wfarner
 */
public class UpdaterMain extends GuicedProcess<Options, RuntimeException> {

  private static final Logger LOG = Logger.getLogger(UpdaterMain.class.getName());

  public static class Options extends GuicedProcessOptions {
    @Option(name = "scheduler_address", required = true,
        usage = "Thrift service address for the scheduler.")
    public InetSocketAddress schedulerAddress;

    @Option(name = "update_token", required = true, usage = "Unique update token.")
    public String updateToken;

    @Option(name = "thrift_timeout", usage = "Scheduler thrift request timeout.")
    public Amount<Long, Time> thriftTimeout = Amount.of(1L, Time.SECONDS);
  }

  @Inject Coordinator updateCoordinator;

  @Override
  protected void runProcess() {
    try {
      updateCoordinator.run();
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Update failed.", e);
    }
  }

  @Override protected Iterable<Class<? extends Module>> getProcessModuleClasses() {
    return ImmutableList.<Class<? extends Module>>of(UpdaterModule.class);
  }

  static class UpdaterModule extends AbstractModule {
    private final Options options;

    @Inject UpdaterModule(Options options) {
      this.options = options;
    }

    @Override protected void configure() {
      Iface scheduler = ThriftFactory.create(Iface.class)
          .withMaxConnectionsPerEndpoint(5)
          .build(ImmutableSet.of(options.schedulerAddress))
          .builder()
          .noRetries()
          .withRequestTimeout(options.thriftTimeout)
          .create();

      bind(Iface.class).toInstance(scheduler);
      bind(String.class).annotatedWith(UpdateToken.class).toInstance(options.updateToken);
    }
  }

  public UpdaterMain() {
    super(Options.class);
  }

  public static void main(String[] args) {
    new UpdaterMain().run(args);
  }
}

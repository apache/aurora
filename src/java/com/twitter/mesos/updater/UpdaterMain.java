package com.twitter.mesos.updater;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.twitter.common.args.Option;
import com.twitter.common.process.GuicedProcess;
import com.twitter.common.process.GuicedProcessOptions;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.thrift.ThriftFactory;
import com.twitter.common.thrift.ThriftFactory.ThriftFactoryException;
import com.twitter.common.zookeeper.ServerSetImpl;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common.zookeeper.ZooKeeperUtils;
import com.twitter.mesos.gen.MesosSchedulerManager.Iface;
import com.twitter.mesos.updater.UpdaterMain.Options;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Mesos updater, coordinates rolling restarts of tasks in a job.
 *
 * @author wfarner
 */
public class UpdaterMain extends GuicedProcess<Options, RuntimeException> {

  private static final Logger LOG = Logger.getLogger(UpdaterMain.class.getName());

  public static class Options extends GuicedProcessOptions {
    @Option(name = "scheduler_zk_path", required = true,
        usage = "ZK discovery path for the scheduler.")
    public String schedulerZkPath;

    @Option(name = "update_token", required = true, usage = "Unique update token.")
    public String updateToken;

    @Option(name = "zk_hosts", usage = "ZooKeeper servers.")
    public List<InetSocketAddress> zkHosts =
        ImmutableList.copyOf(ZooKeeperUtils.DEFAULT_ZK_ENDPOINTS);

    @Option(name = "thrift_timeout", usage = "Scheduler thrift request timeout.")
    public Amount<Long, Time> thriftTimeout = Amount.of(1L, Time.SECONDS);
  }

  @Override
  protected void runProcess() {
    // TODO(wfarner): Implement.
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  class UpdaterModule extends AbstractModule {
    private final Options options;

    @Inject UpdaterModule(Options options) {
      this.options = options;
    }

    @Override protected void configure() {
      ZooKeeperClient zkClient =
          new ZooKeeperClient(ZooKeeperUtils.DEFAULT_ZK_SESSION_TIMEOUT, options.zkHosts);

      Iface scheduler = null;
      try {
        scheduler = ThriftFactory.create(Iface.class)
            .withMaxConnectionsPerEndpoint(5)
            .build(new ServerSetImpl(zkClient, options.schedulerZkPath))
            .builder()
            .noRetries()
            .withRequestTimeout(options.thriftTimeout)
            .create();
      } catch (ThriftFactoryException e) {
        LOG.log(Level.SEVERE, "Failed to create thrift client.", e);
        Throwables.propagate(e);
      }

      bind(Iface.class).toInstance(scheduler);
    }
  }

  public UpdaterMain() {
    super(Options.class);
  }

  public static void main(String[] args) {
    new UpdaterMain().run(args);
  }
}

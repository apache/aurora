package com.twitter.nexus.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.twitter.common.process.GuicedProcess;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.nexus.scheduler.httphandlers.SchedulerzHome;
import com.twitter.nexus.scheduler.httphandlers.SchedulerzJob;
import com.twitter.nexus.scheduler.httphandlers.SchedulerzUser;
import org.apache.zookeeper.ZooDefs;

import java.util.logging.Logger;

public class SchedulerModule extends AbstractModule {
  private final static Logger LOG = Logger.getLogger(SchedulerModule.class.getName());
  private SchedulerMain.TwitterSchedulerOptions options;

  /**
   * {@literal @Named} binding key for the puffin service backend.
   */
  static final String NEXUS_MASTER_SERVER_SET =
      "com.twitter.nexus.scheduler.SchedulerModule.NEXUS_MASTER_SERVER_SET";

  @Inject
  public SchedulerModule(SchedulerMain.TwitterSchedulerOptions options) {
    this.options = Preconditions.checkNotNull(options);
  }

  @Override
  protected void configure() {
    bind(CronJobScheduler.class).in(Singleton.class);
    bind(SchedulerCore.class).in(Singleton.class);
    bind(NexusSchedulerImpl.class).in(Singleton.class);

    GuicedProcess.registerServlet(binder(), "/schedulerz", SchedulerzHome.class, false);
    GuicedProcess.registerServlet(binder(), "/schedulerz/user", SchedulerzUser.class, true);
    GuicedProcess.registerServlet(binder(), "/schedulerz/job", SchedulerzJob.class, true);
  }

  @Provides
  @Singleton
  final ZooKeeperClient provideZooKeeperClient() {
    return new ZooKeeperClient(Amount.of(options.zooKeeperSessionTimeoutSecs, Time.SECONDS),
        ImmutableSet.copyOf(options.zooKeeperEndpoints));
  }

  @Provides
  @Singleton
  final ServerSet provideSchedulerServerSet(ZooKeeperClient zkClient) {
    return new ServerSet(zkClient, ZooDefs.Ids.OPEN_ACL_UNSAFE, options.nexusSchedulerNameSpec);
  }
}

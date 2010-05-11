package com.twitter.nexus.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.common.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.util.logging.Logger;

public class SchedulerModule extends AbstractModule {
  private final static Logger LOG = Logger.getLogger(SchedulerModule.class.getName());
  private SchedulerMain.TwitterSchedulerOptions options;

  @Inject
  public SchedulerModule(SchedulerMain.TwitterSchedulerOptions options) {
    this.options = Preconditions.checkNotNull(options);
  }

  @Override
  protected void configure() {
    bind(SchedulerCore.class);
    bind(NexusSchedulerImpl.class);
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

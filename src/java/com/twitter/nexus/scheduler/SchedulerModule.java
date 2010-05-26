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
import com.twitter.nexus.scheduler.persistence.FileSystemPersistence;
import com.twitter.nexus.scheduler.persistence.PersistenceLayer;
import com.twitter.nexus.scheduler.persistence.ZooKeeperPersistence;
import nexus.NexusSchedulerDriver;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.util.logging.Level;
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
    bind(CronJobManager.class).in(Singleton.class);
    bind(SchedulerCore.class).to(SchedulerCoreImpl.class).in(Singleton.class);
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

  @Provides
  final PersistenceLayer providePersistenceLayer() {
    if (options.schedulerPersistenceZooKeeperPath == null) {
      return new FileSystemPersistence(options.schedulerPersistenceLocalPath);
    } else {
      return new ZooKeeperPersistence(options.schedulerPersistenceZooKeeperPath,
          options.schedulerPersistenceZooKeeperVersion);
    }
  }

  @Provides
  @Singleton
  final NexusSchedulerDriver provideNexusSchedulerDriver(NexusSchedulerImpl scheduler,
      ZooKeeperClient zkClient) {
    String nexusMaster = options.nexusMasterAddress;
    if (nexusMaster == null) {
      try {
        Stat stat = zkClient.get().exists(options.nexusMasterNameSpec, false);
        nexusMaster = new String(zkClient.get().getData(options.nexusMasterNameSpec, false, stat));
      } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
        LOG.log(Level.SEVERE, "Failed to connect to ZooKeeper.", e);
      } catch (KeeperException e) {
        LOG.log(Level.SEVERE, "Failed while reading from ZooKeeper.", e);
      } catch (InterruptedException e) {
        LOG.log(Level.SEVERE, "Interrupted while reading from ZooKeeper.", e);
      }
    }

    if (nexusMaster == null) throw new RuntimeException("Unable to continue without nexus master.");

    LOG.info("Connecting to nexus master: " + nexusMaster);
    return new NexusSchedulerDriver(scheduler, nexusMaster);
  }
}

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
import com.twitter.common.zookeeper.SingletonService;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.nexus.gen.SchedulerState;
import com.twitter.nexus.scheduler.httphandlers.SchedulerzHome;
import com.twitter.nexus.scheduler.httphandlers.SchedulerzJob;
import com.twitter.nexus.scheduler.httphandlers.SchedulerzUser;
import com.twitter.nexus.scheduler.persistence.Codec;
import com.twitter.nexus.scheduler.persistence.EncodingPersistenceLayer;
import com.twitter.nexus.scheduler.persistence.FileSystemPersistence;
import com.twitter.nexus.scheduler.persistence.PersistenceLayer;
import com.twitter.nexus.scheduler.persistence.ThriftBinaryCodec;
import com.twitter.nexus.scheduler.persistence.ZooKeeperPersistence;
import nexus.NexusSchedulerDriver;

import javax.annotation.Nullable;
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
    if (options.zooKeeperEndpoints == null) {
      LOG.info("ZooKeeper endpoints not specified, ZooKeeper interaction disabled.");
      return null;
    } else {
      return new ZooKeeperClient(Amount.of(options.zooKeeperSessionTimeoutSecs, Time.SECONDS),
          ImmutableSet.copyOf(options.zooKeeperEndpoints));
    }
  }

  @Provides
  @Nullable
  @Singleton
  SingletonService provideSingletonService(@Nullable ZooKeeperClient zkClient) {
    if (zkClient == null) {
      LOG.info("Leader election disabled since ZooKeeper integration is disabled.");
      return null;
    }

    return new SingletonService(zkClient, options.nexusSchedulerNameSpec);
  }

  @Provides
  final PersistenceLayer<SchedulerState> providePersistenceLayer(
      @Nullable ZooKeeperClient zkClient) {
    Codec<SchedulerState, byte[]> codec = new ThriftBinaryCodec<SchedulerState>() {
      @Override public SchedulerState createInputInstance() {
        return new SchedulerState();
      }
    };

    PersistenceLayer<byte[]> binaryPersistence;
    if (options.schedulerPersistenceZooKeeperPath == null) {
      binaryPersistence = new FileSystemPersistence(options.schedulerPersistenceLocalPath);
    } else {
      if (zkClient == null) {
        throw new IllegalArgumentException(
            "ZooKeeper client must be available for ZooKeeper persistence layer.");
      }

      binaryPersistence = new ZooKeeperPersistence(zkClient,
          options.schedulerPersistenceZooKeeperPath,
          options.schedulerPersistenceZooKeeperVersion);
    }

    return new EncodingPersistenceLayer<SchedulerState, byte[]>(binaryPersistence, codec);
  }

  @Provides
  @Singleton
  final NexusSchedulerDriver provideNexusSchedulerDriver(NexusSchedulerImpl scheduler,
      SchedulerCore schedulerCore) {
    LOG.info("Connecting to nexus master: " + options.nexusMasterAddress);

    String frameworkId = schedulerCore.getFrameworkId();
    if (frameworkId != null) {
      LOG.info("Found persisted framework ID: " + frameworkId);
      return new NexusSchedulerDriver(scheduler, options.nexusMasterAddress, frameworkId);
    } else {
      return new NexusSchedulerDriver(scheduler, options.nexusMasterAddress);
    }
  }
}

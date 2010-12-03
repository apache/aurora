package com.twitter.mesos.scheduler;

import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.twitter.common.process.GuicedProcess;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.zookeeper.SingletonService;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.mesos.gen.NonVolatileSchedulerState;
import com.twitter.mesos.scheduler.SchedulingFilter.SchedulingFilterImpl;
import com.twitter.mesos.scheduler.httphandlers.SchedulerzHome;
import com.twitter.mesos.scheduler.httphandlers.SchedulerzJob;
import com.twitter.mesos.scheduler.httphandlers.SchedulerzUser;
import com.twitter.mesos.scheduler.persistence.EncodingPersistenceLayer;
import com.twitter.mesos.scheduler.persistence.FileSystemPersistence;
import com.twitter.mesos.scheduler.persistence.PersistenceLayer;
import com.twitter.mesos.scheduler.persistence.ZooKeeperPersistence;
import mesos.MesosSchedulerDriver;

import javax.annotation.Nullable;
import java.util.Map;
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
    bind(CronJobManager.class).in(Singleton.class);
    bind(ExecutorTracker.class).to(ExecutorTrackerImpl.class).in(Singleton.class);
    bind(MesosSchedulerImpl.class).in(Singleton.class);
    bind(WorkQueue.class).to(WorkQueueImpl.class).in(Singleton.class);
    bind(Key.get(new TypeLiteral<Map<String, String>>() {},
        Names.named(SchedulingFilterImpl.MACHINE_RESTRICTIONS)))
        .toInstance(options.machineRestrictions);
    bind(SchedulingFilter.class).to(SchedulingFilterImpl.class);
    bind(SchedulerCore.class).to(SchedulerCoreImpl.class).in(Singleton.class);

    GuicedProcess.registerServlet(binder(), "/schedulerz", SchedulerzHome.class, false);
    GuicedProcess.registerServlet(binder(), "/schedulerz/user", SchedulerzUser.class, true);
    GuicedProcess.registerServlet(binder(), "/schedulerz/job", SchedulerzJob.class, true);
  }

  @Provides
  @Nullable
  @Singleton
  final ZooKeeperClient provideZooKeeperClient() {
    if (options.zooKeeperEndpoints == null) {
      LOG.info("ZooKeeper endpoints not specified, ZooKeeper interaction disabled.");
      return null;
    } else {
      return new ZooKeeperClient(Amount.of(options.zooKeeperSessionTimeoutSecs, Time.SECONDS),
          options.zooKeeperEndpoints);
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

    return new SingletonService(zkClient, options.mesosSchedulerNameSpec);
  }

  @Provides
  final PersistenceLayer<NonVolatileSchedulerState> providePersistenceLayer(
      @Nullable ZooKeeperClient zkClient) {

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

    return new EncodingPersistenceLayer(binaryPersistence);
  }

  @Provides
  @Singleton
  final MesosSchedulerDriver provideMesosSchedulerDriver(MesosSchedulerImpl scheduler,
      SchedulerCore schedulerCore) {
    LOG.info("Connecting to mesos master: " + options.mesosMasterAddress);

    String frameworkId = schedulerCore.getFrameworkId();
    if (frameworkId != null) {
      LOG.info("Found persisted framework ID: " + frameworkId);
      return new MesosSchedulerDriver(scheduler, options.mesosMasterAddress, frameworkId);
    } else {
      LOG.warning("Did not find a persisted framework ID, connecting as a new framework.");
      return new MesosSchedulerDriver(scheduler, options.mesosMasterAddress);
    }
  }
}

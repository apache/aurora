package com.twitter.mesos.scheduler;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.zookeeper.SingletonService;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.mesos.gen.NonVolatileSchedulerState;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.Driver.MesosDriverImpl;
import com.twitter.mesos.scheduler.SchedulingFilter.SchedulingFilterImpl;
import com.twitter.mesos.scheduler.httphandlers.Mname;
import com.twitter.mesos.scheduler.httphandlers.SchedulerzHome;
import com.twitter.mesos.scheduler.httphandlers.SchedulerzJob;
import com.twitter.mesos.scheduler.httphandlers.SchedulerzUser;
import com.twitter.mesos.scheduler.persistence.EncodingPersistenceLayer;
import com.twitter.mesos.scheduler.persistence.FileSystemPersistence;
import com.twitter.mesos.scheduler.persistence.PersistenceLayer;
import com.twitter.mesos.scheduler.persistence.PersistenceLayer.PersistenceException;
import com.twitter.mesos.scheduler.persistence.ZooKeeperPersistence;
import mesos.MesosSchedulerDriver;
import mesos.Protos.FrameworkID;
import mesos.SchedulerDriver;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.twitter.common.process.GuicedProcess.registerServlet;

public class SchedulerModule extends AbstractModule {
  private final static Logger LOG = Logger.getLogger(SchedulerModule.class.getName());
  private SchedulerMain.TwitterSchedulerOptions options;

  @Inject
  public SchedulerModule(SchedulerMain.TwitterSchedulerOptions options) {
    this.options = Preconditions.checkNotNull(options);
  }

  @Override
  protected void configure() {
    // Bindings for SchedulerMain.
    ZooKeeperClient zkClient = new ZooKeeperClient(
        Amount.of(options.zooKeeperSessionTimeoutSecs, Time.SECONDS), options.zooKeeperEndpoints);
    bind(ZooKeeperClient.class).toInstance(zkClient);
    bind(SingletonService.class).toInstance(
        new SingletonService(zkClient, options.mesosSchedulerNameSpec));
    // MesosSchedulerDriver handled in provider.
    bind(new TypeLiteral<AtomicReference<AtomicReference<InetSocketAddress>>>() {})
        .in(Singleton.class);

    // Bindings for MesosSchedulerImpl.
    bind(SchedulerCore.class).to(SchedulerCoreImpl.class).in(Singleton.class);
    bind(ExecutorTracker.class).to(ExecutorTrackerImpl.class).in(Singleton.class);

    // Bindings for SchedulerCoreImpl.
    bind(CronJobManager.class).in(Singleton.class);
    bind(ImmediateJobManager.class).in(Singleton.class);
    // PersistenceLayer handled in provider.
    bind(Driver.class).to(MesosDriverImpl.class).in(Singleton.class);
    bind(SchedulingFilter.class).to(SchedulingFilterImpl.class);
    // updaterTaskProvider handled in provider.

    // Bindings for SchedulingFilterImpl.
    bind(Key.get(new TypeLiteral<Map<String, String>>() {},
        Names.named(SchedulingFilterImpl.MACHINE_RESTRICTIONS)))
        .toInstance(options.machineRestrictions);

    bind(MesosSchedulerImpl.class).in(Singleton.class);

    registerServlet(binder(), "/schedulerz", SchedulerzHome.class, false);
    registerServlet(binder(), "/schedulerz/user", SchedulerzUser.class, true);
    registerServlet(binder(), "/schedulerz/job", SchedulerzJob.class, true);
    registerServlet(binder(), "/mname", Mname.class, false);
  }

  @Provides
  Function<String, TwitterTaskInfo> provideUpdateTaskSupplier(
      final AtomicReference<InetSocketAddress> schedulerThriftPort) {
    return new Function<String, TwitterTaskInfo>() {
      @Override public TwitterTaskInfo apply(String updateToken) {
        InetSocketAddress thriftPort = schedulerThriftPort.get();
        if (thriftPort == null) return null;

        String schedulerAddress = thriftPort.getHostName() + ":" + thriftPort.getPort();

        return new TwitterTaskInfo()
            .setHdfsPath(options.jobUpdaterHdfsPath)
            .setShardId(0)
            .setStartCommand(
                "unzip mesos-updater.zip;"
                + " java -cp dist/mesos-updater.jar:target/mesos-updater/lib/*.jar"
                + " --scheduler_address=" + schedulerAddress + " --update_token=" + updateToken);
      }
    };
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
  final SchedulerDriver provideMesosSchedulerDriver(MesosSchedulerImpl scheduler,
      PersistenceLayer<NonVolatileSchedulerState> persistence) {
    LOG.info("Connecting to mesos master: " + options.mesosMasterAddress);

    // TODO(wfarner): This was a fix to unweave a circular guice dependency.  Fix.
    String frameworkId = null;
    try {
      frameworkId = persistence.fetch().getFrameworkId();
    } catch (PersistenceException e) {
      LOG.log(Level.SEVERE, "Failed to recover persisted state.", e);
    }

    if (frameworkId != null) {
      LOG.info("Found persisted framework ID: " + frameworkId);
      return new MesosSchedulerDriver(scheduler, options.mesosMasterAddress,
          FrameworkID.newBuilder().setValue(frameworkId).build());
    } else {
      LOG.warning("Did not find a persisted framework ID, connecting as a new framework.");
      return new MesosSchedulerDriver(scheduler, options.mesosMasterAddress);
    }
  }
}

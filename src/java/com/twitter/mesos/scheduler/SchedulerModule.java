package com.twitter.mesos.scheduler;

import com.google.common.base.Function;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import com.twitter.common.base.Closure;
import com.twitter.common.base.Command;
import com.twitter.common.base.ExceptionalCommand;
import com.twitter.common.inject.TimedInterceptor;
import com.twitter.common.io.FileUtils;
import com.twitter.common.process.ActionRegistry;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.zookeeper.SingletonService;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.mesos.HttpAssets;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.SchedulingFilter.SchedulingFilterImpl;
import com.twitter.mesos.scheduler.httphandlers.CreateJob;
import com.twitter.mesos.scheduler.httphandlers.Mname;
import com.twitter.mesos.scheduler.httphandlers.SchedulerzHome;
import com.twitter.mesos.scheduler.httphandlers.SchedulerzJob;
import com.twitter.mesos.scheduler.httphandlers.SchedulerzUser;
import com.twitter.mesos.scheduler.storage.Migrator;
import com.twitter.mesos.scheduler.storage.StorageRole;
import com.twitter.mesos.scheduler.storage.db.DbStorageModule;
import com.twitter.mesos.scheduler.storage.stream.StreamStorageModule;
import mesos.MesosSchedulerDriver;
import mesos.Protos.FrameworkID;
import mesos.Protos.TaskID;
import mesos.Scheduler;
import mesos.SchedulerDriver;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperServer.BasicDataTreeBuilder;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.twitter.common.process.GuicedProcess.registerServlet;

public class SchedulerModule extends AbstractModule {
  private static final Logger LOG = Logger.getLogger(SchedulerModule.class.getName());

  private final ActionRegistry shutdownRegistry;
  private final SchedulerMain.TwitterSchedulerOptions options;

  @Inject
  public SchedulerModule(ActionRegistry shutdownRegistry,
      SchedulerMain.TwitterSchedulerOptions options) {
    this.shutdownRegistry = checkNotNull(shutdownRegistry);
    this.options = checkNotNull(options);
  }

  @Override
  protected void configure() {
    // Enable intercepted method timings
    TimedInterceptor.bind(binder());

    // Bindings for SchedulerMain.
    ZooKeeperClient zkClient = createZooKeeperClient();
    bind(ZooKeeperClient.class).toInstance(zkClient);
    bind(SingletonService.class).toInstance(
        new SingletonService(zkClient, options.mesosSchedulerNameSpec));
    // MesosSchedulerDriver handled in provider.
    bind(new TypeLiteral<AtomicReference<InetSocketAddress>>() {}).in(Singleton.class);

    // Bindings for MesosSchedulerImpl.
    bind(SchedulerCore.class).to(SchedulerCoreImpl.class).in(Singleton.class);
    bind(ExecutorTracker.class).to(ExecutorTrackerImpl.class).in(Singleton.class);

    // Bindings for SchedulerCoreImpl.
    bind(CronJobManager.class).in(Singleton.class);
    bind(ImmediateJobManager.class).in(Singleton.class);

    install(new DbStorageModule(options, StorageRole.Role.Primary));
    if (options.upgradeStorage) {
      // Both StreamStorageModule and the Migrator need a binding for the Set of installed job
      // managers
      Multibinder<JobManager> jobManagers = Multibinder.newSetBinder(binder(), JobManager.class);
      jobManagers.addBinding().to(CronJobManager.class);
      jobManagers.addBinding().to(ImmediateJobManager.class);

      install(new StreamStorageModule(StorageRole.Role.Legacy));
      Migrator.bind(binder());
    }

    bind(SchedulingFilter.class).to(SchedulingFilterImpl.class);

    // updaterTaskProvider handled in provider.

    // Bindings for SchedulingFilterImpl.
    bind(Key.get(new TypeLiteral<Map<String, String>>() {},
        Names.named(SchedulingFilterImpl.MACHINE_RESTRICTIONS)))
        .toInstance(options.machineRestrictions);

    bind(Scheduler.class).to(MesosSchedulerImpl.class).in(Singleton.class);

    HttpAssets.register(binder());
    registerServlet(binder(), "/scheduler", SchedulerzHome.class, false);
    registerServlet(binder(), "/scheduler/user", SchedulerzUser.class, true);
    registerServlet(binder(), "/scheduler/job", SchedulerzJob.class, true);
    registerServlet(binder(), "/mname", Mname.class, false);
    registerServlet(binder(), "/create_job", CreateJob.class, false);
  }

  @Provides
  Function<String, TwitterTaskInfo> provideUpdateTaskSupplier(
      final AtomicReference<InetSocketAddress> schedulerThriftPort) {
    return new Function<String, TwitterTaskInfo>() {
      @Override public TwitterTaskInfo apply(String updateToken) {
        InetSocketAddress thriftPort = schedulerThriftPort.get();
        if (thriftPort == null) {
          LOG.severe("Scheduler thrift port requested for updater before it was set!");
          return null;
        }

        String schedulerAddress = thriftPort.getHostName() + ":" + thriftPort.getPort();

        return new TwitterTaskInfo()
            .setHdfsPath(options.jobUpdaterHdfsPath)
            .setShardId(0)
            .setStartCommand(
                "unzip mesos-updater.zip;"
                + " java -cp mesos-updater.jar:libs/*.jar"
                + " com.twitter.mesos.updater.UpdaterMain"
                + " -scheduler_address=" + schedulerAddress + " -update_token=" + updateToken);
      }
    };
  }

  @Provides
  @Singleton
  SchedulerDriver provideMesosSchedulerDriver(Scheduler scheduler, SchedulerCore schedulerCore) {
    LOG.info("Connecting to mesos master: " + options.mesosMasterAddress);

    String frameworkId = schedulerCore.initialize();
    final SchedulerDriver schedulerDriver = createDriver(scheduler, frameworkId);
    schedulerCore.start(new Closure<String>() {
      @Override public void execute(String taskId) throws RuntimeException {
        int result = schedulerDriver.killTask(TaskID.newBuilder().setValue(taskId).build());
        if (result != 0) {
          LOG.severe(String.format("Attempt to kill task %s failed with code %d",
              taskId, result));
        }
      }
    });
    return schedulerDriver;
  }

  private SchedulerDriver createDriver(Scheduler scheduler, @Nullable String frameworkId) {
    if (frameworkId != null) {
      LOG.info("Found persisted framework ID: " + frameworkId);
      return new MesosSchedulerDriver(scheduler, options.mesosMasterAddress,
          FrameworkID.newBuilder().setValue(frameworkId).build());
    } else {
      LOG.warning("Did not find a persisted framework ID, connecting as a new framework.");
      return new MesosSchedulerDriver(scheduler, options.mesosMasterAddress);
    }
  }

  private ZooKeeperClient createZooKeeperClient() {
    Amount<Integer, Time> timeout = Amount.of(options.zooKeeperSessionTimeoutSecs, Time.SECONDS);
    if (options.zooKeeperInProcess) {
      try {
        return startLocalZookeeper(timeout);
      } catch (IOException e) {
        throw new RuntimeException("Unable to start local zookeeper", e);
      } catch (InterruptedException e) {
        throw new RuntimeException("Unable to start local zookeeper", e);
      }
    } else {
      return new ZooKeeperClient(timeout, options.zooKeeperEndpoints);
    }
  }

  private ZooKeeperClient startLocalZookeeper(Amount<Integer, Time> sessionTimeout)
      throws IOException, InterruptedException {
    ZooKeeperServer zooKeeperServer =
        new ZooKeeperServer(new FileTxnSnapLog(createTempDir(), createTempDir()),
            new BasicDataTreeBuilder());

    final NIOServerCnxn.Factory connectionFactory =
        new NIOServerCnxn.Factory(new InetSocketAddress(0));
    connectionFactory.startup(zooKeeperServer);
    shutdownRegistry.addAction(new Command() {
      @Override public void execute() throws RuntimeException {
        if (connectionFactory.isAlive()) {
          connectionFactory.shutdown();
        }
      }
    });
    int zkPort = zooKeeperServer.getClientPort();
    LOG.info("Embedded zookeeper cluster started on port " + zkPort);
    return new ZooKeeperClient(sessionTimeout,
        InetSocketAddress.createUnresolved("localhost", zkPort));
  }

  private File createTempDir() {
    final File tempDir = FileUtils.createTempDir();
    shutdownRegistry.addAction(new ExceptionalCommand<IOException>() {
      @Override public void execute() throws IOException {
        org.apache.commons.io.FileUtils.deleteDirectory(tempDir);
      }
    });
    return tempDir;
  }
}

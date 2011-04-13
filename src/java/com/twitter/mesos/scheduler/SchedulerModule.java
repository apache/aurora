package com.twitter.mesos.scheduler;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import com.twitter.common.application.ActionRegistry;
import com.twitter.common.application.ShutdownStage;
import com.twitter.common.application.http.Registration;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.common.base.Closure;
import com.twitter.common.base.Closures;
import com.twitter.common.base.Command;
import com.twitter.common.base.ExceptionalCommand;
import com.twitter.common.inject.TimedInterceptor;
import com.twitter.common.io.FileUtils;
import com.twitter.common.logging.ScribeLog;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.thrift.ThriftFactory.ThriftFactoryException;
import com.twitter.common.zookeeper.SingletonService;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common.zookeeper.ZooKeeperUtils;
import com.twitter.common_internal.cuckoo.CuckooWriter;
import com.twitter.common_internal.zookeeper.TwitterZk;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.MesosSchedulerImpl.ExecutorPath;
import com.twitter.mesos.scheduler.PulseMonitor.PulseMonitorImpl;
import com.twitter.mesos.scheduler.SchedulingFilter.SchedulingFilterImpl;
import com.twitter.mesos.scheduler.httphandlers.CreateJob;
import com.twitter.mesos.scheduler.httphandlers.HttpAssets;
import com.twitter.mesos.scheduler.httphandlers.Mname;
import com.twitter.mesos.scheduler.httphandlers.SchedulerzHome;
import com.twitter.mesos.scheduler.httphandlers.SchedulerzJob;
import com.twitter.mesos.scheduler.httphandlers.SchedulerzUser;
import com.twitter.mesos.scheduler.storage.Migrator;
import com.twitter.mesos.scheduler.storage.StorageRole;
import com.twitter.mesos.scheduler.storage.stream.StreamStorageModule;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperServer.BasicDataTreeBuilder;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

public class SchedulerModule extends AbstractModule {
  private static final Logger LOG = Logger.getLogger(SchedulerModule.class.getName());

  @NotNull
  @CmdLine(name = "mesos_scheduler_ns",
      help ="The name service name for the mesos scheduler thrift server.")
  private static final Arg<String> mesosSchedulerNameSpec = Arg.create();

  @CmdLine(name = "scheduler_upgrade_storage",
          help ="True to upgrade storage from a legacy system to a new primary system.")
  private static final Arg<Boolean>upgradeStorage = Arg.create(true);

  @CmdLine(name = "machine_restrictions",
      help ="Map of machine hosts to job keys."
              + "  If A maps to B, only B can run on A and B can only run on A.")
  public static final Arg<Map<String, String>> machineRestrictions =
      Arg.create(Collections.<String, String>emptyMap());

  @CmdLine(name = "job_updater_hdfs_path", help ="HDFS path to the job updater package.")
  private static final Arg<String> jobUpdaterHdfsPath =
      Arg.create("/mesos/pkg/mesos/bin/mesos-updater.zip");

  @NotNull
  @CmdLine(name = "mesos_master_address",
          help ="Mesos address for the master, can be a mesos address or zookeeper path.")
  private static final Arg<String> mesosMasterAddress = Arg.create();

  @CmdLine(name = "zk_in_proc",
          help ="Launches an embedded zookeeper server for local testing")
  private static final Arg<Boolean> zooKeeperInProcess = Arg.create(false);

  @CmdLine(name = "zk_endpoints", help ="Endpoint specification for the ZooKeeper servers.")
  private static final Arg<List<InetSocketAddress>> zooKeeperEndpoints =
      Arg.<List<InetSocketAddress>>create(ImmutableList.copyOf(TwitterZk.DEFAULT_ZK_ENDPOINTS));

  @CmdLine(name = "zk_session_timeout", help ="The ZooKeeper session timeout.")
  public static final Arg<Amount<Integer, Time>> zooKeeperSessionTimeout =
      Arg.create(ZooKeeperUtils.DEFAULT_ZK_SESSION_TIMEOUT);

  @NotNull
  @CmdLine(name = "executor_path", help ="Path to the executor launch script.")
  private static final Arg<String> executorPath = Arg.create();

  @CmdLine(name = "cuckoo_scribe_endpoints",
      help = "Cuckoo endpoints for stat export.  Leave empty to disable stat export.")
  private static final Arg<List<InetSocketAddress>> CUCKOO_SCRIBE_ENDPOINTS = Arg.create(
      Arrays.asList(InetSocketAddress.createUnresolved("localhost", 1463)));

  @CmdLine(name = "cuckoo_scribe_category", help = "Scribe category to send cuckoo stats to.")
  private static final Arg<String> CUCKOO_SCRIBE_CATEGORY =
      Arg.create(CuckooWriter.DEFAULT_SCRIBE_CATEGORY);

  @NotNull
  @CmdLine(name = "cuckoo_service_id", help = "Cuckoo service ID.")
  private static final Arg<String> CUCKOO_SERVICE_ID = Arg.create();

  @CmdLine(name = "cuckoo_source_id", help = "Cuckoo stat source ID.")
  private static final Arg<String> CUCKOO_SOURCE_ID = Arg.create("mesos_scheduler");

  @CmdLine(name = "executor_dead_threashold", help =
      "Time after which the scheduler will consider an executor dead and attempt to revive it.")
  private static final Arg<Amount<Long, Time>> EXECUTOR_DEAD_THRESHOLD =
      Arg.create(Amount.of(10L, Time.MINUTES));

  @Override
  protected void configure() {
    // Enable intercepted method timings
    TimedInterceptor.bind(binder());

    // Bindings for MesosSchedulerImpl.
    bind(SchedulerCore.class).to(SchedulerCoreImpl.class).in(Singleton.class);
    bind(ExecutorTracker.class).to(ExecutorTrackerImpl.class).in(Singleton.class);

    // Bindings for SchedulerCoreImpl.
    bind(CronJobManager.class).in(Singleton.class);
    bind(ImmediateJobManager.class).in(Singleton.class);
    bind(new TypeLiteral<PulseMonitor<String>>() {})
        .toInstance(new PulseMonitorImpl<String>(EXECUTOR_DEAD_THRESHOLD.get()));

    if (upgradeStorage.get()) {
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
        .toInstance(machineRestrictions.get());
    bind(String.class).annotatedWith(ExecutorPath.class).toInstance(executorPath.get());
    bind(Scheduler.class).to(MesosSchedulerImpl.class).in(Singleton.class);

    HttpAssets.register(binder());
    Registration.registerServlet(binder(), "/scheduler", SchedulerzHome.class, false);
    Registration.registerServlet(binder(), "/scheduler/user", SchedulerzUser.class, true);
    Registration.registerServlet(binder(), "/scheduler/job", SchedulerzJob.class, true);
    Registration.registerServlet(binder(), "/mname", Mname.class, false);
    Registration.registerServlet(binder(), "/create_job", CreateJob.class, true);
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
            .setHdfsPath(jobUpdaterHdfsPath.get())
            .setShardId(0)
            .setStartCommand(
                "unzip mesos-updater.zip;"
                + " java -cp mesos-updater.jar"
                + " com.twitter.common.application.AppLauncher"
                + " -arg_scan_packages=com.twitter"
                + " -app_class=com.twitter.mesos.updater.UpdaterMain"
                + " -scheduler_address=" + schedulerAddress + " -update_token=" + updateToken);
      }
    };
  }

  @Provides
  @Singleton
  SchedulerDriver provideMesosSchedulerDriver(Scheduler scheduler, SchedulerCore schedulerCore) {
    LOG.info("Connecting to mesos master: " + mesosMasterAddress.get());

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
      return new MesosSchedulerDriver(scheduler, mesosMasterAddress.get(),
          FrameworkID.newBuilder().setValue(frameworkId).build());
    } else {
      LOG.warning("Did not find a persisted framework ID, connecting as a new framework.");
      return new MesosSchedulerDriver(scheduler, mesosMasterAddress.get());
    }
  }

  @Provides
  @Singleton
  SingletonService provideSingletonService(ZooKeeperClient zkClient) {
    return new SingletonService(zkClient, mesosSchedulerNameSpec.get());
  }

  @Provides
  @Singleton
  ZooKeeperClient provideZooKeeperClient(@ShutdownStage ActionRegistry shutdownRegistry) {
    if (zooKeeperInProcess.get()) {
      try {
        return startLocalZookeeper(shutdownRegistry);
      } catch (IOException e) {
        throw new RuntimeException("Unable to start local zookeeper", e);
      } catch (InterruptedException e) {
        throw new RuntimeException("Unable to start local zookeeper", e);
      }
    } else {
      return new ZooKeeperClient(zooKeeperSessionTimeout.get(), zooKeeperEndpoints.get());
    }
  }

  @Provides
  @Singleton
  Closure<Map<String, ? extends Number>> provideStatSink() throws ThriftFactoryException {
    if (CUCKOO_SCRIBE_ENDPOINTS.get().isEmpty()) {
      LOG.info("No scribe hosts provided, cuckoo stat export disabled.");
      return Closures.noop();
    } else {
      return new CuckooWriter(new ScribeLog(CUCKOO_SCRIBE_ENDPOINTS.get()),
          CUCKOO_SCRIBE_CATEGORY.get(), CUCKOO_SERVICE_ID.get(), CUCKOO_SOURCE_ID.get());
    }
  }

  private ZooKeeperClient startLocalZookeeper(ActionRegistry shutdownRegistry)
      throws IOException, InterruptedException {
    ZooKeeperServer zooKeeperServer =
        new ZooKeeperServer(
            new FileTxnSnapLog(createTempDir(shutdownRegistry), createTempDir(shutdownRegistry)),
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
    return new ZooKeeperClient(zooKeeperSessionTimeout.get(),
        InetSocketAddress.createUnresolved("localhost", zkPort));
  }

  private File createTempDir(ActionRegistry shutdownRegistry) {
    final File tempDir = FileUtils.createTempDir();
    shutdownRegistry.addAction(new ExceptionalCommand<IOException>() {
      @Override public void execute() throws IOException {
        org.apache.commons.io.FileUtils.deleteDirectory(tempDir);
      }
    });
    return tempDir;
  }
}

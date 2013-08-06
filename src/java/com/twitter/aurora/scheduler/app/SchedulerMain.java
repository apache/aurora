package com.twitter.aurora.scheduler.app;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nonnegative;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Singleton;

import com.twitter.aurora.auth.UnsecureAuthModule;
import com.twitter.aurora.scheduler.DriverFactory;
import com.twitter.aurora.scheduler.DriverFactory.DriverFactoryImpl;
import com.twitter.aurora.scheduler.MesosTaskFactory.ExecutorConfig;
import com.twitter.aurora.scheduler.SchedulerLifecycle;
import com.twitter.aurora.scheduler.SchedulerLifecycle.ShutdownOnDriverExit;
import com.twitter.aurora.scheduler.local.IsolatedSchedulerModule;
import com.twitter.aurora.scheduler.log.mesos.MesosLogStreamModule;
import com.twitter.aurora.scheduler.storage.backup.BackupModule;
import com.twitter.aurora.scheduler.storage.log.LogStorage;
import com.twitter.aurora.scheduler.storage.log.LogStorageModule;
import com.twitter.aurora.scheduler.storage.log.SnapshotStoreImpl;
import com.twitter.aurora.scheduler.storage.mem.MemStorageModule;
import com.twitter.aurora.scheduler.thrift.ThriftConfiguration;
import com.twitter.aurora.scheduler.thrift.ThriftModule;
import com.twitter.aurora.scheduler.thrift.auth.ThriftAuthModule;
import com.twitter.common.application.AbstractApplication;
import com.twitter.common.application.AppLauncher;
import com.twitter.common.application.modules.HttpModule;
import com.twitter.common.application.modules.LocalServiceRegistry;
import com.twitter.common.application.modules.LogModule;
import com.twitter.common.application.modules.StatsModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.CanRead;
import com.twitter.common.args.constraints.Exists;
import com.twitter.common.args.constraints.IsDirectory;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.common.inject.Bindings;
import com.twitter.common.inject.Bindings.KeyFactory;
import com.twitter.common.logging.RootLogConfig;
import com.twitter.common.zookeeper.Group;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.common.zookeeper.SingletonService;
import com.twitter.common.zookeeper.ZooKeeperUtils;
import com.twitter.common_internal.zookeeper.TwitterServerSet.Service;
import com.twitter.common_internal.zookeeper.TwitterServerSetModule;
import com.twitter.common_internal.zookeeper.ZooKeeperModule;
import com.twitter.common_internal.zookeeper.legacy.ServerSetMigrationModule.ServiceDiscovery;

/**
 * Launcher for the twitter mesos scheduler.
 */
public class SchedulerMain extends AbstractApplication {

  private static final Logger LOG = Logger.getLogger(SchedulerMain.class.getName());

  @CmdLine(name = "testing_isolated_scheduler",
      help = "If true, run in a testing mode with the scheduler isolated from other components.")
  private static final Arg<Boolean> ISOLATED_SCHEDULER = Arg.create(false);

  @NotNull
  @CmdLine(name = "cluster_name", help = "Name to identify the cluster being served.")
  private static final Arg<String> CLUSTER_NAME = Arg.create();

  @CanRead
  @NotNull
  @CmdLine(name = "mesos_ssl_keyfile",
      help = "JKS keyfile for operating the Mesos Thrift-over-SSL interface.")
  private static final Arg<File> MESOS_SSL_KEY_FILE = Arg.create();

  @Nonnegative
  @CmdLine(name = "thrift_port", help = "Thrift server port.")
  private static final Arg<Integer> THRIFT_PORT = Arg.create(0);

  @NotNull
  @CmdLine(name = "thermos_executor_path", help = "Path to the thermos executor launch script.")
  private static final Arg<String> THERMOS_EXECUTOR_PATH = Arg.create();

  @NotNull
  @Exists
  @IsDirectory
  @CmdLine(name = "backup_dir", help = "Directory to store backups under.")
  private static final Arg<File> BACKUP_DIR = Arg.create();

  @CmdLine(name = "auth_module",
      help = "A Guice module to provide auth bindings. NOTE: The default is unsecure.")
  public static final  Arg<? extends Class<? extends AbstractModule>> AUTH_MODULE =
      Arg.create(UnsecureAuthModule.class);

  @Inject private SingletonService schedulerService;
  @Inject private LocalServiceRegistry serviceRegistry;
  @Inject private SchedulerLifecycle schedulerLifecycle;
  @Inject private Optional<RootLogConfig.Configuration> glogConfig;

  private static Iterable<? extends Module> getSystemModules() {
    return Arrays.asList(
        new HttpModule(),
        new LogModule(),
        new StatsModule()
    );
  }

  @VisibleForTesting
  static Service createService(String clusterName) {
    return new Service("mesos", clusterName, "scheduler");
  }

  static Iterable<? extends Module> getModules(
      final String clusterName,
      final Optional<InetSocketAddress> zkHost,
      File backupDir,
      Module... additionalModules) {

    final Service schedulerService = createService(clusterName);
    Module serviceBinder = new AbstractModule() {
      @Override protected void configure() {
        bind(Service.class).toInstance(schedulerService);
      }
    };

    ImmutableList.Builder<Module> modules = ImmutableList.<Module>builder()
        .addAll(getSystemModules())
        .add(new AppModule(clusterName))
        .add(new ThriftModule())
        .add(new ThriftAuthModule())
        .add(serviceBinder)
        .add(additionalModules);

    Class<? extends AbstractModule> authModule = AUTH_MODULE.get();
    try {
      // If installation fails, no SessionValidator will be bound and app will fail to startup.
      modules.add(authModule.newInstance());
    } catch (InstantiationException e) {
      LOG.log(Level.WARNING, "Failed to instantiate auth module: " + authModule.getName(), e);
      throw new IllegalArgumentException(e);
    } catch (IllegalAccessException e) {
      LOG.log(Level.WARNING, "Failed to instantiate auth module: " + authModule.getName(), e);
      throw new IllegalArgumentException(e);
    }

    KeyFactory zkClientKeyFactory = Bindings.annotatedKeyFactory(ServiceDiscovery.class);
    if (zkHost.isPresent()) {
      modules.add(ZooKeeperModule.builder(ImmutableSet.of(zkHost.get()))
          .withDigestCredentials(schedulerService.getRole(), schedulerService.getRole())
          .withAcl(ZooKeeperUtils.EVERYONE_READ_CREATOR_ALL)
          .build(zkClientKeyFactory));
    } else {
      modules.add(TwitterServerSetModule
          .authenticatedZooKeeperModule(schedulerService)
          .withFlagOverrides()
          .build(zkClientKeyFactory));
    }
    modules.add(new TwitterServerSetModule(
        Key.get(ServerSet.class),
        zkClientKeyFactory,
        schedulerService));
    modules.add(new BackupModule(backupDir, SnapshotStoreImpl.class));
    // TODO(William Farner): Make all mem store implementation classes package private.
    modules.add(new MemStorageModule(Bindings.annotatedKeyFactory(LogStorage.WriteBehind.class)));
    modules.add(new LogStorageModule());
    return modules.build();
  }

  @Override
  public Iterable<? extends Module> getModules() {
    Module additional;
    if (ISOLATED_SCHEDULER.get()) {
      additional = new IsolatedSchedulerModule();
    } else {
      // TODO(William Farner): Push these bindings down into a "production" module.
      additional = new AbstractModule() {
        @Override protected void configure() {
          bind(DriverFactory.class).to(DriverFactoryImpl.class);
          bind(DriverFactoryImpl.class).in(Singleton.class);
          bind(Boolean.class).annotatedWith(ShutdownOnDriverExit.class).toInstance(true);

          MesosLogStreamModule.bind(binder(),
              Bindings.annotatedKeyFactory(ServiceDiscovery.class));
        }
      };
    }

    Module configModule = new AbstractModule() {
      @Override protected void configure() {
        bind(ThriftConfiguration.class).toInstance(new ThriftConfiguration() {
          @Override public InputStream getSslKeyStream() throws FileNotFoundException {
            return new FileInputStream(MESOS_SSL_KEY_FILE.get());
          }

          @Override public int getServingPort() {
            return THRIFT_PORT.get();
          }
        });
        bind(ExecutorConfig.class).toInstance(new ExecutorConfig(THERMOS_EXECUTOR_PATH.get()));
        bind(Boolean.class).annotatedWith(ShutdownOnDriverExit.class).toInstance(true);
      }
    };

    return getModules(
        CLUSTER_NAME.get(),
        Optional.<InetSocketAddress>absent(),
        BACKUP_DIR.get(),
        configModule,
        additional);
  }

  @Override
  public void run() {
    if (glogConfig.isPresent()) {
      // Setup log4j to match our jul glog config in order to pick up zookeeper logging.
      Log4jConfigurator.configureConsole(glogConfig.get());
    } else {
      LOG.warning("Running without expected glog configuration.");
    }

    SchedulerLifecycle.SchedulerCandidate candidate = schedulerLifecycle.prepare();

    Optional<InetSocketAddress> primarySocket = serviceRegistry.getPrimarySocket();
    if (!primarySocket.isPresent()) {
      throw new IllegalStateException("No primary service registered with LocalServiceRegistry.");
    }

    try {
      schedulerService.lead(primarySocket.get(), serviceRegistry.getAuxiliarySockets(), candidate);
    } catch (Group.WatchException e) {
      throw new IllegalStateException("Failed to watch group and lead service.", e);
    } catch (Group.JoinException e) {
      throw new IllegalStateException("Failed to join scheduler service group.", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Interrupted while joining scheduler service group.", e);
    }

    candidate.awaitShutdown();
  }

  public static void main(String[] args) {
    AppLauncher.launch(SchedulerMain.class, args);
  }
}

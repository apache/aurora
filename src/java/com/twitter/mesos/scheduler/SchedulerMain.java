package com.twitter.mesos.scheduler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Arrays;

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

import com.twitter.common.application.AbstractApplication;
import com.twitter.common.application.AppLauncher;
import com.twitter.common.application.modules.HttpModule;
import com.twitter.common.application.modules.LocalServiceRegistry;
import com.twitter.common.application.modules.LogModule;
import com.twitter.common.application.modules.StatsModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.CanRead;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.common.inject.Bindings;
import com.twitter.common.inject.Bindings.KeyFactory;
import com.twitter.common.zookeeper.Group;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.common.zookeeper.SingletonService;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common.zookeeper.ZooKeeperUtils;
import com.twitter.common_internal.webassets.BlueprintModule;
import com.twitter.common_internal.zookeeper.TwitterServerSet.Service;
import com.twitter.common_internal.zookeeper.TwitterServerSetModule;
import com.twitter.common_internal.zookeeper.ZooKeeperModule;
import com.twitter.common_internal.zookeeper.legacy.ServerSetMigrationModule;
import com.twitter.common_internal.zookeeper.legacy.ServerSetMigrationModule.ServiceDiscovery;
import com.twitter.mesos.scheduler.DriverFactory.DriverFactoryImpl;
import com.twitter.mesos.scheduler.MesosTaskFactory.MesosTaskFactoryImpl.ExecutorConfig;
import com.twitter.mesos.scheduler.SchedulerLifecycle.ShutdownOnDriverExit;
import com.twitter.mesos.scheduler.SchedulerModule.AuthMode;
import com.twitter.mesos.scheduler.ThriftServerLauncher.ThriftConfiguration;
import com.twitter.mesos.scheduler.log.mesos.MesosLogStreamModule;
import com.twitter.mesos.scheduler.storage.log.LogStorageModule;
import com.twitter.mesos.scheduler.testing.IsolatedSchedulerModule;

/**
 * Launcher for the twitter mesos scheduler.
 */
public class SchedulerMain extends AbstractApplication {

  @CmdLine(name = "testing_isolated_scheduler",
      help = "If true, run in a testing mode with the scheduler isolated from other components.")
  private static final Arg<Boolean> ISOLATED_SCHEDULER = Arg.create(false);

  @NotNull
  @CmdLine(name = "cluster_name", help = "Name to identify the cluster being served.")
  private static final Arg<String> CLUSTER_NAME = Arg.create();

  @CmdLine(name = "auth_mode", help = "Enforces RPC authentication with mesos client.")
  private static final Arg<AuthMode> AUTH_MODE = Arg.create(AuthMode.SECURE);

  @CanRead
  @NotNull
  @CmdLine(name = "mesos_ssl_keyfile",
      help = "JKS keyfile for operating the Mesos Thrift-over-SSL interface.")
  private static final Arg<File> MESOS_SSL_KEY_FILE = Arg.create();

  @Nonnegative
  @CmdLine(name = "thrift_port", help = "Thrift server port.")
  private static final Arg<Integer> THRIFT_PORT = Arg.create(0);

  @NotNull
  @CmdLine(name = "executor_path", help = "Path to the executor launch script.")
  private static final Arg<String> EXECUTOR_PATH = Arg.create();

  @NotNull
  @CmdLine(name = "thermos_executor_path", help = "Path to the thermos executor launch script.")
  private static final Arg<String> THERMOS_EXECUTOR_PATH = Arg.create();

  @CmdLine(name = "dual_publish",
      help = "If enabled the scheduler will dual publish its leadership in the legacy"
          + " -zk_endpoints cluster and the local service discovery cluster.")
  private static final Arg<Boolean> DUAL_PUBLISH = Arg.create(false);

  @Inject private SingletonService schedulerService;
  @Inject private LocalServiceRegistry serviceRegistry;
  @Inject private SchedulerLifecycle schedulerLifecycle;

  private static Iterable<? extends Module> getSystemModules() {
    return Arrays.asList(
        new HttpModule(),
        new LogModule(),
        new StatsModule(),
        new BlueprintModule()
    );
  }

  @VisibleForTesting
  static Service createService(String clusterName) {
    return new Service("mesos", clusterName, "scheduler");
  }

  static Iterable<? extends Module> getModules(
      final String clusterName,
      AuthMode authMode,
      final Optional<InetSocketAddress> zkHost,
      Module... additionalModules) {

    final Service schedulerService = createService(clusterName);
    Module serviceBinder = new AbstractModule() {
      @Override protected void configure() {
        bind(Service.class).toInstance(schedulerService);
      }
    };

    ImmutableList.Builder<Module> modules = ImmutableList.<Module>builder()
        .addAll(getSystemModules())
        .add(new SchedulerModule(clusterName, authMode))
        .add(serviceBinder)
        .add(additionalModules);

    if (DUAL_PUBLISH.get()) {
      modules.add(new ServerSetMigrationModule(
          schedulerService,
          Optional.of(ZooKeeperClient.digestCredentials("mesos", "mesos")),
          Optional.<String>absent())); // use the existing flagged legacy ss path
    } else {
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
    }

    if (!ISOLATED_SCHEDULER.get()) {
      modules.add(new AbstractModule() {
        @Override protected void configure() {
          LogStorageModule.bind(binder());
        }
      });
    }

    return modules.build();
  }

  @Override
  public Iterable<? extends Module> getModules() {
    Module additional;
    if (ISOLATED_SCHEDULER.get()) {
      additional = new IsolatedSchedulerModule();
    } else {
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
        bind(ExecutorConfig.class)
            .toInstance(new ExecutorConfig(EXECUTOR_PATH.get(), THERMOS_EXECUTOR_PATH.get()));
        bind(Boolean.class).annotatedWith(ShutdownOnDriverExit.class).toInstance(true);
      }
    };

    return getModules(
        CLUSTER_NAME.get(),
        AUTH_MODE.get(),
        Optional.<InetSocketAddress>absent(),
        configModule,
        additional);
  }

  @Override
  public void run() {
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

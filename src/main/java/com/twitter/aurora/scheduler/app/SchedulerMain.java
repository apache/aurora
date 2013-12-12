/*
 * Copyright 2013 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.aurora.scheduler.app;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.logging.Logger;

import javax.annotation.Nonnegative;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.PrivateModule;

import com.twitter.aurora.auth.CapabilityValidator;
import com.twitter.aurora.auth.SessionValidator;
import com.twitter.aurora.auth.UnsecureAuthModule;
import com.twitter.aurora.scheduler.DriverFactory;
import com.twitter.aurora.scheduler.DriverFactory.DriverFactoryImpl;
import com.twitter.aurora.scheduler.MesosTaskFactory.ExecutorConfig;
import com.twitter.aurora.scheduler.SchedulerLifecycle;
import com.twitter.aurora.scheduler.SchedulerLifecycle.ShutdownOnDriverExit;
import com.twitter.aurora.scheduler.cron.CronPredictor;
import com.twitter.aurora.scheduler.cron.CronScheduler;
import com.twitter.aurora.scheduler.cron.noop.NoopCronModule;
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
import com.twitter.common.args.constraints.NotEmpty;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.common.inject.Bindings;
import com.twitter.common.logging.RootLogConfig;
import com.twitter.common.zookeeper.Group;
import com.twitter.common.zookeeper.SingletonService;
import com.twitter.common.zookeeper.guice.client.ZooKeeperClientModule;
import com.twitter.common.zookeeper.guice.client.ZooKeeperClientModule.ClientConfig;
import com.twitter.common.zookeeper.guice.client.flagged.FlaggedClientConfig;

/**
 * Launcher for the aurora scheduler.
 */
public class SchedulerMain extends AbstractApplication {

  private static final Logger LOG = Logger.getLogger(SchedulerMain.class.getName());

  @CmdLine(name = "testing_isolated_scheduler",
      help = "If true, run in a testing mode with the scheduler isolated from other components.")
  private static final Arg<Boolean> ISOLATED_SCHEDULER = Arg.create(false);

  @NotNull
  @CmdLine(name = "cluster_name", help = "Name to identify the cluster being served.")
  private static final Arg<String> CLUSTER_NAME = Arg.create();

  @NotNull
  @NotEmpty
  @CmdLine(name = "serverset_path", help = "ZooKeeper ServerSet path to register at.")
  private static final Arg<String> SERVERSET_PATH = Arg.create();

  @CmdLine(name = "mesos_ssl_keyfile",
      help = "JKS keyfile for operating the Mesos Thrift-over-SSL interface.")
  private static final Arg<File> MESOS_SSL_KEY_FILE = Arg.create();

  @Nonnegative
  @CmdLine(name = "thrift_port", help = "Thrift server port.")
  private static final Arg<Integer> THRIFT_PORT = Arg.create(0);

  @NotNull
  @CmdLine(name = "thermos_executor_path", help = "Path to the thermos executor launch script.")
  private static final Arg<String> THERMOS_EXECUTOR_PATH = Arg.create();

  @CmdLine(name = "auth_module",
      help = "A Guice module to provide auth bindings. NOTE: The default is unsecure.")
  private static final Arg<? extends Class<? extends Module>> AUTH_MODULE =
      Arg.create(UnsecureAuthModule.class);

  private static final Iterable<Class<?>> AUTH_MODULE_CLASSES = ImmutableList.<Class<?>>builder()
      .add(SessionValidator.class)
      .add(CapabilityValidator.class)
      .build();

  @CmdLine(name = "cron_module",
      help = "A Guice module to provide cron bindings. NOTE: The default is a no-op.")
  private static final Arg<? extends Class<? extends Module>> CRON_MODULE =
      Arg.create(NoopCronModule.class);

  private static final Iterable<Class<?>> CRON_MODULE_CLASSES = ImmutableList.<Class<?>>builder()
      .add(CronPredictor.class)
      .add(CronScheduler.class)
      .build();

  @Inject private SingletonService schedulerService;
  @Inject private LocalServiceRegistry serviceRegistry;
  @Inject private SchedulerLifecycle schedulerLifecycle;
  @Inject private Optional<RootLogConfig.Configuration> glogConfig;

  private static Iterable<? extends Module> getSystemModules() {
    return ImmutableList.of(
        new LogModule(),
        new HttpModule(),
        new StatsModule()
    );
  }

  // TODO(ksweeney): Consider factoring this out into a ModuleParser library.
  private static Module instantiateFlaggedModule(Arg<? extends Class<? extends Module>> moduleArg) {
    Class<? extends Module> moduleClass = moduleArg.get();
    try {
      return moduleClass.newInstance();
    } catch (InstantiationException e) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to instantiate module %s. Are you sure it has a no-arg constructor?",
              moduleClass.getName()),
          e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to instantiate module %s. Are you sure it's public?",
              moduleClass.getName()),
          e);
    }
  }

  // Defensively wrap each module provided on the command-line in a PrivateModule that only
  // exposes requested classes to ensure that we don't depend on surprise extra bindings across
  // different implementations.
  private static Module getFlaggedModule(
      Arg<? extends Class<? extends Module>> moduleArg,
      final Iterable<Class<?>> exposedClasses) {

    final Module module = instantiateFlaggedModule(moduleArg);
    return new PrivateModule() {
      @Override protected void configure() {
        install(module);
        for (Class<?> klass : exposedClasses) {
          expose(klass);
        }
      }
    };
  }

  private static Iterable<? extends Module> getFlaggedModules() {
    return ImmutableList.of(
        getFlaggedModule(AUTH_MODULE, AUTH_MODULE_CLASSES),
        getFlaggedModule(CRON_MODULE, CRON_MODULE_CLASSES));
  }

  static Iterable<? extends Module> getModules(
      String clusterName,
      String serverSetPath,
      ClientConfig zkClientConfig) {

    return ImmutableList.<Module>builder()
        .addAll(getFlaggedModules())
        .addAll(getSystemModules())
        .add(new AppModule(clusterName, serverSetPath, zkClientConfig))
        .add(new LogStorageModule())
        .add(new MemStorageModule(Bindings.annotatedKeyFactory(LogStorage.WriteBehind.class)))
        .add(new ThriftModule())
        .add(new ThriftAuthModule())
        .build();
  }

  @Override
  public Iterable<? extends Module> getModules() {
    Module additional;
    final ClientConfig zkClientConfig = FlaggedClientConfig.create();
    if (ISOLATED_SCHEDULER.get()) {
      additional = new IsolatedSchedulerModule();
    } else {
      // TODO(Kevin Sweeney): Push these bindings down into a "production" module.
      additional = new AbstractModule() {
        @Override protected void configure() {
          bind(DriverFactory.class).to(DriverFactoryImpl.class);
          bind(DriverFactoryImpl.class).in(Singleton.class);
          bind(Boolean.class).annotatedWith(ShutdownOnDriverExit.class).toInstance(true);
          install(new MesosLogStreamModule(zkClientConfig));
        }
      };
    }

    Module configModule = new AbstractModule() {
      @Override protected void configure() {
        bind(ThriftConfiguration.class).toInstance(new ThriftConfiguration() {
          @Override public Optional<InputStream> getSslKeyStream() throws FileNotFoundException {
            if (MESOS_SSL_KEY_FILE.hasAppliedValue()) {
              return Optional.<InputStream>of(new FileInputStream(MESOS_SSL_KEY_FILE.get()));
            } else {
              return Optional.absent();
            }
          }

          @Override public int getServingPort() {
            return THRIFT_PORT.get();
          }
        });
        bind(ExecutorConfig.class).toInstance(new ExecutorConfig(THERMOS_EXECUTOR_PATH.get()));
        bind(Boolean.class).annotatedWith(ShutdownOnDriverExit.class).toInstance(true);
      }
    };

    return ImmutableList.<Module>builder()
        .add(new BackupModule(SnapshotStoreImpl.class))
        .addAll(getModules(CLUSTER_NAME.get(), SERVERSET_PATH.get(), zkClientConfig))
        .add(new ZooKeeperClientModule(zkClientConfig))
        .add(configModule)
        .add(additional)
        .build();
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

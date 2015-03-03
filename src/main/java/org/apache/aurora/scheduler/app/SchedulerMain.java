/**
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
package org.apache.aurora.scheduler.app;

import java.net.InetSocketAddress;
import java.util.List;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.twitter.common.application.AbstractApplication;
import com.twitter.common.application.AppLauncher;
import com.twitter.common.application.Lifecycle;
import com.twitter.common.application.modules.StatsModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotEmpty;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.common.inject.Bindings;
import com.twitter.common.logging.RootLogConfig;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.common.zookeeper.Group;
import com.twitter.common.zookeeper.SingletonService;
import com.twitter.common.zookeeper.SingletonService.LeadershipListener;
import com.twitter.common.zookeeper.guice.client.ZooKeeperClientModule;
import com.twitter.common.zookeeper.guice.client.ZooKeeperClientModule.ClientConfig;
import com.twitter.common.zookeeper.guice.client.flagged.FlaggedClientConfig;

import org.apache.aurora.auth.CapabilityValidator;
import org.apache.aurora.auth.SessionValidator;
import org.apache.aurora.auth.UnsecureAuthModule;
import org.apache.aurora.gen.Volume;
import org.apache.aurora.scheduler.SchedulerLifecycle;
import org.apache.aurora.scheduler.configuration.Resources;
import org.apache.aurora.scheduler.cron.quartz.CronModule;
import org.apache.aurora.scheduler.log.mesos.MesosLogStreamModule;
import org.apache.aurora.scheduler.mesos.CommandLineDriverSettingsModule;
import org.apache.aurora.scheduler.mesos.ExecutorSettings;
import org.apache.aurora.scheduler.mesos.LibMesosLoadingModule;
import org.apache.aurora.scheduler.storage.backup.BackupModule;
import org.apache.aurora.scheduler.storage.db.DbModule;
import org.apache.aurora.scheduler.storage.db.MigrationModule;
import org.apache.aurora.scheduler.storage.log.LogStorage;
import org.apache.aurora.scheduler.storage.log.LogStorageModule;
import org.apache.aurora.scheduler.storage.log.SnapshotStoreImpl;
import org.apache.aurora.scheduler.storage.mem.MemStorage.Delegated;
import org.apache.aurora.scheduler.storage.mem.MemStorageModule;
import org.apache.aurora.scheduler.thrift.ThriftModule;
import org.apache.aurora.scheduler.thrift.auth.ThriftAuthModule;

import static com.twitter.common.logging.RootLogConfig.Configuration;

/**
 * Launcher for the aurora scheduler.
 */
public class SchedulerMain extends AbstractApplication {

  @NotNull
  @CmdLine(name = "cluster_name", help = "Name to identify the cluster being served.")
  private static final Arg<String> CLUSTER_NAME = Arg.create();

  @NotNull
  @NotEmpty
  @CmdLine(name = "serverset_path", help = "ZooKeeper ServerSet path to register at.")
  private static final Arg<String> SERVERSET_PATH = Arg.create();

  @CmdLine(name = "thermos_executor_path", help = "Path to the thermos executor entry point.")
  private static final Arg<String> THERMOS_EXECUTOR_PATH = Arg.create();

  @CmdLine(name = "thermos_executor_resources",
      help = "A comma seperated list of additional resources to copy into the sandbox."
          + "Note: if thermos_executor_path is not the thermos_executor.pex file itself, "
          + "this must include it.")
  private static final Arg<List<String>> THERMOS_EXECUTOR_RESOURCES =
      Arg.<List<String>>create(ImmutableList.<String>of());

  @CmdLine(name = "thermos_executor_flags",
      help = "Extra arguments to be passed to the thermos executor")
  private static final Arg<String> THERMOS_EXECUTOR_FLAGS = Arg.create(null);

  @CmdLine(name = "thermos_observer_root",
      help = "Path to the thermos observer root (by default /var/run/thermos.)")
  private static final Arg<String> THERMOS_OBSERVER_ROOT = Arg.create("/var/run/thermos");

  @CmdLine(name = "auth_module",
      help = "A Guice module to provide auth bindings. NOTE: The default is unsecure.")
  private static final Arg<? extends Class<? extends Module>> AUTH_MODULE =
      Arg.create(UnsecureAuthModule.class);

  /**
   * Extra CPU allocated for each executor.
   */
  @CmdLine(name = "thermos_executor_cpu",
      help = "The number of CPU cores to allocate for each instance of the executor.")
  private static final Arg<Double> EXECUTOR_OVERHEAD_CPUS = Arg.create(0.25);

  /**
   * Extra RAM allocated for the executor.
   */
  @CmdLine(name = "thermos_executor_ram",
      help = "The amount of RAM to allocate for each instance of the executor.")
  private static final Arg<Amount<Long, Data>> EXECUTOR_OVERHEAD_RAM =
      Arg.create(Amount.of(128L, Data.MB));

  private static final Iterable<Class<?>> AUTH_MODULE_CLASSES = ImmutableList.<Class<?>>builder()
      .add(SessionValidator.class)
      .add(CapabilityValidator.class)
      .build();

  // TODO(Suman Karumuri): Pass in AUTH as extra module
  @CmdLine(name = "extra_modules",
      help = "A list of modules that provide additional functionality.")
  private static final Arg<List<Class<? extends Module>>> EXTRA_MODULES =
      Arg.create((List<Class<? extends Module>>) ImmutableList.<Class<? extends Module>>of());

  // TODO(Suman Karumuri): Rename viz_job_url_prefix to stats_job_url_prefix for consistency.
  @CmdLine(name = "viz_job_url_prefix", help = "URL prefix for job container stats.")
  private static final Arg<String> STATS_URL_PREFIX = Arg.create("");

  @CmdLine(name = "hostname",
      help = "The hostname to advertise in ZooKeeper instead of the locally-resolved hostname.")
  private static final Arg<String> HOSTNAME_OVERRIDE = Arg.create(null);

  @CmdLine(name = "global_container_mounts",
      help = "A comma seperated list of mount points (in host:container form) to mount "
          + "into all (non-mesos) containers.")
  private static final Arg<List<Volume>> GLOBAL_CONTAINER_MOUNTS =
      Arg.<List<Volume>>create(ImmutableList.<Volume>of());

  @Inject private SingletonService schedulerService;
  @Inject private LocalServiceRegistryWithOverrides serviceRegistry;
  @Inject private SchedulerLifecycle schedulerLifecycle;
  @Inject private Lifecycle appLifecycle;

  private static Iterable<? extends Module> getExtraModules() {
    Builder<Module> modules = ImmutableList.builder();
    modules.add(Modules.wrapInPrivateModule(AUTH_MODULE.get(), AUTH_MODULE_CLASSES));

    for (Class<? extends Module> moduleClass : EXTRA_MODULES.get()) {
      modules.add(Modules.getModule(moduleClass));
    }

    return modules.build();
  }

  @VisibleForTesting
  Iterable<? extends Module> getModules(
      String clusterName,
      String serverSetPath,
      ClientConfig zkClientConfig,
      String statsURLPrefix,
      Optional<String> zkLocalDnsNameOverride) {

    return ImmutableList.<Module>builder()
        .add(new StatsModule())
        .add(new AppModule(
            clusterName,
            serverSetPath,
            zkClientConfig,
            statsURLPrefix,
            zkLocalDnsNameOverride))
        .addAll(getExtraModules())
        .add(getPersistentStorageModule())
        .add(new MemStorageModule(Bindings.annotatedKeyFactory(LogStorage.WriteBehind.class)))
        .add(new CronModule())
        .add(new DbModule(Bindings.annotatedKeyFactory(Delegated.class)))
        .add(new MigrationModule(
            Bindings.annotatedKeyFactory(LogStorage.WriteBehind.class),
            Bindings.annotatedKeyFactory(Delegated.class))
        )
        .add(new ThriftModule())
        .add(new ThriftAuthModule())
        .build();
  }

  protected Module getPersistentStorageModule() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        install(new LogStorageModule());
      }
    };
  }

  protected Module getMesosModules() {
    final ClientConfig zkClientConfig = FlaggedClientConfig.create();
    return new AbstractModule() {
      @Override
      protected void configure() {
        install(new CommandLineDriverSettingsModule());
        install(new LibMesosLoadingModule());
        install(new MesosLogStreamModule(zkClientConfig));
      }
    };
  }

  @Override
  public Iterable<? extends Module> getModules() {
    ClientConfig zkClientConfig = FlaggedClientConfig.create();
    return ImmutableList.<Module>builder()
        .add(new BackupModule(SnapshotStoreImpl.class))
        .addAll(
            getModules(
                CLUSTER_NAME.get(),
                SERVERSET_PATH.get(),
                zkClientConfig,
                STATS_URL_PREFIX.get(),
                Optional.fromNullable(HOSTNAME_OVERRIDE.get())))
        .add(new ZooKeeperClientModule(zkClientConfig))
        .add(new AbstractModule() {
          @Override
          protected void configure() {
            Resources executorOverhead = new Resources(
                EXECUTOR_OVERHEAD_CPUS.get(),
                EXECUTOR_OVERHEAD_RAM.get(),
                Amount.of(0L, Data.MB),
                0);

            bind(ExecutorSettings.class)
                .toInstance(ExecutorSettings.newBuilder()
                    .setExecutorPath(THERMOS_EXECUTOR_PATH.get())
                    .setExecutorResources(THERMOS_EXECUTOR_RESOURCES.get())
                    .setThermosObserverRoot(THERMOS_OBSERVER_ROOT.get())
                    .setExecutorFlags(Optional.fromNullable(THERMOS_EXECUTOR_FLAGS.get()))
                    .setExecutorOverhead(executorOverhead)
                    .setGlobalContainerMounts(GLOBAL_CONTAINER_MOUNTS.get())
                    .build());
          }
        })
        .add(getMesosModules())
        .build();
  }

  @Override
  public void run() {
    // Setup log4j to match our jul glog config in order to pick up zookeeper logging.
    Configuration logConfiguration = RootLogConfig.configurationFromFlags();
    logConfiguration.apply();
    Log4jConfigurator.configureConsole(logConfiguration);

    LeadershipListener leaderListener = schedulerLifecycle.prepare();

    Optional<InetSocketAddress> httpSocket =
        Optional.fromNullable(serviceRegistry.getAuxiliarySockets().get("http"));
    if (!httpSocket.isPresent()) {
      throw new IllegalStateException("No HTTP service registered with LocalServiceRegistry.");
    }

    try {
      schedulerService.lead(
          httpSocket.get(),
          serviceRegistry.getAuxiliarySockets(),
          leaderListener);
    } catch (Group.WatchException e) {
      throw new IllegalStateException("Failed to watch group and lead service.", e);
    } catch (Group.JoinException e) {
      throw new IllegalStateException("Failed to join scheduler service group.", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Interrupted while joining scheduler service group.", e);
    }

    appLifecycle.awaitShutdown();
  }

  public static void main(String[] args) {
    AppLauncher.launch(SchedulerMain.class, args);
  }
}

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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.inject.AbstractModule;
import com.google.inject.CreationException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.ProvisionException;
import com.google.inject.spi.Message;
import com.google.inject.util.Modules;

import org.apache.aurora.GuavaUtils.ServiceManagerIface;
import org.apache.aurora.common.application.Lifecycle;
import org.apache.aurora.common.inject.Bindings;
import org.apache.aurora.common.stats.Stats;
import org.apache.aurora.common.zookeeper.SingletonService;
import org.apache.aurora.common.zookeeper.SingletonService.LeadershipListener;
import org.apache.aurora.gen.ServerInfo;
import org.apache.aurora.scheduler.AppStartup;
import org.apache.aurora.scheduler.SchedulerLifecycle;
import org.apache.aurora.scheduler.TierModule;
import org.apache.aurora.scheduler.config.CliOptions;
import org.apache.aurora.scheduler.config.CommandLine;
import org.apache.aurora.scheduler.config.validators.NotEmptyString;
import org.apache.aurora.scheduler.configuration.executor.ExecutorModule;
import org.apache.aurora.scheduler.cron.quartz.CronModule;
import org.apache.aurora.scheduler.discovery.FlaggedZooKeeperConfig;
import org.apache.aurora.scheduler.discovery.ServiceDiscoveryModule;
import org.apache.aurora.scheduler.events.WebhookModule;
import org.apache.aurora.scheduler.http.HttpService;
import org.apache.aurora.scheduler.log.mesos.MesosLogStreamModule;
import org.apache.aurora.scheduler.mesos.CommandLineDriverSettingsModule;
import org.apache.aurora.scheduler.mesos.FrameworkInfoFactory.FrameworkInfoFactoryImpl.SchedulerProtocol;
import org.apache.aurora.scheduler.mesos.LibMesosLoadingModule;
import org.apache.aurora.scheduler.stats.StatsModule;
import org.apache.aurora.scheduler.storage.Storage.Volatile;
import org.apache.aurora.scheduler.storage.backup.BackupModule;
import org.apache.aurora.scheduler.storage.entities.IServerInfo;
import org.apache.aurora.scheduler.storage.log.LogStorageModule;
import org.apache.aurora.scheduler.storage.log.SnapshotStoreImpl;
import org.apache.aurora.scheduler.storage.mem.MemStorageModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Launcher for the aurora scheduler.
 */
public class SchedulerMain {
  private static final Logger LOG = LoggerFactory.getLogger(SchedulerMain.class);

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-cluster_name",
        required = true,
        description = "Name to identify the cluster being served.")
    public String clusterName;

    @Parameter(
        names = "-serverset_path",
        required = true,
        validateValueWith = NotEmptyString.class,
        description = "ZooKeeper ServerSet path to register at.")
    public String serversetPath;

    // TODO(zmanji): Consider making this an enum of HTTP or HTTPS.
    @Parameter(names = "-serverset_endpoint_name",
        description = "Name of the scheduler endpoint published in ZooKeeper.")
    public String serversetEndpointName = "http";

    // TODO(Suman Karumuri): Rename viz_job_url_prefix to stats_job_url_prefix for consistency.
    @Parameter(names = "-viz_job_url_prefix", description = "URL prefix for job container stats.")
    public String statsUrlPrefix = "";

    @Parameter(names = "-allow_gpu_resource",
        description = "Allow jobs to request Mesos GPU resource.",
        arity = 1)
    public boolean allowGpuResource = false;

    public enum DriverKind {
      // TODO(zmanji): Remove this option once V0_DRIVER has been proven out in production.
      // This is the original driver that libmesos shipped with. Uses unversioned protobufs, and has
      // minimal backwards compatability guarantees.
      SCHEDULER_DRIVER,
      // These are the new drivers that libmesos ships with. They use versioned (V1) protobufs for
      // the Java API.
      // V0 Driver offers the V1 API over the old Scheduler Driver. It does not fully support
      // the V1 API (ie mesos maintenance).
      V0_DRIVER,
      // V1 Driver offers the V1 API over a full HTTP API implementation. It allows for maintenance
      // primatives and other new features.
      V1_DRIVER,
    }

    @Parameter(names = "-mesos_driver", description = "Which Mesos Driver to use")
    public DriverKind driverImpl = DriverKind.SCHEDULER_DRIVER;
  }

  public static class ProtocolModule extends AbstractModule {
    private final Options options;

    public ProtocolModule(Options options) {
      this.options = options;
    }

    @Override
    protected void configure() {
      bind(String.class)
          .annotatedWith(SchedulerProtocol.class)
          .toInstance(options.serversetEndpointName);
    }
  }

  @Inject private SingletonService schedulerService;
  @Inject private HttpService httpService;
  @Inject private SchedulerLifecycle schedulerLifecycle;
  @Inject private Lifecycle appLifecycle;
  @Inject
  @AppStartup
  private ServiceManagerIface startupServices;

  private void stop() {
    LOG.info("Stopping scheduler services.");
    try {
      startupServices.stopAsync().awaitStopped(5L, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      LOG.info("Shutdown did not complete in time: " + e);
    }
    appLifecycle.shutdown();
  }

  void run(Options options) {
    try {
      startupServices.startAsync();
      Runtime.getRuntime().addShutdownHook(new Thread(SchedulerMain.this::stop, "ShutdownHook"));
      startupServices.awaitHealthy();

      LeadershipListener leaderListener = schedulerLifecycle.prepare();

      HostAndPort httpAddress = httpService.getAddress();
      InetSocketAddress httpSocketAddress =
          InetSocketAddress.createUnresolved(httpAddress.getHost(), httpAddress.getPort());

      schedulerService.lead(
          httpSocketAddress,
          ImmutableMap.of(options.serversetEndpointName, httpSocketAddress),
          leaderListener);
    } catch (SingletonService.LeadException e) {
      throw new IllegalStateException("Failed to lead service.", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Interrupted while joining scheduler service group.", e);
    } finally {
      appLifecycle.awaitShutdown();
      stop();
    }
  }

  @VisibleForTesting
  static Module getUniversalModule(CliOptions options) {
    return Modules.combine(
        new ProtocolModule(options.main),
        new LifecycleModule(),
        new StatsModule(options.stats),
        new AppModule(options),
        new CronModule(options.cron),
        new MemStorageModule(Bindings.annotatedKeyFactory(Volatile.class)));
  }

  /**
   * Runs the scheduler by including modules configured from command line arguments in
   * addition to the provided environment-specific module.
   *
   * @param appEnvironmentModule Additional modules based on the execution environment.
   */
  @VisibleForTesting
  public static void flagConfiguredMain(CliOptions options, Module appEnvironmentModule) {
    AtomicLong uncaughtExceptions = Stats.exportLong("uncaught_exceptions");
    Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
      uncaughtExceptions.incrementAndGet();

      if (e instanceof CreationException) {
        try {
          LOG.error("Uncaught exception from " + t + ":" + e, e);
        } catch (RuntimeException ex) {
          LOG.warn("Using fallback printer for guice CreationException");

          // Special handling for guice creation exceptions, which break in guice 3 when java 8
          // lambdas are used.  Remove this once using guice >=4.0.
          CreationException creationException = (CreationException) e;
          for (Message m : creationException.getErrorMessages()) {
            LOG.error(m.getMessage());
            LOG.error("  source: " + m.getSource());
          }
        }
      } else if (e instanceof ProvisionException) {
        // More special handling for guice 3 + java 8.  Remove this once using guice >=4.0.
        ProvisionException pe = (ProvisionException) e;
        for (Message message : pe.getErrorMessages()) {
          LOG.error(message.getMessage());
        }
      } else {
        LOG.error("Uncaught exception from " + t + ":" + e, e);
      }
    });

    Module module = Modules.combine(
        appEnvironmentModule,
        getUniversalModule(options),
        new ServiceDiscoveryModule(
            FlaggedZooKeeperConfig.create(options.zk),
            options.main.serversetPath),
        new BackupModule(options.backup, SnapshotStoreImpl.class),
        new ExecutorModule(options.executor),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(CliOptions.class).toInstance(options);
            bind(IServerInfo.class).toInstance(
                IServerInfo.build(
                    new ServerInfo()
                        .setClusterName(options.main.clusterName)
                        .setStatsUrlPrefix(options.main.statsUrlPrefix)));
          }
        });

    Lifecycle lifecycle = null;
    try {
      Injector injector = Guice.createInjector(module);
      lifecycle = injector.getInstance(Lifecycle.class);
      SchedulerMain scheduler = new SchedulerMain();
      injector.injectMembers(scheduler);
      try {
        scheduler.run(options.main);
      } finally {
        LOG.info("Application run() exited.");
      }
    } finally {
      if (lifecycle != null) {
        lifecycle.shutdown();
      }
    }
  }

  public static void main(String... args) {
    CliOptions options = CommandLine.parseOptions(args);

    List<Module> modules = ImmutableList.<Module>builder()
        .add(
            new CommandLineDriverSettingsModule(options.driver, options.main.allowGpuResource),
            new LibMesosLoadingModule(options.main.driverImpl),
            new MesosLogStreamModule(options.mesosLog, FlaggedZooKeeperConfig.create(options.zk)),
            new LogStorageModule(options.logStorage),
            new TierModule(options.tiers),
            new WebhookModule(options.webhook)
        )
        .build();
    flagConfiguredMain(options, Modules.combine(modules));
  }
}

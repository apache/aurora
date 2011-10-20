package com.twitter.mesos.scheduler;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.zookeeper.data.ACL;

import com.twitter.common.application.http.Registration;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.common.base.Closure;
import com.twitter.common.base.Closures;
import com.twitter.common.inject.TimedInterceptor;
import com.twitter.common.logging.ScribeLog;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.common.quantity.Time;
import com.twitter.common.thrift.ThriftFactory.ThriftFactoryException;
import com.twitter.common.thrift.ThriftServer;
import com.twitter.common.util.Clock;
import com.twitter.common.zookeeper.SingletonService;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common.zookeeper.ZooKeeperUtils;
import com.twitter.common_internal.cuckoo.CuckooWriter;
import com.twitter.common_internal.zookeeper.ZooKeeperModule;
import com.twitter.mesos.ExecutorKey;
import com.twitter.mesos.gen.MesosAdmin;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.PulseMonitor.PulseMonitorImpl;
import com.twitter.mesos.scheduler.SchedulingFilter.SchedulingFilterImpl;
import com.twitter.mesos.scheduler.auth.SessionValidator;
import com.twitter.mesos.scheduler.auth.SessionValidator.SessionValidatorImpl;
import com.twitter.mesos.scheduler.httphandlers.CreateJob;
import com.twitter.mesos.scheduler.httphandlers.Mname;
import com.twitter.mesos.scheduler.httphandlers.SchedulerzHome;
import com.twitter.mesos.scheduler.httphandlers.SchedulerzJob;
import com.twitter.mesos.scheduler.httphandlers.SchedulerzRole;
import com.twitter.mesos.scheduler.quota.QuotaModule;
import com.twitter.mesos.scheduler.storage.StorageModule;
import com.twitter.mesos.scheduler.sync.SyncModule;

/**
 * Binding module for the twitter mesos scheduler.
 */
public class SchedulerModule extends AbstractModule {
  private static final Logger LOG = Logger.getLogger(SchedulerModule.class.getName());

  @NotNull
  @CmdLine(name = "mesos_scheduler_ns",
      help ="The name service name for the mesos scheduler thrift server.")
  private static final Arg<String> MESOS_SCHEDULER_NAME_SPEC = Arg.create();

  @CmdLine(name = "machine_restrictions",
      help ="Map of machine hosts to job keys."
              + "  If A maps to B, only B can run on A and B can only run on A.")
  public static final Arg<Map<String, String>> MACHINE_RESTRICTIONS =
      Arg.create(Collections.<String, String>emptyMap());

  @NotNull
  @CmdLine(name = "mesos_master_address",
          help ="Mesos address for the master, can be a mesos address or zookeeper path.")
  private static final Arg<String> MESOS_MASTER_ADDRESS = Arg.create();

  @NotNull
  @CmdLine(name = "executor_path", help ="Path to the executor launch script.")
  private static final Arg<String> EXECUTOR_PATH = Arg.create();

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

  @NotNull
  @CmdLine(name = "cluster_name", help = "Name to identify the cluster being served.")
  private static final Arg<String> CLUSTER_NAME = Arg.create();

  @CmdLine(name = "executor_resources_cpus",
      help = "The number of CPUS that should be reserved by mesos for the executor.")
  private static final Arg<Double> EXECUTOR_CPUS = Arg.create(0.25);

  @CmdLine(name = "executor_resources_ram",
      help = "The amount of RAM that should be reserved by mesos for the executor.")
  private static final Arg<Amount<Double, Data>> EXECUTOR_RAM = Arg.create(Amount.of(2d, Data.GB));

  private static final String TWITTER_EXECUTOR_ID = "twitter";

  private static final String TWITTER_FRAMEWORK_NAME = "TwitterScheduler";

  @Override
  protected void configure() {
    // Enable intercepted method timings
    TimedInterceptor.bind(binder());

    // Bind a ZooKeeperClient
    install(new ZooKeeperModule(
        ZooKeeperClient.digestCredentials("mesos", "mesos"),
        ZooKeeperUtils.EVERYONE_READ_CREATOR_ALL));

    bind(Key.get(String.class, ClusterName.class)).toInstance(CLUSTER_NAME.get());

    // Bindings for MesosSchedulerImpl.
    bind(SessionValidator.class).to(SessionValidatorImpl.class);
    bind(SchedulerCore.class).to(SchedulerCoreImpl.class).in(Singleton.class);
    bind(ExecutorTracker.class).to(ExecutorTrackerImpl.class).in(Singleton.class);

    // Bindings for SchedulerCoreImpl.
    bind(CronJobManager.class).in(Singleton.class);
    bind(ImmediateJobManager.class).in(Singleton.class);
    bind(new TypeLiteral<PulseMonitor<ExecutorKey>>() {})
        .toInstance(new PulseMonitorImpl<ExecutorKey>(EXECUTOR_DEAD_THRESHOLD.get()));
    bind(new TypeLiteral<Supplier<Set<ExecutorKey>>>() {})
        .to(new TypeLiteral<PulseMonitor<ExecutorKey>>() {});

    // Bindings for thrift interfaces.
    bind(MesosAdmin.Iface.class).to(SchedulerThriftInterface.class).in(Singleton.class);
    bind(ThriftServer.class).to(SchedulerThriftServer.class).in(Singleton.class);

    install(new StorageModule());

    bind(SchedulingFilter.class).to(SchedulingFilterImpl.class);

    // updaterTaskProvider handled in provider.

    // Bindings for SchedulingFilterImpl.
    bind(Key.get(new TypeLiteral<Map<String, String>>() {},
        Names.named(SchedulingFilterImpl.MACHINE_RESTRICTIONS)))
        .toInstance(MACHINE_RESTRICTIONS.get());
    bind(Scheduler.class).to(MesosSchedulerImpl.class).in(Singleton.class);

    // Bindings for StateManager
    bind(Clock.class).toInstance(Clock.SYSTEM_CLOCK);
    bind(StateManager.class).in(Singleton.class);

    bind(TaskReaper.class).in(Singleton.class);

    Registration.registerServlet(binder(), "/scheduler", SchedulerzHome.class, false);
    Registration.registerServlet(binder(), "/scheduler/role", SchedulerzRole.class, true);
    Registration.registerServlet(binder(), "/scheduler/job", SchedulerzJob.class, true);
    Registration.registerServlet(binder(), "/mname", Mname.class, false);
    Registration.registerServlet(binder(), "/create_job", CreateJob.class, true);

    LifecycleModule.bindServiceLauncher(binder(), ThriftServerLauncher.THRIFT_PORT_NAME,
        ThriftServerLauncher.class);

    QuotaModule.bind(binder());
    SyncModule.bind(binder());
  }

  @Provides
  @Singleton
  SchedulerDriver provideMesosSchedulerDriver(Scheduler scheduler, SchedulerCore schedulerCore) {
    String frameworkId = schedulerCore.initialize();
    LOG.info("Connecting to mesos master: " + MESOS_MASTER_ADDRESS.get());

    if (frameworkId != null) {
      LOG.info("Found persisted framework ID: " + frameworkId);
      return new MesosSchedulerDriver(scheduler, TWITTER_FRAMEWORK_NAME, provideExecutorInfo(),
          MESOS_MASTER_ADDRESS.get(), FrameworkID.newBuilder().setValue(frameworkId).build());
    } else {
      LOG.warning("Did not find a persisted framework ID, connecting as a new framework.");
      return new MesosSchedulerDriver(scheduler, TWITTER_FRAMEWORK_NAME, provideExecutorInfo(),
          MESOS_MASTER_ADDRESS.get());
    }
  }

  @Provides
  @Singleton
  SingletonService provideSingletonService(ZooKeeperClient zkClient, List<ACL> acl) {
    return new SingletonService(zkClient, MESOS_SCHEDULER_NAME_SPEC.get(), acl);
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

  @Provides
  @Singleton
  ExecutorInfo provideExecutorInfo() {
    return ExecutorInfo.newBuilder().setUri(EXECUTOR_PATH.get())
        .setExecutorId(ExecutorID.newBuilder().setValue(TWITTER_EXECUTOR_ID))
        .addResources(Resources.makeMesosResource(Resources.CPUS, EXECUTOR_CPUS.get()))
        .addResources(Resources.makeMesosResource(Resources.RAM_MB, EXECUTOR_RAM.get().as(Data.MB)))
        .build();
  }

  @Provides
  @Singleton
  Function<TwitterTaskInfo, TwitterTaskInfo> provideExecutorResourceAugmenter() {
    final Double executorCpus = EXECUTOR_CPUS.get();
    final long executorRam = EXECUTOR_RAM.get().as(Data.MB).longValue();
    return new Function<TwitterTaskInfo, TwitterTaskInfo>() {
      @Override public TwitterTaskInfo apply(TwitterTaskInfo task) {
        return task.deepCopy()
            .setNumCpus(task.getNumCpus() + executorCpus)
            .setRamMb(task.getRamMb() + executorRam);
      }
    };
  }
}

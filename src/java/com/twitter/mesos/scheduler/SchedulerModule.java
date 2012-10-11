package com.twitter.mesos.scheduler;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;

import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.zookeeper.data.ACL;

import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.common.base.Command;
import com.twitter.common.inject.TimedInterceptor;
import com.twitter.common.net.pool.DynamicHostSet;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.thrift.ThriftServer;
import com.twitter.common.util.BackoffStrategy;
import com.twitter.common.util.Clock;
import com.twitter.common.util.TruncatedBinaryBackoff;
import com.twitter.common.zookeeper.ServerSetImpl;
import com.twitter.common.zookeeper.SingletonService;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common.zookeeper.ZooKeeperUtils;
import com.twitter.common_internal.zookeeper.ZooKeeperModule;
import com.twitter.mesos.GuiceUtils;
import com.twitter.mesos.auth.AuthBindings;
import com.twitter.mesos.scheduler.BackoffSchedulingFilter.BackoffDelegate;
import com.twitter.mesos.scheduler.CronJobManager.CronScheduler;
import com.twitter.mesos.scheduler.CronJobManager.CronScheduler.Cron4jScheduler;
import com.twitter.mesos.scheduler.Driver.DriverImpl;
import com.twitter.mesos.scheduler.DriverFactory.DriverFactoryImpl;
import com.twitter.mesos.scheduler.MesosSchedulerImpl.SlaveHosts;
import com.twitter.mesos.scheduler.MesosSchedulerImpl.SlaveHostsImpl;
import com.twitter.mesos.scheduler.MesosSchedulerImpl.SlaveMapper;
import com.twitter.mesos.scheduler.MesosTaskFactory.MesosTaskFactoryImpl;
import com.twitter.mesos.scheduler.PulseMonitor.PulseMonitorImpl;
import com.twitter.mesos.scheduler.RegisteredListener.FanoutRegisteredListener;
import com.twitter.mesos.scheduler.ScheduleBackoff.ScheduleBackoffImpl;
import com.twitter.mesos.scheduler.ScheduleBackoff.ScheduleBackoffImpl.Backoff;
import com.twitter.mesos.scheduler.SchedulerLifecycle.DriverReference;
import com.twitter.mesos.scheduler.StateManagerVars.MutableState;
import com.twitter.mesos.scheduler.async.AsyncModule;
import com.twitter.mesos.scheduler.events.TaskEventModule;
import com.twitter.mesos.scheduler.httphandlers.ServletModule;
import com.twitter.mesos.scheduler.log.mesos.MesosLogStreamModule;
import com.twitter.mesos.scheduler.metadata.MetadataModule;
import com.twitter.mesos.scheduler.periodic.BootstrapTaskLauncher;
import com.twitter.mesos.scheduler.periodic.BootstrapTaskLauncher.Bootstrap;
import com.twitter.mesos.scheduler.periodic.GcExecutorLauncher;
import com.twitter.mesos.scheduler.periodic.GcExecutorLauncher.GcExecutor;
import com.twitter.mesos.scheduler.periodic.PeriodicTaskModule;
import com.twitter.mesos.scheduler.quota.QuotaModule;
import com.twitter.mesos.scheduler.storage.AttributeStore;
import com.twitter.mesos.scheduler.storage.AttributeStore.AttributeStoreImpl;
import com.twitter.mesos.scheduler.storage.log.LogStorageModule;
import com.twitter.mesos.scheduler.testing.IsolatedSchedulerModule;
import com.twitter.thrift.ServiceInstance;

/**
 * Binding module for the twitter mesos scheduler.
 */
public class SchedulerModule extends AbstractModule {
  private static final Logger LOG = Logger.getLogger(SchedulerModule.class.getName());

  @NotNull
  @CmdLine(name = "mesos_scheduler_ns",
      help = "The name service name for the mesos scheduler thrift server.")
  private static final Arg<String> MESOS_SCHEDULER_NAME_SPEC = Arg.create();

  @CmdLine(name = "executor_dead_threashold", help =
      "Time after which the scheduler will consider an executor dead and attempt to revive it.")
  private static final Arg<Amount<Long, Time>> EXECUTOR_DEAD_THRESHOLD =
      Arg.create(Amount.of(10L, Time.MINUTES));

  @CmdLine(name = "executor_gc_interval",
      help = "Interval on which to run the GC executor on a host to clean up dead tasks.")
  private static final Arg<Amount<Long, Time>> EXECUTOR_GC_INTERVAL =
      Arg.create(Amount.of(1L, Time.HOURS));

  @NotNull
  @CmdLine(name = "cluster_name", help = "Name to identify the cluster being served.")
  private static final Arg<String> CLUSTER_NAME = Arg.create();

  @CmdLine(name = "gc_executor_path", help = "Path to the gc executor launch script.")
  private static final Arg<String> GC_EXECUTOR_PATH = Arg.create(null);

  @CmdLine(name = "testing_isolated_scheduler",
      help = "If true, run in a testing mode with the scheduler isolated from other components.")
  private static final Arg<Boolean> ISOLATED_SCHEDULER = Arg.create(false);

  @CmdLine(name = "auth_mode", help = "Enforces RPC authentication with mesos client.")
  private static final Arg<AuthMode> AUTH_MODE = Arg.create(AuthMode.SECURE);

  @CmdLine(name = "initial_task_reschedule_backoff",
      help = "Initial backoff delay for a rescheduled task.")
  private static final Arg<Amount<Long, Time>> INITIAL_RESCHEDULE_BACKOFF =
      Arg.create(Amount.of(10L, Time.SECONDS));

  @CmdLine(name = "max_task_reschedule_backoff",
      help = "Maximum backoff delay for a rescheduled task.")
  private static final Arg<Amount<Long, Time>> MAX_RESCHEDULE_BACKOFF =
      Arg.create(Amount.of(2L, Time.MINUTES));

  private enum AuthMode {
    UNSECURE,
    ANGRYBIRD_UNSECURE,
    SECURE
  }

  @Override
  protected void configure() {
    // Enable intercepted method timings and context classloader repair.
    TimedInterceptor.bind(binder());
    GuiceUtils.bindJNIContextClassLoader(binder(), Scheduler.class);
    GuiceUtils.bindExceptionTrap(binder(), Scheduler.class);

    // Bind a ZooKeeperClient
    install(
        ZooKeeperModule.flaggedHostsBuilder()
            .withFlagOverrides()
            .withDigestCredentials("mesos", "mesos")
            .withAcl(ZooKeeperUtils.EVERYONE_READ_CREATOR_ALL)
            .build());

    bind(Key.get(String.class, ClusterName.class)).toInstance(CLUSTER_NAME.get());

    bind(Driver.class).to(DriverImpl.class);
    bind(DriverImpl.class).in(Singleton.class);

    bind(new TypeLiteral<Supplier<Optional<SchedulerDriver>>>() { }).to(DriverReference.class);
    bind(DriverReference.class).in(Singleton.class);

    bind(MesosTaskFactory.class).to(MesosTaskFactoryImpl.class);

    // Bindings for MesosSchedulerImpl.
    switch(AUTH_MODE.get()) {
      case SECURE:
        LOG.info("Using secure authentication mode");
        AuthBindings.bindLdapAuth(binder());
        break;

      case UNSECURE:
        LOG.warning("Using unsecure authentication mode");
        AuthBindings.bindTestAuth(binder());
        break;

      case ANGRYBIRD_UNSECURE:
        LOG.warning("Using angrybird authentication mode");
        AuthBindings.bindAngryBirdAuth(binder());
        break;

       default:
         throw new IllegalArgumentException("Invalid authentication mode: " + AUTH_MODE.get());
    }

    bind(SchedulerCore.class).to(SchedulerCoreImpl.class).in(Singleton.class);

    bind(new TypeLiteral<Optional<String>>() { }).annotatedWith(GcExecutor.class)
        .toInstance(Optional.fromNullable(GC_EXECUTOR_PATH.get()));
    bind(new TypeLiteral<PulseMonitor<String>>() { })
        .annotatedWith(GcExecutor.class)
        .toInstance(new PulseMonitorImpl<String>(EXECUTOR_GC_INTERVAL.get()));

    bind(CronScheduler.class).to(Cron4jScheduler.class);

    // Bindings for SchedulerCoreImpl.
    bind(CronJobManager.class).in(Singleton.class);
    bind(ImmediateJobManager.class).in(Singleton.class);
    bind(new TypeLiteral<PulseMonitor<String>>() { })
        .annotatedWith(Bootstrap.class)
        .toInstance(new PulseMonitorImpl<String>(EXECUTOR_DEAD_THRESHOLD.get()));

    // Bindings for thrift interfaces.
    LoggingThriftInterface.bind(binder(), SchedulerThriftInterface.class);
    bind(SchedulerThriftInterface.class).in(Singleton.class);
    bind(ThriftServer.class).to(SchedulerThriftServer.class).in(Singleton.class);

    if (ISOLATED_SCHEDULER.get()) {
      install(new IsolatedSchedulerModule());
    } else {
      MesosLogStreamModule.bind(binder());
      LogStorageModule.bind(binder());
      bind(DriverFactory.class).to(DriverFactoryImpl.class);
      bind(DriverFactoryImpl.class).in(Singleton.class);
    }

    bind(new TypeLiteral<Amount<Long, Time>>() { }).annotatedWith(Backoff.class)
        .toInstance(MAX_RESCHEDULE_BACKOFF.get());
    bind(BackoffStrategy.class).toInstance(
        new TruncatedBinaryBackoff(INITIAL_RESCHEDULE_BACKOFF.get(), MAX_RESCHEDULE_BACKOFF.get()));
    bind(ScheduleBackoff.class).to(ScheduleBackoffImpl.class);
    bind(ScheduleBackoffImpl.class).in(Singleton.class);
    TaskEventModule.bindSubscriber(binder(), ScheduleBackoff.class);

    // Filter layering: notifier filter -> backoff filter -> base impl
    TaskEventModule.bind(binder(), BackoffSchedulingFilter.class);
    bind(SchedulingFilter.class).annotatedWith(BackoffDelegate.class)
        .to(SchedulingFilterImpl.class);
    bind(SchedulingFilterImpl.class).in(Singleton.class);

    install(new MetadataModule());

    // updaterTaskProvider handled in provider.

    bind(SlaveHosts.class).to(SlaveHostsImpl.class);
    bind(SlaveMapper.class).to(SlaveHostsImpl.class);
    bind(SlaveHostsImpl.class).in(Singleton.class);
    bind(Scheduler.class).to(MesosSchedulerImpl.class);
    bind(MesosSchedulerImpl.class).in(Singleton.class);

    // Bindings for StateManager
    bind(StateManager.class).to(StateManagerImpl.class);
    bind(MutableState.class).in(Singleton.class);
    bind(Clock.class).toInstance(Clock.SYSTEM_CLOCK);
    bind(StateManagerImpl.class).in(Singleton.class);

    LifecycleModule.bindServiceRunner(binder(), ThriftServerLauncher.class);
    LifecycleModule.bindStartupAction(binder(), RegisterShutdownStackPrinter.class);

    bind(SchedulerLifecycle.class).in(Singleton.class);
    bind(AttributeStore.Mutable.class).to(AttributeStoreImpl.class);
    bind(AttributeStoreImpl.class).in(Singleton.class);

    QuotaModule.bind(binder());
    PeriodicTaskModule.bind(binder());

    install(new ServletModule());

    Multibinder<RegisteredListener> registeredListeners =
        Multibinder.newSetBinder(binder(), RegisteredListener.class);
    registeredListeners.addBinding().to(SchedulerLifecycle.class);
    registeredListeners.addBinding().to(SchedulerCore.class);

    bind(RegisteredListener.class).to(FanoutRegisteredListener.class);
    bind(FanoutRegisteredListener.class).in(Singleton.class);

    bind(BootstrapTaskLauncher.class).in(Singleton.class);
    bind(GcExecutorLauncher.class).in(Singleton.class);
    bind(UserTaskLauncher.class).in(Singleton.class);

    install(new AsyncModule());
  }

  /**
   * Command to register a thread stack printer that identifies initiator of a shutdown.
   */
  private static class RegisterShutdownStackPrinter implements Command {
    private static final Function<StackTraceElement, String> STACK_ELEM_TOSTRING =
        new Function<StackTraceElement, String>() {
          @Override public String apply(StackTraceElement element) {
            return element.getClassName() + "." + element.getMethodName()
                + String.format("(%s:%s)", element.getFileName(), element.getLineNumber());
          }
        };

    private final ShutdownRegistry shutdownRegistry;

    @Inject
    RegisterShutdownStackPrinter(ShutdownRegistry shutdownRegistry) {
      this.shutdownRegistry = shutdownRegistry;
    }

    @Override
    public void execute() {
      shutdownRegistry.addAction(new Command() {
        @Override public void execute() {
          Thread thread = Thread.currentThread();
          String message = new StringBuilder()
              .append("Thread: ").append(thread.getName())
              .append(" (id ").append(thread.getId()).append(")")
              .append("\n")
              .append(Joiner.on("\n  ").join(
                  Iterables.transform(Arrays.asList(thread.getStackTrace()), STACK_ELEM_TOSTRING)))
              .toString();

          LOG.info("Shutdown initiated by: " + message);
        }
      });
    }
  }

  @Provides
  @Singleton
  SingletonService provideSingletonService(ZooKeeperClient zkClient, List<ACL> acl) {
    return new SingletonService(zkClient, MESOS_SCHEDULER_NAME_SPEC.get(), acl);
  }

  @Provides
  @Singleton
  DynamicHostSet<ServiceInstance> provideSchedulerHostSet(ZooKeeperClient zkClient, List<ACL> acl) {
    return new ServerSetImpl(zkClient, acl, MESOS_SCHEDULER_NAME_SPEC.get());
  }

  @Provides
  @Singleton
  List<TaskLauncher> provideTaskLaunchers(
      BootstrapTaskLauncher bootstrapLauncher,
      GcExecutorLauncher gcLauncher,
      UserTaskLauncher userTaskLauncher) {

    return ImmutableList.of(bootstrapLauncher, gcLauncher, userTaskLauncher);
  }
}

package com.twitter.mesos.executor;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mesos.Executor;

import com.twitter.common.application.http.Registration;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.CanRead;
import com.twitter.common.args.constraints.Exists;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.common.base.ExceptionalClosure;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.common.inject.TimedInterceptor;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.Clock;
import com.twitter.mesos.GuiceUtils;
import com.twitter.mesos.executor.Driver.DriverImpl;
import com.twitter.mesos.executor.FileCopier.HdfsFileCopier;
import com.twitter.mesos.executor.FileDeleter.FileDeleterImpl;
import com.twitter.mesos.executor.FileToInt.FetchException;
import com.twitter.mesos.executor.HealthChecker.HealthCheckException;
import com.twitter.mesos.executor.HttpSignaler.SignalException;
import com.twitter.mesos.executor.ProcessKiller.KillCommand;
import com.twitter.mesos.executor.ProcessKiller.KillException;
import com.twitter.mesos.executor.httphandlers.ExecutorHome;
import com.twitter.mesos.executor.httphandlers.TaskHome;
import com.twitter.mesos.executor.migrations.DeadTaskMigratorModule;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduleStatus;

/**
 * ExecutorModule
 *
 * @author Florian Leibert
 */
public class ExecutorModule extends AbstractModule {
  private static final Logger LOG = Logger.getLogger(ExecutorModule.class.getName());

  @NotNull
  @Exists
  @CmdLine(name = "task_root_dir", help = "Mesos task working directory root.")
  private static final Arg<File> TASK_ROOT_DIR = Arg.create();

  @CmdLine(name = "multi_user", help = "True to execute tasks as the job owner")
  private static final Arg<Boolean> MULTI_USER_MODE = Arg.create(true);

  @NotNull
  @CmdLine(name = "hdfs_config", help = "Hadoop configuration path")
  private static final Arg<String> HDFS_CONFIG = Arg.create();

  @CmdLine(name = "http_signal_timeout", help = "Timeout for HTTP signals to tasks.")
  private static final Arg<Amount<Long, Time>> HTTP_SIGNAL_TIMEOUT =
      Arg.create(Amount.of(1L, Time.SECONDS));

  @Exists
  @CanRead
  @CmdLine(name = "kill_tree_path", help = "Path to kill tree shell script")
  private static final Arg<File> KILLTREE_PATH =
      Arg.create(new File("/usr/local/libexec/mesos/killtree.sh"));

  @CmdLine(name = "kill_escalation_delay_ms",
      help = "Time to wait before escalating between task kill procedures.")
  private static final Arg<Amount<Long, Time>> KILL_ESCALATION_DELAY =
      Arg.create(Amount.of(5L, Time.SECONDS));

  @CmdLine(name = "process_scanner_task_schedule_interval",
      help = "the schedule interval of executing process scanner task, use to check tasks" +
          " using unallocated port and kill orphan tasks")
  private static final Arg<Amount<Integer, Time>> PROCESS_SCANNER_TASK_SCHEDULE_INTERVAL =
      Arg.create(Amount.of(2, Time.MINUTES));

  @CmdLine(name = "valid_task_port_range", help = "The port range that can assign to tasks.")
  private static final Arg<Range<Integer>> TASK_PORT_RANGE =
      Arg.create(Ranges.closed(31000, 32000));

  @Override
  protected void configure() {
    // Enable intercepted method timings and context classloader repair.
    TimedInterceptor.bind(binder());
    GuiceUtils.bindJNIContextClassLoader(binder());

    bind(Clock.class).toInstance(Clock.SYSTEM_CLOCK);

    // Bindings needed for ExecutorMain.
    bind(File.class).annotatedWith(ExecutorRootDir.class).toInstance(TASK_ROOT_DIR.get());
    bind(Executor.class).to(MesosExecutorImpl.class).in(Singleton.class);
    bind(DriverRunner.class).in(Singleton.class);
    bind(TaskManager.class).to(ExecutorCore.class);
    bind(ExecutorCore.class).in(Singleton.class);
    bind(new TypeLiteral<Supplier<Iterable<Task>>>() {})
        .to(DeadTaskLoader.class).in(Singleton.class);

    // Bindings for MesosExecutorImpl.
    bind(new TypeLiteral<Supplier<Map<String, ScheduleStatus>>>() {}).to(ExecutorCore.class);
    bind(Driver.class).to(DriverImpl.class);
    bind(DriverImpl.class).in(Singleton.class);

    // Bindings needed for ExecutorCore.
    bind(new TypeLiteral<Function<AssignedTask, Task>>() {}).to(TaskFactory.class);
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("MesosExecutor-[%d]").build();
    bind(Key.get(ExecutorService.class, Names.named(ExecutorCore.TASK_EXECUTOR)))
        .toInstance(Executors.newCachedThreadPool(threadFactory));
    bind(new TypeLiteral<Function<Message, Integer>>() {}).to(DriverImpl.class);
    bind(new TypeLiteral<Amount<Integer, Time>>() {})
        .annotatedWith(Names.named(ExecutorCore.PROCESS_SCANNER_TASK_SCHEDULE_INTERVAL))
        .toInstance(PROCESS_SCANNER_TASK_SCHEDULE_INTERVAL.get());
    bind(new TypeLiteral<Range<Integer>>() {})
        .annotatedWith(Names.named(ExecutorCore.TASK_PORT_RANGE))
        .toInstance(TASK_PORT_RANGE.get());
    bind(FileDeleter.class).to(FileDeleterImpl.class);

    // Bindings needed for TaskFactory.
    bind(new TypeLiteral<ExceptionalFunction<Integer, Boolean, HealthCheckException>>() {})
        .to(HealthChecker.class);
    // processKiller handled in provider method.
    bind(new TypeLiteral<ExceptionalFunction<File, Integer, FetchException>>() {})
        .to(FileToInt.class);
    bind(FileCopier.class).to(HdfsFileCopier.class);
    bind(FileCopier.HdfsFileCopier.class).in(Singleton.class);
    bind(Key.get(boolean.class, MultiUserMode.class)).toInstance(MULTI_USER_MODE.get());

    // Bindings needed for HealthChecker.
    ThreadFactory httpSignalThreadFactory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("HTTP-signaler-%d")
        .build();
    bind(new TypeLiteral<ExceptionalFunction<String, List<String>, SignalException>>() {})
        .toInstance(new HttpSignaler(Executors.newCachedThreadPool(httpSignalThreadFactory),
            HTTP_SIGNAL_TIMEOUT.get()));

    // Bindings needed for FileCopier
    try {
      bind(Configuration.class).toInstance(getHdfsConfiguration(HDFS_CONFIG.get()));
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to create HDFS fileSystem.", e);
      Throwables.propagate(e);
    }

    // Binds a migrator for dead task serialized structs
    install(new DeadTaskMigratorModule());

    Registration.registerServlet(binder(), "/task", TaskHome.class, false);
    Registration.registerServlet(binder(), "/executor", ExecutorHome.class, false);

    install(new DiskScannerModule());
  }

  /**
   * A helper function to construct a Configuration. The code is directly copied from
   * HdfsUtils.getHdfsConfiguration().
   * TODO(vinod): This is a temporary fix for MESOS-514..
   * Use HdfsUtils.getHdfsConfiguration() instead, when smf1 is migrated to cdh3.
   */
  private static Configuration getHdfsConfiguration(String hdfsConfigPath) throws IOException {
    Configuration conf = new Configuration(true);
    conf.addResource(new Path(hdfsConfigPath));
    conf.reloadConfiguration();
    return conf;
  }

  @Provides
  public ExceptionalClosure<KillCommand, KillException> provideProcessKiller(
      ExceptionalFunction<String, List<String>, SignalException> httpSignaler) throws IOException {

    return new ProcessKiller(httpSignaler, KILLTREE_PATH.get(), KILL_ESCALATION_DELAY.get());
  }
}

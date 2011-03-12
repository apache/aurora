package com.twitter.mesos.executor;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

import org.apache.hadoop.conf.Configuration;
import org.apache.mesos.Executor;

import com.twitter.common.application.http.Registration;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.CanRead;
import com.twitter.common.args.constraints.Exists;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.common.base.ExceptionalClosure;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.mesos.Message;
import com.twitter.mesos.executor.Driver.DriverImpl;
import com.twitter.mesos.executor.FileToInt.FetchException;
import com.twitter.mesos.executor.HealthChecker.HealthCheckException;
import com.twitter.mesos.executor.HttpSignaler.SignalException;
import com.twitter.mesos.executor.ProcessKiller.KillCommand;
import com.twitter.mesos.executor.ProcessKiller.KillException;
import com.twitter.mesos.executor.httphandlers.ExecutorHome;
import com.twitter.mesos.executor.httphandlers.TaskHome;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.util.HdfsUtil;

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
  private static final Arg<File> taskRootDir = Arg.create();

  @CmdLine(name = "multi_user", help = "True to execute tasks as the job owner")
  private static final Arg<Boolean> multiUserMode = Arg.create(true);

  @CmdLine(name = "managed_port_range",
      help = "Port range that the executor should manage, format: min-max")
  private static final Arg<String> managedPortRange = Arg.create("50000-60000");

  @NotNull
  @CmdLine(name = "hdfs_config", help = "Hadoop configuration path")
  private static final Arg<String> hdfsConfig = Arg.create();

  @CmdLine(name = "http_signal_timeout", help = "Timeout for HTTP signals to tasks.")
  private static final Arg<Amount<Long, Time>> httpSignalTimeout =
      Arg.create(Amount.of(1L, Time.SECONDS));

  @Exists
  @CanRead
  @CmdLine(name = "kill_tree_path", help = "Path to kill tree shell script")
  private static final Arg<File> killTreePath =
      Arg.create(new File("/usr/local/mesos/bin/killtree.sh"));

  @CmdLine(name = "kill_escalation_delay_ms",
      help = "Time to wait before escalating between task kill procedures.")
  private static final Arg<Amount<Long, Time>> killEscalationDelay =
      Arg.create(Amount.of(5L, Time.SECONDS));

  @Override
  protected void configure() {

    // Bindings needed for ExecutorMain.
    bind(File.class).annotatedWith(ExecutorRootDir.class).toInstance(taskRootDir.get());
    bind(Executor.class).to(MesosExecutorImpl.class).in(Singleton.class);
    bind(ExecutorCore.class).in(Singleton.class);
    bind(new TypeLiteral<Supplier<Iterable<Task>>>() {})
        .to(DeadTaskLoader.class).in(Singleton.class);

    // Bindings for MesosExecutorImpl.
    bind(Driver.class).to(DriverImpl.class);
    bind(DriverImpl.class).in(Singleton.class);

    // Bindings needed for ExecutorCore.
    bind(new TypeLiteral<Function<AssignedTask, Task>>() {}).to(TaskFactory.class);
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("MesosExecutor-[%d]").build();
    bind(Key.get(ExecutorService.class, Names.named(ExecutorCore.TASK_EXECUTOR)))
        .toInstance(Executors.newCachedThreadPool(threadFactory));
    bind(new TypeLiteral<Function<Message, Integer>>() {}).to(DriverImpl.class);

    // Bindings needed for TaskFactory.
    String[] portRange = managedPortRange.get().split("-");
    Preconditions.checkArgument(portRange.length == 2, "Malformed managed port range value: "
                                         + managedPortRange);
    // TODO(William Farner): Clean this up, inject.
    bind(SocketManager.class).toInstance(new SocketManagerImpl(Integer.parseInt(portRange[0]),
        Integer.parseInt(portRange[1])));
    bind(new TypeLiteral<ExceptionalFunction<Integer, Boolean, HealthCheckException>>() {})
        .to(HealthChecker.class);
    // processKiller handled in provider method.
    bind(new TypeLiteral<ExceptionalFunction<File, Integer, FetchException>>() {})
        .to(FileToInt.class);
    bind(new TypeLiteral<ExceptionalFunction<FileCopyRequest, File, IOException>>() {})
        .to(HdfsFileCopier.class).in(Singleton.class);
    bind(Key.get(boolean.class, MultiUserMode.class)).toInstance(multiUserMode.get());

    // Bindings needed for HealthChecker.
    ThreadFactory httpSignalThreadFactory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("HTTP-signaler-%d")
        .build();
    bind(new TypeLiteral<ExceptionalFunction<String, List<String>, SignalException>>() {})
        .toInstance(new HttpSignaler(Executors.newCachedThreadPool(httpSignalThreadFactory),
            httpSignalTimeout.get()));

    // Bindings needed for HdfsFileCopier
    try {
      bind(Configuration.class).toInstance(HdfsUtil.getHdfsConfiguration(hdfsConfig.get()));
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to create HDFS fileSystem.", e);
      Throwables.propagate(e);
    }

    Registration.registerServlet(binder(), "/task", TaskHome.class, false);
    Registration.registerServlet(binder(), "/executor", ExecutorHome.class, false);
  }

  @Provides
  public ExceptionalClosure<KillCommand, KillException> provideProcessKiller(
      ExceptionalFunction<String, List<String>, SignalException> httpSignaler) throws IOException {

    return new ProcessKiller(httpSignaler, killTreePath.get(), killEscalationDelay.get());
  }
}

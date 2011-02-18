package com.twitter.mesos.executor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.twitter.common.base.ExceptionalClosure;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.common.process.GuicedProcess;
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
import mesos.Executor;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ExecutorModule
 *
 * @author Florian Leibert
 */
public class ExecutorModule extends AbstractModule {
  private final static Logger LOG = Logger.getLogger(ExecutorModule.class.getName());
  private final ExecutorMain.TwitterExecutorOptions options;

  @Inject
  public ExecutorModule(ExecutorMain.TwitterExecutorOptions options) {
    this.options = Preconditions.checkNotNull(options);
  }

  @Override
  protected void configure() {

    // Bindings needed for ExecutorMain.
    bind(File.class).annotatedWith(ExecutorRootDir.class).toInstance(options.taskRootDir);
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
    String[] portRange = options.managedPortRange.split("-");
    Preconditions.checkArgument(portRange.length == 2, "Malformed managed port range value: "
                                         + options.managedPortRange);
    // TODO(wfarner): Clean this up, inject.
    bind(SocketManager.class).toInstance(new SocketManagerImpl(Integer.parseInt(portRange[0]),
        Integer.parseInt(portRange[1])));
    bind(new TypeLiteral<ExceptionalFunction<Integer, Boolean, HealthCheckException>>() {})
        .to(HealthChecker.class);
    // processKiller handled in provider method.
    bind(new TypeLiteral<ExceptionalFunction<File, Integer, FetchException>>() {})
        .to(FileToInt.class);
    bind(new TypeLiteral<ExceptionalFunction<FileCopyRequest, File, IOException>>() {})
        .to(HdfsFileCopier.class).in(Singleton.class);
    bind(Key.get(boolean.class, MultiUserMode.class)).toInstance(options.multiUserMode);

    // Bindings needed for HealthChecker.
    ThreadFactory httpSignalThreadFactory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("HTTP-signaler-%d")
        .build();
    bind(new TypeLiteral<ExceptionalFunction<String, List<String>, SignalException>>() {})
        .toInstance(new HttpSignaler(Executors.newCachedThreadPool(httpSignalThreadFactory),
        Amount.of((long) options.httpSignalTimeoutMs, Time.MILLISECONDS)));

    // Bindings needed for HdfsFileCopier
    try {
      bind(Configuration.class).toInstance(HdfsUtil.getHdfsConfiguration(options.hdfsConfig));
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to create HDFS fileSystem.", e);
      Throwables.propagate(e);
    }

    GuicedProcess.registerServlet(binder(), "/task", TaskHome.class, false);
    GuicedProcess.registerServlet(binder(), "/executor", ExecutorHome.class, false);
  }

  @Provides
  public ExceptionalClosure<KillCommand, KillException> provideProcessKiller(
      ExceptionalFunction<String, List<String>, SignalException> httpSignaler) throws IOException {

    return new ProcessKiller(httpSignaler, options.killTreePath,
        Amount.of((long) options.killEscalationMs, Time.MILLISECONDS));
  }
}

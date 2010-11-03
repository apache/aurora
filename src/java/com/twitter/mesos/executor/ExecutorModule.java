package com.twitter.mesos.executor;

import com.google.common.base.Preconditions;
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
import com.twitter.mesos.executor.HttpSignaler.SignalException;
import com.twitter.mesos.executor.ProcessKiller.KillCommand;
import com.twitter.mesos.executor.ProcessKiller.KillException;
import com.twitter.mesos.executor.httphandlers.ExecutorHome;
import com.twitter.mesos.executor.httphandlers.TaskHome;
import com.twitter.mesos.util.HdfsUtil;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Logger;

/**
 * ExecutorModule
 *
 * @author Florian Leibert
 */
public class ExecutorModule extends AbstractModule {
  private final static Logger LOG = Logger.getLogger(ExecutorModule.class.getName());
  private final ExecutorMain.TwitterExecutorOptions options;

  // TODO(wfarner): Export the executor root dir with the @Named binding

  @Inject
  public ExecutorModule(ExecutorMain.TwitterExecutorOptions options) {
    this.options = Preconditions.checkNotNull(options);
  }

  @Override
  protected void configure() {
    bind(ExecutorCore.class).in(Singleton.class);
    bind(MesosExecutorImpl.class).in(Singleton.class);
    bind(new TypeLiteral<ExceptionalFunction<File, Integer, FileToInt.FetchException>>() {})
        .to(FileToInt.class);
    bind(new TypeLiteral<
        ExceptionalFunction<Integer, Boolean, HealthChecker.HealthCheckException>>() {})
        .to(HealthChecker.class);

    bind(Key.get(File.class, Names.named(ExecutorCore.EXECUTOR_ROOT_DIR)))
        .toInstance(options.taskRootDir);

    GuicedProcess.registerServlet(binder(), "/task", TaskHome.class, false);
    GuicedProcess.registerServlet(binder(), "/executor", ExecutorHome.class, false);
  }

  @Provides
  @Singleton
  public FileSystem provideFileSystem() throws IOException {
    return HdfsUtil.getHdfsConfiguration(options.hdfsConfig);
  }

  @Provides
  public ExceptionalFunction<FileCopyRequest, File, IOException> provideFileCopier(
      final FileSystem fileSystem) {
    return new ExceptionalFunction<FileCopyRequest, File, IOException>() {
      @Override public File apply(FileCopyRequest copy) throws IOException {
        LOG.info(String.format(
            "HDFS file %s -> local file %s", copy.getSourcePath(), copy.getDestPath()));
        // Thanks, Apache, for writing good code and just assuming that the path i give you has
        // a trailing slash.  Of course it makes sense to blindly append a file name to the path
        // i provide.
        String dirWithSlash = copy.getDestPath();
        if (!dirWithSlash.endsWith("/")) dirWithSlash += "/";

        return HdfsUtil.downloadFileFromHdfs(fileSystem, copy.getSourcePath(), dirWithSlash, true);
      }
    };
  }

  @Provides
  @Singleton
  public SocketManager provideSocketManager() {
    String[] portRange = options.managedPortRange.split("-");
    if (portRange.length != 2) {
      throw new IllegalArgumentException("Malformed managed port range value: "
                                         + options.managedPortRange);
    }

    return new SocketManagerImpl(Integer.parseInt(portRange[0]), Integer.parseInt(portRange[1]));
  }

  @Provides
  public ExceptionalFunction<String, List<String>, SignalException> provideHttpSignaler() {
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("HTTP-signaler-%d")
        .build();
    ExecutorService executor = Executors.newSingleThreadExecutor(threadFactory);

    return new HttpSignaler(executor,
        Amount.of((long) options.httpSignalTimeoutMs, Time.MILLISECONDS));
  }

  @Provides
  public ExceptionalClosure<KillCommand, KillException> provideProcessKiller(
      ExceptionalFunction<String, List<String>, SignalException> httpSignaler) throws IOException {

    return new ProcessKiller(httpSignaler, options.killTreePath,
        Amount.of((long) options.killEscalationMs, Time.MILLISECONDS));
  }
}

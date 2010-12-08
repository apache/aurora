package com.twitter.mesos.executor;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.twitter.common.args.Option;
import com.twitter.common.base.Command;
import com.twitter.common.process.GuicedProcess;
import com.twitter.common.process.GuicedProcessOptions;
import com.twitter.mesos.scheduler.ExecutorRootDir;
import mesos.Executor;
import mesos.MesosExecutorDriver;

import java.io.File;
import java.util.logging.Logger;

/**
 * ExecutorMain
 *
 * @author Florian Leibert
 */
public class ExecutorMain extends GuicedProcess<ExecutorMain.TwitterExecutorOptions, Exception> {
  private static Logger LOG = Logger.getLogger(ExecutorMain.class.getName());

  public static class TwitterExecutorOptions extends GuicedProcessOptions {
    @Option(name = "hdfs_config", required = true, usage = "Hadoop configuration path")
    public String hdfsConfig;

    @Option(name = "kill_tree_path", usage = "Path to kill tree shell script")
    public File killTreePath = new File("/usr/local/mesos/bin/killtree.sh");

    @Option(name = "task_root_dir", required = true, usage = "Mesos task working directory root.")
    public File taskRootDir;

    @Option(name = "managed_port_range",
        usage = "Port range that the executor should manage, format: min-max")
    public String managedPortRange = "50000-60000";

    @Option(name = "http_signal_timeout_ms", usage = "Timeout for HTTP signals to tasks.")
    public int httpSignalTimeoutMs = 1000;

    @Option(name = "kill_escalation_delay_ms",
        usage = "Time to wait before escalating between task kill procedures.")
    public int killEscalationMs = 5000;
  }

  @Inject @ExecutorRootDir private File executorRootDir;
  @Inject private Executor mesosExecutor;
  @Inject private ExecutorCore executorCore;
  @Inject private Supplier<Iterable<Task>> deadTaskLoader;

  protected ExecutorMain() {
    super(TwitterExecutorOptions.class);
  }

  @Override
  public void runProcess() throws Exception {
    addShutdownAction(new Command() {
      @Override public void execute() {
        System.out.println("Shutting down the executor.");
        executorCore.shutdownCore();
      }
    });

    if (!executorRootDir.exists()) {
      Preconditions.checkState(executorRootDir.mkdirs(), "Failed to create executor root dir.");
    }

    executorCore.addDeadTasks(deadTaskLoader.get());
    executorCore.startPeriodicTasks();

    new MesosExecutorDriver(mesosExecutor).run();
  }

  @Override
  protected Iterable<Class<? extends Module>> getProcessModuleClasses() {
    return ImmutableList.<Class<? extends Module>>of(ExecutorModule.class);
  }

  public static void main(String[] args) throws Exception {
    new ExecutorMain().run(args);
  }
}

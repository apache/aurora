package com.twitter.nexus.executor;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.twitter.common.args.Option;
import com.twitter.common.inject.process.InjectableMain;
import com.twitter.common.process.GuicedProcessOptions;
import nexus.NexusExecutorDriver;

/**
 * ExecutorMain
 *
 * @author Florian Leibert
 */
public class ExecutorMain extends InjectableMain<ExecutorMain.TwitterExecutorOptions, Exception> {

  public static class TwitterExecutorOptions extends GuicedProcessOptions {
    @Option(name = "hdfs_config", required = true, usage = "Hadoop configuration path")
    public String hdfsConfig;
  }

  @Inject
  private ExecutorHub executorHub;

  protected ExecutorMain() {
    super(TwitterExecutorOptions.class);
  }

  @Override
  public void execute() throws Exception {
    new NexusExecutorDriver(executorHub).run();
  }

  @Override
  protected Iterable<Class<? extends Module>> getModuleClasses() {
    return ImmutableList.<Class<? extends Module>>of(ExecutorModule.class);
  }

  public static void main(String[] args) throws Exception {
    new ExecutorMain().run(args);
  }
}

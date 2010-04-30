package com.twitter.nexus;

import com.google.inject.Inject;
import com.twitter.common.args.Option;
import com.twitter.common.process.GuicedProcess;
import com.twitter.common.process.GuicedProcessOptions;
import nexus.NexusExecutorDriver;

/**
 * ExecutorMain
 *
 * @author Florian Leibert
 */
public class ExecutorMain extends GuicedProcess<ExecutorMain.TwitterExecutorOptions, Exception> {

  public static class TwitterExecutorOptions extends GuicedProcessOptions {
    @Option(name = "hdfs_config", required = true, usage = "Hadoop configuration path")
    public String hdfsConfig;
  }

  @Inject
  TwitterExecutor executor;

  protected ExecutorMain() {
    super(TwitterExecutorOptions.class);
  }

  @Override
  protected void runProcess() throws Exception {
    new NexusExecutorDriver(executor).run();
  }

  public static void main(String[] args) throws Exception {
    new ExecutorMain().run(args);
  }
}

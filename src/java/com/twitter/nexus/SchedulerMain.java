package com.twitter.nexus;

import com.google.inject.Inject;
import com.twitter.common.args.Option;
import com.twitter.common.process.GuicedProcess;
import com.twitter.common.process.GuicedProcessOptions;
import nexus.NexusSchedulerDriver;

/**
 * Launcher for the twitter nexue scheduler.
 *
 * @author wfarner
 */
public class SchedulerMain extends GuicedProcess<SchedulerMain.TwitterSchedulerOptions,Exception> {
  public static class TwitterSchedulerOptions extends GuicedProcessOptions {
    @Option(name = "executor_path", required = true, usage = "Path to the executor launch script.")
    public String executorPath;

    @Option(name = "master_address", required = true, usage = "Nexus address for the master node.")
    public String masterAddress;

    @Option(name = "thrift_port", required = true, usage = "Port for thrift server to listen on.")
    public int thriftPort;

    @Option(name = "hdfs_config", required = true, usage = "Hadoop configuration path")
    public String hdfsConfig;
  }

  public SchedulerMain() {
    super(TwitterSchedulerOptions.class);
  }

  @Inject
  SchedulerHub sched;


  @Override
  protected void runProcess() {
    NexusSchedulerDriver driver = new NexusSchedulerDriver(sched, getOptions().masterAddress);
    driver.start();
    sched.startThriftServer(getOptions().thriftPort);
  }

  protected boolean checkOptions() {
    return true;
  }

  //TODO(flo): proper exception handling...
  public static void main(String[] args) throws Exception{
    new SchedulerMain().run(args);
  }
}

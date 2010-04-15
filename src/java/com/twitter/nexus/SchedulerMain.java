package com.twitter.nexus;

import com.twitter.common.args.Option;
import com.twitter.common.process.Process;
import nexus.NexusSchedulerDriver;

/**
 * Launcher for the twitter nexue scheduler.
 *
 * @author wfarner
 */
public class SchedulerMain extends Process {
  protected static class Options extends Process.Options {
    @Option(name = "executor_path", required = true, usage = "Path to the executor launch script.")
    private static String executorPath;

    @Option(name = "master_address", required = true, usage = "Nexus address for the master node.")
    private static String masterAddress;

    @Option(name = "thrift_port", required = true, usage = "Port for thrift server to listen on.")
    private static int thriftPort;
  }
  protected static Process.Options options = new Options();

  @Override
  protected void runProcess() {
    SchedulerHub sched = new SchedulerHub(Options.executorPath);
    NexusSchedulerDriver driver = new NexusSchedulerDriver(sched, Options.masterAddress);
    driver.start();

    sched.startThriftServer(Options.thriftPort);
  }

  @Override
  protected boolean checkOptions() {
    return true;
  }

  @Override
  protected Process.Options getOptions() {
    return options;
  }

  public static void main(String[] args) {
    new SchedulerMain().run(args);
  }
}

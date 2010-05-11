package com.twitter.nexus.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.twitter.common.args.Option;
import com.twitter.common.base.Command;
import com.twitter.common.process.GuicedProcess;
import com.twitter.common.process.GuicedProcessOptions;
import com.twitter.common.zookeeper.Group;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.nexus.gen.NexusSchedulerManager;
import com.twitter.thrift.Status;
import nexus.NexusSchedulerDriver;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Launcher for the twitter nexue scheduler.
 *
 * @author wfarner
 */
public class SchedulerMain extends GuicedProcess<SchedulerMain.TwitterSchedulerOptions, Exception> {
  public static class TwitterSchedulerOptions extends GuicedProcessOptions {
    @Option(name = "executor_path", required = true,
            usage = "Path to the executorHub launch script.")
    String executorPath;

    @Option(name = "master_address", required = true, usage = "Nexus address for the master node.")
    String masterAddress;

    @Option(name = "zk_endpoints", required = true,
            usage = "Endpoint specification for the ZooKeeper servers.")
    List<InetSocketAddress> zooKeeperEndpoints;

    @Option(name = "zk_session_timeout_secs",
            usage = "The ZooKeeper session timeout in seconds.")
    int zooKeeperSessionTimeoutSecs = 5;

    @Option(name = "nexus_scheduler_ns", required = true,
            usage = "The name service name for the nexus scheduler thrift server.")
    String nexusSchedulerNameSpec;
  }

  private static Logger LOG = Logger.getLogger(SchedulerMain.class.getName());

  public SchedulerMain() {
    super(TwitterSchedulerOptions.class);
  }

  @Inject
  private NexusSchedulerImpl scheduler;

  @Inject
  private SchedulerManager schedulerManager;

  @Inject
  private ServerSet nexusSchedulerServerSet;

  @Override
  protected Iterable<Class<? extends Module>> getProcessModuleClasses() {
    return ImmutableList.<Class<? extends Module>>of(SchedulerModule.class);
  }

  @Override
  protected void runProcess() throws Exception {
    NexusSchedulerDriver driver = new NexusSchedulerDriver(scheduler, getOptions().masterAddress);
    driver.start();
    startThriftServer();
    waitForEver();
  }

  protected boolean checkOptions() {
    return true;
  }

  private ServerSet.EndpointStatus startThriftServer()
      throws IOException, TTransportException, Group.JoinException, InterruptedException {
    schedulerManager.start(0, new NexusSchedulerManager.Processor(schedulerManager));

    addShutdownAction(new Command() {
      @Override public void execute() {
        LOG.info("Stopping thrift server");
        try {
          schedulerManager.shutdown();
        } catch (TException e) {
          LOG.log(Level.WARNING, "Error while stopping thrift server.", e);
        }
      }
    });
    return joinServerSet(nexusSchedulerServerSet, schedulerManager.getListeningPort(),
        Status.STARTING);
  }

  //TODO(flo): proper exception handling...
  public static void main(String[] args) throws Exception {
    new SchedulerMain().run(args);
  }
}

package com.twitter.nexus.scheduler;

import com.google.common.base.Preconditions;
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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
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

    @Option(name = "zk_endpoints", required = true,
            usage = "Endpoint specification for the ZooKeeper servers.")
    List<InetSocketAddress> zooKeeperEndpoints;

    @Option(name = "zk_session_timeout_secs",
            usage = "The ZooKeeper session timeout in seconds.")
    int zooKeeperSessionTimeoutSecs = 5;

    @Option(name = "nexus_master_ns",
            usage = "The name service name for the nexus master.")
    String nexusMasterNameSpec;

    @Option(name = "nexus_scheduler_ns", required = true,
            usage = "The name service name for the nexus scheduler thrift server.")
    String nexusSchedulerNameSpec;

    @Option(name = "nexus_master_address",
            usage = "Nexus address for the master, overrides nexus_master_ns.")
    String nexusMasterAddress;

    @Option(name = "scheduler_persistence_zookeeper_path",
            usage = "Path in ZooKeeper that scheduler will persist to/from (overrides local).")
    String schedulerPersistenceZooKeeperPath;

    @Option(name = "scheduler_persistence_zookeeper_version",
            usage = "Version for scheduler persistence node in ZooKeeper.")
    int schedulerPersistenceZooKeeperVersion = 1;

    @Option(name = "scheduler_persistence_local_path",
            usage = "Local file path that scheduler will persist to/from.")
    File schedulerPersistenceLocalPath = new File("/tmp/nexus_scheduler_dump");
  }

  private static Logger LOG = Logger.getLogger(SchedulerMain.class.getName());

  public SchedulerMain() {
    super(TwitterSchedulerOptions.class);
  }

  @Inject
  private NexusSchedulerImpl scheduler;

  @Inject
  private SchedulerThriftInterface schedulerThriftInterface;

  @Inject
  private ServerSet nexusSchedulerServerSet;

  @Inject
  private NexusSchedulerDriver driver;

  @Override
  protected Iterable<Class<? extends Module>> getProcessModuleClasses() {
    return ImmutableList.<Class<? extends Module>>of(SchedulerModule.class);
  }

  @Override
  protected void runProcess() {
    Preconditions.checkNotNull(driver);
    driver.start();

    ServerSet.EndpointStatus endpointStatus = null;
    try {
      endpointStatus = startThriftServer();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to start thrift server.", e);
    } catch (TTransportException e) {
      LOG.log(Level.SEVERE, "Failed to start thrift server.", e);
    } catch (Group.JoinException e) {
      LOG.log(Level.SEVERE, "Failed to join nexus scheduler server set in ZooKeeper.", e);
    } catch (InterruptedException e) {
      LOG.log(Level.SEVERE, "Interrupted while starting thrift server.", e);
    }

    /** TODO(wfarner): Re-enable.
    if (endpointStatus == null) return;
    try {
      endpointStatus.update(Status.ALIVE);
    } catch (ServerSet.UpdateException e) {
      LOG.log(Level.SEVERE, "Failed to update server status in ZooKeeper.", e);
    } catch (InterruptedException e) {
      LOG.log(Level.SEVERE, "Interrupted while updating server status in ZooKeeper.", e);
    }
    */

    waitForEver();
  }

  protected boolean checkOptions() {
    return true;
  }

  private ServerSet.EndpointStatus startThriftServer() throws IOException, TTransportException,
      Group.JoinException, InterruptedException {
    schedulerThriftInterface.start(0,
        new NexusSchedulerManager.Processor(schedulerThriftInterface));

    addShutdownAction(new Command() {
      @Override public void execute() {
        System.out.println("Stopping thrift server");
        try {
          schedulerThriftInterface.shutdown();
        } catch (TException e) {
          LOG.log(Level.WARNING, "Error while stopping thrift server.", e);
        }
      }
    });

    /** TODO(wfarner): Re-enable this.
    LOG.info("Registering host in server set " + getOptions().nexusSchedulerNameSpec
             + " as listening on port " + schedulerThriftInterface.getListeningPort());
    return joinServerSet(nexusSchedulerServerSet, schedulerThriftInterface.getListeningPort(),
        Status.STARTING);
     */
    return null;
  }

  public static void main(String[] args) throws Exception {
    new SchedulerMain().run(args);
  }
}

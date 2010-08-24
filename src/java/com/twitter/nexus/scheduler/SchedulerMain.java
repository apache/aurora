package com.twitter.nexus.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.twitter.common.args.Option;
import com.twitter.common.base.Closure;
import com.twitter.common.base.Command;
import com.twitter.common.process.GuicedProcess;
import com.twitter.common.process.GuicedProcessOptions;
import com.twitter.common.zookeeper.Group;
import com.twitter.common.zookeeper.ServerSet.EndpointStatus;
import com.twitter.common.zookeeper.ServerSet.UpdateException;
import com.twitter.common.zookeeper.SingletonService;
import com.twitter.nexus.gen.NexusSchedulerManager;
import com.twitter.thrift.Status;
import nexus.NexusSchedulerDriver;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Launcher for the twitter nexue scheduler.
 *
 * TODO(wfarner): Include information in /schedulerz about who is the current scheduler leader.
 *
 * @author wfarner
 */
public class SchedulerMain extends GuicedProcess<SchedulerMain.TwitterSchedulerOptions, Exception> {
  public static class TwitterSchedulerOptions extends GuicedProcessOptions {
    @Option(name = "executor_path", required = true,
            usage = "Path to the executorHub launch script.")
    String executorPath;

    @Option(name = "zk_endpoints", usage = "Endpoint specification for the ZooKeeper servers.")
    List<InetSocketAddress> zooKeeperEndpoints;

    @Option(name = "zk_session_timeout_secs",
            usage = "The ZooKeeper session timeout in seconds.")
    int zooKeeperSessionTimeoutSecs = 5;

    @Option(name = "nexus_scheduler_ns", required = true,
            usage = "The name service name for the nexus scheduler thrift server.")
    String nexusSchedulerNameSpec;

    @Option(name = "nexus_master_address",
            usage = "Nexus address for the master, can be a nexus address or zookeeper path.")
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

  // Closure to execute when the scheduler has been elected group leader.
  private final Closure<EndpointStatus> onLeading =
      new Closure<EndpointStatus>() {
        @Override public void execute(EndpointStatus status) {
          LOG.info("Elected as leading scheduler!");
          try {
            status.update(Status.ALIVE);
            runNexusDriver();
          } catch (UpdateException e) {
            LOG.log(Level.SEVERE, "Failed to update endpoint status.", e);
          } catch (InterruptedException e) {
            LOG.log(Level.SEVERE, "Interrupted while updating endpoint status.", e);
          }
        }
      };

  public SchedulerMain() {
    super(TwitterSchedulerOptions.class);
  }

  @Inject
  private SchedulerCore schedulerCore;

  @Inject
  private SingletonService schedulerService;

  @Inject
  private SchedulerThriftInterface schedulerThriftInterface;

  @Inject
  private NexusSchedulerDriver driver;

  @Override
  protected Iterable<Class<? extends Module>> getProcessModuleClasses() {
    return ImmutableList.<Class<? extends Module>>of(SchedulerModule.class);
  }

  @Override
  protected void runProcess() {
    int port = -1;
    try {
      port = startThriftServer();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to start thrift server.", e);
    } catch (TTransportException e) {
      LOG.log(Level.SEVERE, "Failed to start thrift server.", e);
    } catch (Group.JoinException e) {
      LOG.log(Level.SEVERE, "Failed to join nexus scheduler server set in ZooKeeper.", e);
    } catch (InterruptedException e) {
      LOG.log(Level.SEVERE, "Interrupted while starting thrift server.", e);
    }
    if (port == -1) return;

    if (schedulerService != null) {
      try {
        leadService(schedulerService, port, Status.STARTING, onLeading);
      } catch (Group.WatchException e) {
        LOG.log(Level.SEVERE, "Failed to watch group and lead service.", e);
      } catch (Group.JoinException e) {
        LOG.log(Level.SEVERE, "Failed to join scheduler service group.", e);
      } catch (InterruptedException e) {
        LOG.log(Level.SEVERE, "Interrupted while joining scheduler service group.", e);
      } catch (UnknownHostException e) {
        LOG.log(Level.SEVERE, "Failed to find self host name.", e);
      }
    } else {
      runNexusDriver();
    }

    waitForEver();
  }

  private void runNexusDriver() {
    int result = driver.run();
    LOG.info("Driver completed with exit code " + result);
  }

  private int startThriftServer() throws IOException, TTransportException,
      Group.JoinException, InterruptedException {
    schedulerThriftInterface.start(0,
        new NexusSchedulerManager.Processor(schedulerThriftInterface));

    addShutdownAction(new Command() {
      @Override public void execute() {
        LOG.info("Stopping thrift server.");
        try {
          schedulerThriftInterface.shutdown();
        } catch (TException e) {
          LOG.log(Level.WARNING, "Error while stopping thrift server.", e);
        }
      }
    });

    return schedulerThriftInterface.getListeningPort();
  }

  public static void main(String[] args) throws Exception {
    new SchedulerMain().run(args);
  }
}

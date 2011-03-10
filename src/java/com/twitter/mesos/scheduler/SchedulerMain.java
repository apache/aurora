package com.twitter.mesos.scheduler;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.twitter.common_internal.args.Option;
import com.twitter.common.base.Command;
import com.twitter.common.net.InetSocketAddressHelper;
import com.twitter.common_internal.process.GuicedProcess;
import com.twitter.common_internal.process.GuicedProcessOptions;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.common.thrift.Util;
import com.twitter.common.zookeeper.Group;
import com.twitter.common.zookeeper.ServerSet.EndpointStatus;
import com.twitter.common.zookeeper.ServerSet.UpdateException;
import com.twitter.common.zookeeper.SingletonService;
import com.twitter.common.zookeeper.SingletonService.LeadershipListener;
import com.twitter.common.zookeeper.ZooKeeperUtils;
import com.twitter.common_internal.zookeeper.TwitterZk;
import com.twitter.mesos.gen.MesosSchedulerManager;
import com.twitter.mesos.gen.StorageMigrationResult;
import com.twitter.mesos.scheduler.storage.Migrator;
import com.twitter.thrift.Status;

import org.apache.mesos.SchedulerDriver;
import org.apache.thrift.transport.TTransportException;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

import static com.twitter.common.quantity.Time.SECONDS;

/**
 * Launcher for the twitter mesos scheduler.
 *
 * TODO(William Farner): Include information in /schedulerz about who is the current scheduler leader.
 *
 * @author William Farner
 */
public class SchedulerMain extends GuicedProcess<SchedulerMain.TwitterSchedulerOptions, Exception> {
  public static class TwitterSchedulerOptions extends GuicedProcessOptions {
    @Option(name = "executor_path", required = true,
            usage = "Path to the executor launch script.")
    public String executorPath;

    @Option(name = "zk_in_proc",
            usage = "Launches an embedded zookeeper server for local testing")
    public boolean zooKeeperInProcess = false;

    @Option(name = "zk_endpoints", usage = "Endpoint specification for the ZooKeeper servers.")
    public List<InetSocketAddress> zooKeeperEndpoints =
        Lists.newArrayList(TwitterZk.DEFAULT_ZK_ENDPOINTS);

    @Option(name = "zk_session_timeout_secs",
            usage = "The ZooKeeper session timeout in seconds.")
    public int zooKeeperSessionTimeoutSecs = ZooKeeperUtils.DEFAULT_ZK_SESSION_TIMEOUT.as(SECONDS);

    @Option(name = "mesos_scheduler_ns", required = true,
            usage = "The name service name for the mesos scheduler thrift server.")
    public String mesosSchedulerNameSpec;

    @Option(name = "mesos_master_address",
            usage = "Mesos address for the master, can be a mesos address or zookeeper path.")
    public String mesosMasterAddress;

    @Option(name = "machine_restrictions",
        usage = "Map of machine hosts to job keys."
                + "  If A maps to B, only B can run on A and B can only run on A.")
    public Map<String, String> machineRestrictions = Maps.newHashMap();

    @Option(name = "job_updater_hdfs_path", usage = "HDFS path to the job updater package.")
    public String jobUpdaterHdfsPath = "/mesos/pkg/mesos/bin/mesos-updater.zip";

    @Option(name = "scheduler_upgrade_storage",
            usage = "True to upgrade storage from a legacy system to a new primary system.")
    public boolean upgradeStorage = true;

    // Stream storage
    @Option(name = "scheduler_persistence_zookeeper_path",
            usage = "Path in ZooKeeper that scheduler will persist to/from (overrides local).")
    public String schedulerPersistenceZooKeeperPath;

    @Option(name = "scheduler_persistence_zookeeper_version",
            usage = "Version for scheduler persistence node in ZooKeeper.")
    public int schedulerPersistenceZooKeeperVersion = 1;

    @Option(name = "scheduler_persistence_local_path",
            usage = "Local file path that scheduler will persist to/from.")
    public File schedulerPersistenceLocalPath = new File("/tmp/mesos_scheduler_dump");

    // Db storage
    // TODO(John Sirois): get a mesos data dir setup in puppet and move the db files there
    @Option(name = "scheduler_db_file_path",
            usage = "The path of the H2 db files.")
    public File dbFilePath = new File("/tmp/mesos_scheduler_db");

    @Option(name = "scheduler_db_cache_size",
            usage = "The size to use for the H2 in-memory db cache.")
    public Amount<Long, Data> dbCacheSize = Amount.of(128L, Data.Mb);
  }

  private static final Logger LOG = Logger.getLogger(SchedulerMain.class.getName());

  private final LeadershipListener leadershipListener = new LeadershipListener() {
    @Override public void onLeading(EndpointStatus status) {
      LOG.info("Elected as leading scheduler!");
      try {
        runMesosDriver();
        status.update(Status.ALIVE);
      } catch (UpdateException e) {
        LOG.log(Level.SEVERE, "Failed to update endpoint status.", e);
      }
    }

    @Override public void onDefeated(@Nullable EndpointStatus status) {
      LOG.info("Lost leadership, committing suicide.");

      try {
        if (status != null) {
          status.update(Status.DEAD);
        }
      } catch (UpdateException e) {
        LOG.log(Level.WARNING, "Failed to leave server set.", e);
      } finally {
        scheduler.stop();
        destroy();
        // TODO(William Farner): This seems necessary to break out of the blocking driver run.
        System.exit(1);
      }
    }
  };

  public SchedulerMain() {
    super(TwitterSchedulerOptions.class);
  }

  @Inject private SingletonService schedulerService;
  @Inject private SchedulerThriftInterface schedulerThriftInterface;
  @Inject private SchedulerDriver driver;
  @Inject private AtomicReference<InetSocketAddress> schedulerThriftPort;
  @Inject private SchedulerCore scheduler;

  @Nullable private Migrator migrator;

  @Inject(optional = true)
  public void setMigrator(Migrator migrator) {
    this.migrator = migrator;
  }

  @Override
  protected Iterable<Class<? extends Module>> getProcessModuleClasses() {
    return ImmutableList.<Class<? extends Module>>of(SchedulerModule.class);
  }

  @Override
  protected void runProcess() {
    if (migrator != null) {
      LOG.info("Attempting storage migration for: "
               + Util.prettyPrint(migrator.getMigrationPath()));
      StorageMigrationResult result = migrator.migrate();
      switch(result.getStatus()) {
        case NO_MIGRATION_NEEDED:
          LOG.info("Skipping migration, already performed: " + result);
          break;

        case SUCCESS:
          LOG.info("Successfully migrated storage: " + result);
          break;

        default:
          LOG.log(Level.SEVERE, "Failed to migrate storage: " + result);
          return;
      }
    }

    int port = -1;
    try {
      port = startThriftServer();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to start thrift server.", e);
    } catch (TTransportException e) {
      LOG.log(Level.SEVERE, "Failed to start thrift server.", e);
    } catch (Group.JoinException e) {
      LOG.log(Level.SEVERE, "Failed to join mesos scheduler server set in ZooKeeper.", e);
    } catch (InterruptedException e) {
      LOG.log(Level.SEVERE, "Interrupted while starting thrift server.", e);
    }
    if (port == -1) {
      return;
    }

    // TODO(William Farner): This is a bit of a hack, clean it up, maybe by exposing the thrift interface.
    try {
      schedulerThriftPort.set(InetSocketAddressHelper.getLocalAddress(port));
    } catch (UnknownHostException e) {
      LOG.severe("Unable to get local host address.");
      Throwables.propagate(e);
    }

    if (schedulerService != null) {
      try {
        leadService(schedulerService, port, Status.STARTING, leadershipListener);
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
      runMesosDriver();
    }

    waitForEver();
  }

  private void runMesosDriver() {
    new ThreadFactoryBuilder().setNameFormat("Driver-Runner-%d").setDaemon(true).build().newThread(
        new Runnable() {
          @Override public void run() {
            int result = driver.run();
            LOG.info("Driver completed with exit code " + result);
            destroy();
          }
        }
    ).start();
  }

  private int startThriftServer() throws IOException, TTransportException,
      Group.JoinException, InterruptedException {
    schedulerThriftInterface.start(0,
        new MesosSchedulerManager.Processor(schedulerThriftInterface));

    addShutdownAction(new Command() {
      @Override public void execute() {
        LOG.info("Stopping thrift server.");
        schedulerThriftInterface.shutdown();
      }
    });

    return schedulerThriftInterface.getListeningPort();
  }

  public static void main(String[] args) throws Exception {
    new SchedulerMain().run(args);
  }
}

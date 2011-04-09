package com.twitter.mesos.scheduler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.Module;

import org.apache.mesos.SchedulerDriver;
import org.apache.thrift.transport.TTransportException;

import com.twitter.common.application.AbstractApplication;
import com.twitter.common.application.ActionRegistry;
import com.twitter.common.application.Lifecycle;
import com.twitter.common.application.ShutdownStage;
import com.twitter.common.application.modules.StatsExportModule;
import com.twitter.common.application.modules.HttpModule;
import com.twitter.common.application.modules.LogModule;
import com.twitter.common.application.modules.StatsModule;
import com.twitter.common.base.Command;
import com.twitter.common.net.InetSocketAddressHelper;
import com.twitter.common.thrift.Util;
import com.twitter.common.zookeeper.Group;
import com.twitter.common.zookeeper.ServerSet.EndpointStatus;
import com.twitter.common.zookeeper.ServerSet.UpdateException;
import com.twitter.common.zookeeper.SingletonService;
import com.twitter.common.zookeeper.SingletonService.LeadershipListener;
import com.twitter.mesos.gen.MesosSchedulerManager;
import com.twitter.mesos.gen.StorageMigrationResult;
import com.twitter.mesos.scheduler.storage.Migrator;
import com.twitter.mesos.scheduler.storage.StorageRole;
import com.twitter.mesos.scheduler.storage.db.DbStorageModule;
import com.twitter.thrift.Status;

/**
 * Launcher for the twitter mesos scheduler.
 *
 * TODO(William Farner): Include information in /schedulerz about who is the current scheduler leader.
 *
 * @author William Farner
 */
public class SchedulerMain extends AbstractApplication {

  private static final Logger LOG = Logger.getLogger(SchedulerMain.class.getName());

  @Inject private SingletonService schedulerService;
  @Inject private SchedulerThriftInterface schedulerThriftInterface;
  @Inject private SchedulerDriver driver;
  @Inject private AtomicReference<InetSocketAddress> schedulerThriftPort;
  @Inject private SchedulerCore scheduler;
  @Inject private Lifecycle lifecycle;
  @Inject @ShutdownStage ActionRegistry shutdownRegistry;

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
        lifecycle.shutdown();
        // TODO(William Farner): This seems necessary to break out of the blocking driver run.
        System.exit(1);
      }
    }
  };

  @Nullable private Migrator migrator;

  @Inject(optional = true)
  public void setMigrator(Migrator migrator) {
    this.migrator = migrator;
  }

  @Override
  public Iterable<Module> getModules() {
    return Arrays.<Module>asList(
        new DbStorageModule(StorageRole.Role.Primary),
        new StatsExportModule(),
        new HttpModule(),
        new LogModule(),
        new SchedulerModule(),
        new StatsModule()
    );
  }

  @Override
  public void run() {
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

    // TODO(William Farner): This is a bit of a hack, clean it up, maybe by exposing the thrift
    //     interface.
    try {
      schedulerThriftPort.set(InetSocketAddressHelper.getLocalAddress(port));
    } catch (UnknownHostException e) {
      LOG.severe("Unable to get local host address.");
      Throwables.propagate(e);
    }

    if (schedulerService != null) {
      try {
        schedulerService.lead(InetSocketAddressHelper.getLocalAddress(port),
            Collections.<String, InetSocketAddress>emptyMap(), Status.STARTING, leadershipListener);
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

    lifecycle.awaitShutdown();
  }

  private void runMesosDriver() {
    new ThreadFactoryBuilder().setNameFormat("Driver-Runner-%d").setDaemon(true).build().newThread(
        new Runnable() {
          @Override public void run() {
            int result = driver.run();
            LOG.info("Driver completed with exit code " + result);
            lifecycle.shutdown();
          }
        }
    ).start();

    shutdownRegistry.addAction(new Command() {
      @Override public void execute() {
        LOG.info("Stopping mesos driver.");
        driver.stop();
      }
    });
  }

  private int startThriftServer() throws IOException, TTransportException,
      Group.JoinException, InterruptedException {
    schedulerThriftInterface.start(0,
        new MesosSchedulerManager.Processor(schedulerThriftInterface));

    shutdownRegistry.addAction(new Command() {
      @Override public void execute() {
        LOG.info("Stopping thrift server.");
        schedulerThriftInterface.shutdown();
      }
    });

    return schedulerThriftInterface.getListeningPort();
  }
}

package com.twitter.mesos.scheduler;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provider;

import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.thrift.transport.TTransportException;

import com.twitter.common.application.AbstractApplication;
import com.twitter.common.application.ActionRegistry;
import com.twitter.common.application.Lifecycle;
import com.twitter.common.application.ShutdownStage;
import com.twitter.common.application.modules.HttpModule;
import com.twitter.common.application.modules.LogModule;
import com.twitter.common.application.modules.StatsExportModule;
import com.twitter.common.application.modules.StatsModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.CanRead;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.common.args.constraints.Positive;
import com.twitter.common.base.Closure;
import com.twitter.common.base.Command;
import com.twitter.common.net.InetSocketAddressHelper;
import com.twitter.common.thrift.ThriftServer;
import com.twitter.common.thrift.ThriftServer.ServerSetup;
import com.twitter.common.zookeeper.Group;
import com.twitter.common.zookeeper.ServerSet.EndpointStatus;
import com.twitter.common.zookeeper.ServerSet.UpdateException;
import com.twitter.common.zookeeper.SingletonService;
import com.twitter.common.zookeeper.SingletonService.LeadershipListener;
import com.twitter.common_internal.webassets.Blueprint;
import com.twitter.mesos.gen.MesosAdmin;
import com.twitter.thrift.Status;

/**
 * Launcher for the twitter mesos scheduler.
 *
 * TODO(William Farner): Include information in /schedulerz about who is the current scheduler
 * leader.
 *
 * @author William Farner
 */
public class SchedulerMain extends AbstractApplication {

  private static final Logger LOG = Logger.getLogger(SchedulerMain.class.getName());

  @Inject private SingletonService schedulerService;
  @Inject private ThriftServer schedulerThriftServer;
  @Inject private MesosAdmin.Iface schedulerThriftInterface;
  @Inject private Provider<SchedulerDriver> driverProvider;
  @Inject private AtomicReference<InetSocketAddress> schedulerThriftPort;
  @Inject private SchedulerCore scheduler;
  @Inject private Lifecycle lifecycle;
  @Inject @ShutdownStage ActionRegistry shutdownRegistry;

  @CanRead
  @NotNull
  @CmdLine(name = "mesos_ssl_keyfile",
           help = "JKS keyfile for operating the Mesos Thrift-over-SSL interface.")
  private static final Arg<File> mesosSSLKeyFile = Arg.create();

  @Positive
  @CmdLine(name = "thrift_port", help = "Thrift server port.")
  private static final Arg<Integer> thriftPort = Arg.create(0);

  // Security is enforced via file permissions, not via this password, for what it's worth.
  private static final String SSL_KEYFILE_PASSWORD = "MesosKeyStorePassword";

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

        // TODO(John Sirois): add a call to driver.failover() or driver.restarting() when this
        // becomes available.  The existing driver.stop() deregisters our framework and kills all
        // our executors causing more ripple than is needed.
        scheduler.stop();

        lifecycle.shutdown();
        // TODO(William Farner): This seems necessary to break out of the blocking driver run.
        System.exit(1);
      }
    }
  };

  @Override
  public Iterable<? extends Module> getModules() {
    return Arrays.asList(
        new StatsExportModule(),
        new HttpModule(),
        new LogModule(),
        new SchedulerModule(),
        new StatsModule(),
        new Blueprint.HttpAssetModule()
    );
  }

  @Override
  public void run() {
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
    } catch (GeneralSecurityException e) {
      LOG.log(Level.SEVERE, "Failed to initialize SSL context for thrift server.", e);
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
      throw Throwables.propagate(e);
    }

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

    lifecycle.awaitShutdown();
  }

  private void runMesosDriver() {
    // Initialize the driver just in time
    // TODO(John Sirois): control this lifecycle in a more explicit - non Guice specific way
    final SchedulerDriver driver = driverProvider.get();

    scheduler.start(new Closure<String>() {
      @Override public void execute(String taskId) throws RuntimeException {
        Protos.Status status = driver.killTask(TaskID.newBuilder().setValue(taskId).build());
        if (status != Protos.Status.OK) {
          LOG.severe(String.format("Attempt to kill task %s failed with code %d",
              taskId, status));
        }
      }
    });

    new ThreadFactoryBuilder().setNameFormat("Driver-Runner-%d").setDaemon(true).build().newThread(
        new Runnable() {
          @Override public void run() {
            Protos.Status status = driver.run();
            LOG.info("Driver completed with exit code " + status);
            lifecycle.shutdown();
          }
        }
    ).start();
  }

  private int startThriftServer() throws IOException, TTransportException,
      Group.JoinException, InterruptedException, GeneralSecurityException {
    // TODO(wickman): Add helper to science thrift to perform this keyfile import.
    SSLContext ctx;

    ctx = SSLContext.getInstance("TLS");
    KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
    KeyStore ks = KeyStore.getInstance("JKS");
    ks.load(new FileInputStream(mesosSSLKeyFile.get()), SSL_KEYFILE_PASSWORD.toCharArray());
    kmf.init(ks, SSL_KEYFILE_PASSWORD.toCharArray());
    ctx.init(kmf.getKeyManagers(), null, null);

    SSLServerSocketFactory ssf = ctx.getServerSocketFactory();
    SSLServerSocket serverSocket = (SSLServerSocket) ssf.createServerSocket(thriftPort.get());
    serverSocket.setEnabledCipherSuites(serverSocket.getSupportedCipherSuites());
    serverSocket.setNeedClientAuth(false);

    ServerSetup setup = new ServerSetup(
        0,  // TODO(John Sirois): unused, fix ServerSetup constructors
        new MesosAdmin.Processor(schedulerThriftInterface),
        ThriftServer.BINARY_PROTOCOL.get());
    setup.setSocket(serverSocket);
    schedulerThriftServer.start(setup);

    shutdownRegistry.addAction(new Command() {
      @Override public void execute() {
        LOG.info("Stopping thrift server.");
        schedulerThriftServer.shutdown();
      }
    });

    return schedulerThriftServer.getListeningPort();
  }
}

package com.twitter.mesos.scheduler;

import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.logging.Logger;

import javax.annotation.Nonnegative;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import com.twitter.common.application.LocalServiceRegistry;
import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.application.modules.LifecycleModule.RegisteringServiceLauncher;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.CanRead;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.common.base.Command;
import com.twitter.common.thrift.ThriftServer;
import com.twitter.common.thrift.ThriftServer.ServerSetup;
import com.twitter.mesos.gen.MesosAdmin;
import com.twitter.mesos.gen.MesosAdmin.Iface;

/**
 * Service launcher that starts up and registers the scheduler thrift server as a primary service
 * for the application.
 *
 * @author William Farner
 */
class ThriftServerLauncher extends RegisteringServiceLauncher<Exception> {

  private static final Logger LOG = Logger.getLogger(ThriftServerLauncher.class.getName());

  @CanRead
  @NotNull
  @CmdLine(name = "mesos_ssl_keyfile",
           help = "JKS keyfile for operating the Mesos Thrift-over-SSL interface.")
  private static final Arg<File> MESOS_SSL_KEY_FILE = Arg.create();

  @Nonnegative
  @CmdLine(name = "thrift_port", help = "Thrift server port.")
  private static final Arg<Integer> THRIFT_PORT = Arg.create(0);

  static final String THRIFT_PORT_NAME = "thrift";

  // Security is enforced via file permissions, not via this password, for what it's worth.
  private static final String SSL_KEYFILE_PASSWORD = "MesosKeyStorePassword";

  private final Iface schedulerThriftInterface;
  private final ThriftServer schedulerThriftServer;
  private final ShutdownRegistry shutdownRegistry;

  @Inject
  ThriftServerLauncher(
      LocalServiceRegistry serviceRegistry,
      Iface schedulerThriftInterface,
      ThriftServer schedulerThriftServer,
      ShutdownRegistry shutdownRegistry) {
    super(serviceRegistry);

    this.schedulerThriftInterface = Preconditions.checkNotNull(schedulerThriftInterface);
    this.schedulerThriftServer = Preconditions.checkNotNull(schedulerThriftServer);
    this.shutdownRegistry = Preconditions.checkNotNull(shutdownRegistry);
  }

  @Override public String getPortName() {
    return THRIFT_PORT_NAME;
  }

  @Override public boolean isPrimaryService() {
    return true;
  }

  @Override
  public int launchAndGetPort() throws Exception {
    // TODO(wickman): Add helper to science thrift to perform this keyfile import.
    SSLContext ctx;

    ctx = SSLContext.getInstance("TLS");
    KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
    KeyStore ks = KeyStore.getInstance("JKS");
    ks.load(new FileInputStream(MESOS_SSL_KEY_FILE.get()), SSL_KEYFILE_PASSWORD.toCharArray());
    kmf.init(ks, SSL_KEYFILE_PASSWORD.toCharArray());
    ctx.init(kmf.getKeyManagers(), null, null);

    SSLServerSocketFactory ssf = ctx.getServerSocketFactory();
    SSLServerSocket serverSocket = (SSLServerSocket) ssf.createServerSocket(THRIFT_PORT.get());
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

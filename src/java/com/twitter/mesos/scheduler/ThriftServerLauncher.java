package com.twitter.mesos.scheduler;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.logging.Logger;

import javax.annotation.Nonnegative;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import com.twitter.common.application.modules.LifecycleModule.ServiceRunner;
import com.twitter.common.application.modules.LocalServiceRegistry.LocalService;
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
class ThriftServerLauncher implements ServiceRunner {

  private static final Logger LOG = Logger.getLogger(ThriftServerLauncher.class.getName());

  @CanRead
  @NotNull
  @CmdLine(name = "mesos_ssl_keyfile",
           help = "JKS keyfile for operating the Mesos Thrift-over-SSL interface.")
  private static final Arg<File> MESOS_SSL_KEY_FILE = Arg.create();

  @Nonnegative
  @CmdLine(name = "thrift_port", help = "Thrift server port.")
  private static final Arg<Integer> THRIFT_PORT = Arg.create(0);

  // Security is enforced via file permissions, not via this password, for what it's worth.
  private static final String SSL_KEYFILE_PASSWORD = "MesosKeyStorePassword";

  private final Iface schedulerThriftInterface;
  private final ThriftServer schedulerThriftServer;

  @Inject
  ThriftServerLauncher(Iface schedulerThriftInterface, ThriftServer schedulerThriftServer) {
    this.schedulerThriftInterface = Preconditions.checkNotNull(schedulerThriftInterface);
    this.schedulerThriftServer = Preconditions.checkNotNull(schedulerThriftServer);
  }

  @Override
  public LocalService launch() {
    // TODO(wickman): Add helper to science thrift to perform this keyfile import.
    SSLServerSocket serverSocket;
    try {
      KeyStore ks = KeyStore.getInstance("JKS");
      ks.load(new FileInputStream(MESOS_SSL_KEY_FILE.get()), SSL_KEYFILE_PASSWORD.toCharArray());

      KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
      kmf.init(ks, SSL_KEYFILE_PASSWORD.toCharArray());

      SSLContext ctx = SSLContext.getInstance("TLS");
      ctx.init(kmf.getKeyManagers(), null, null);

      SSLServerSocketFactory ssf = ctx.getServerSocketFactory();
      serverSocket = (SSLServerSocket) ssf.createServerSocket(THRIFT_PORT.get());
      serverSocket.setEnabledCipherSuites(serverSocket.getSupportedCipherSuites());
      serverSocket.setNeedClientAuth(false);
    } catch (IOException e) {
      throw new RuntimeException("Failed to read key file.", e);
    } catch (GeneralSecurityException e) {
      throw new RuntimeException("SSL setup failed.", e);
    }

    ServerSetup setup = new ServerSetup(
        0,  // TODO(John Sirois): unused, fix ServerSetup constructors
        new MesosAdmin.Processor(schedulerThriftInterface),
        ThriftServer.BINARY_PROTOCOL.get());
    setup.setSocket(serverSocket);
    schedulerThriftServer.start(setup);

    Command shutdown = new Command() {
      @Override public void execute() {
        LOG.info("Stopping thrift server.");
        schedulerThriftServer.shutdown();
      }
    };

    return LocalService.primaryService(schedulerThriftServer.getListeningPort(), shutdown);
  }
}

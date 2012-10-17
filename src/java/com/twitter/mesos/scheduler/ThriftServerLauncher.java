package com.twitter.mesos.scheduler;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.logging.Logger;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;

import com.google.inject.Inject;

import com.twitter.common.application.modules.LifecycleModule.ServiceRunner;
import com.twitter.common.application.modules.LocalServiceRegistry.LocalService;
import com.twitter.common.base.Command;
import com.twitter.common.thrift.ThriftServer;
import com.twitter.common.thrift.ThriftServer.ServerSetup;
import com.twitter.mesos.gen.MesosAdmin;
import com.twitter.mesos.gen.MesosAdmin.Iface;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Service launcher that starts up and registers the scheduler thrift server as a primary service
 * for the application.
 */
class ThriftServerLauncher implements ServiceRunner {

  private static final Logger LOG = Logger.getLogger(ThriftServerLauncher.class.getName());

  private final ThriftConfiguration configuration;

  interface ThriftConfiguration {
    InputStream getSslKeyStream() throws IOException;

    int getServingPort();
  }

  // Security is enforced via file permissions, not via this password, for what it's worth.
  private static final String SSL_KEYFILE_PASSWORD = "MesosKeyStorePassword";

  private final Iface schedulerThriftInterface;
  private final ThriftServer schedulerThriftServer;

  @Inject
  ThriftServerLauncher(
      Iface schedulerThriftInterface,
      ThriftServer schedulerThriftServer,
      ThriftConfiguration configuration) {

    this.schedulerThriftInterface = checkNotNull(schedulerThriftInterface);
    this.schedulerThriftServer = checkNotNull(schedulerThriftServer);
    this.configuration = checkNotNull(configuration);
  }

  @Override
  public LocalService launch() {
    // TODO(wickman): Add helper to science thrift to perform this keyfile import.
    SSLServerSocket serverSocket;
    try {
      KeyStore ks = KeyStore.getInstance("JKS");
      ks.load(configuration.getSslKeyStream(), SSL_KEYFILE_PASSWORD.toCharArray());

      KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
      kmf.init(ks, SSL_KEYFILE_PASSWORD.toCharArray());

      SSLContext ctx = SSLContext.getInstance("TLS");
      ctx.init(kmf.getKeyManagers(), null, null);

      SSLServerSocketFactory ssf = ctx.getServerSocketFactory();
      serverSocket = (SSLServerSocket) ssf.createServerSocket(configuration.getServingPort());
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

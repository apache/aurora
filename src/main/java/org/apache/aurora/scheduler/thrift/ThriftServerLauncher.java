/*
 * Copyright 2013 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.thrift;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;

import com.google.common.base.Optional;

import com.twitter.common.application.modules.LifecycleModule.ServiceRunner;
import com.twitter.common.application.modules.LocalServiceRegistry.LocalService;
import com.twitter.common.base.Command;

import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.AuroraAdmin.Iface;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Service launcher that starts up and registers the scheduler thrift server as a primary service
 * for the application.
 */
class ThriftServerLauncher implements ServiceRunner {

  private static final Logger LOG = Logger.getLogger(ThriftServerLauncher.class.getName());

  private final ThriftConfiguration configuration;

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
    ServerSocket socket = getServerSocket();
    schedulerThriftServer.start(
        socket,
        new AuroraAdmin.Processor<>(schedulerThriftInterface));

    Command shutdown = new Command() {
      @Override public void execute() {
        LOG.info("Stopping thrift server.");
        schedulerThriftServer.shutdown();
      }
    };

    return LocalService.primaryService(socket.getLocalPort(), shutdown);
  }

  private ServerSocket getServerSocket() {
    try {
      Optional<? extends InputStream> sslKeyStream = configuration.getSslKeyStream();
      if (!sslKeyStream.isPresent()) {
        LOG.warning("Running Thrift Server without SSL.");
        return new ServerSocket(configuration.getServingPort());
      } else {
        // TODO(Kevin Sweeney): Add helper to perform this keyfile import.
        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(sslKeyStream.get(), SSL_KEYFILE_PASSWORD.toCharArray());

        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        kmf.init(ks, SSL_KEYFILE_PASSWORD.toCharArray());

        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(kmf.getKeyManagers(), null, null);

        SSLServerSocketFactory ssf = ctx.getServerSocketFactory();
        SSLServerSocket serverSocket = (SSLServerSocket) ssf.createServerSocket(
            configuration.getServingPort());
        serverSocket.setEnabledCipherSuites(serverSocket.getSupportedCipherSuites());
        serverSocket.setNeedClientAuth(false);
        return serverSocket;
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read key file.", e);
    } catch (GeneralSecurityException e) {
      throw new RuntimeException("SSL setup failed.", e);
    }
  }
}

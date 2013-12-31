/**
 * Copyright 2013 Apache Software Foundation
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

import java.net.ServerSocket;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.twitter.thrift.Status;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;

class ThriftServer {
  private static final Logger LOG = Logger.getLogger(ThriftServer.class.getName());

  private TServer server = null;

  // Current health status of the server.
  private Status status = Status.STARTING;

  /**
   * Starts the server.
   * This may be called at any point except when the server is already alive.  That is, it's
   * allowable to start, stop, and re-start the server.
   *
   * @param socket The socket to use.
   * @param processor The processor to handle requests.
   */
  public synchronized void start(ServerSocket socket, TProcessor processor) {
    Preconditions.checkNotNull(socket);
    Preconditions.checkNotNull(processor);
    Preconditions.checkState(status != Status.ALIVE, "Server must only be started once.");
    setStatus(Status.ALIVE);
    TThreadPoolServer.Args args = new TThreadPoolServer.Args(new TServerSocket(socket))
        .processor(processor)
        .protocolFactory(new TBinaryProtocol.Factory(false, true));

    final TServer starting = new TThreadPoolServer(args);
    server = starting;
    LOG.info("Starting thrift server on port " + socket.getLocalPort());

    Thread listeningThread = new ThreadFactoryBuilder().setDaemon(false).build()
        .newThread(new Runnable() {
          @Override public void run() {
            try {
              starting.serve();
            } catch (Throwable t) {
              LOG.log(Level.WARNING,
                  "Uncaught exception while attempting to handle service requests: " + t, t);
              setStatus(Status.DEAD);
            }
          }
    });

    listeningThread.start();
  }

  private synchronized void setStatus(Status status) {
    LOG.info("Moving from status " + this.status + " to " + status);
    this.status = status;
  }

  /**
   * Attempts to shut down the server.
   * The server may be shut down at any time, though the request will be ignored if the server is
   * already stopped.
   */
  public synchronized void shutdown() {
    if (status == Status.STOPPED) {
      LOG.info("Server already stopped, shutdown request ignored.");
      return;
    }

    LOG.info("Received shutdown request, stopping server.");
    setStatus(Status.STOPPING);

    // TODO(William Farner): Figure out what happens to queued / in-process requests when the server
    // is stopped.  Might want to allow a sleep period for the active requests to be completed.

    if (server != null) {
      server.stop();
    }

    server = null;
    setStatus(Status.STOPPED);
  }
}

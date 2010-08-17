package com.twitter.mesos.executor;

/**
 * Manages a range of TCP sockets that can be leased and returned.
 *
 * TODO(wfarner): It may be useful in the future to have this be a service launched on a machine
 *    to ensure there is only one manager per machine, and mitigate race conditions between checking
 *    availability of and leasing sockets.
 *
 * @author wfarner
 */
public interface SocketManager {
  /**
   * Attempts to lease an available socket. The socket manager will look for an unleased socket and
   * also make an effort to verify that it can be opened before returning.
   *
   * @return An unused socket.
   * @throws com.twitter.mesos.executor.SocketManager.SocketLeaseException If no available sockets
   *    were found.
   */
  int leaseSocket() throws SocketLeaseException;

  /**
   * Returns a leased socket to the manager. Returned sockets are available immediately for leasing.
   *
   * @param socket The socket to return.
   */
  void returnSocket(int socket);

  public static class SocketLeaseException extends Exception {
    public SocketLeaseException(String msg) {
      super(msg);
    }
  }
}

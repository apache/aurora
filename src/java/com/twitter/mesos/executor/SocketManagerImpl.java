package com.twitter.mesos.executor;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of a {@link SocketManager}.
 *
 * @author wfarner
 */
public class SocketManagerImpl implements SocketManager {
  private static Logger LOG = Logger.getLogger(SocketManagerImpl.class.getName());

  // The minimum allowed socket number.
  private static final int MIN_SOCKET_NUMBER = 1;
  private static final int MAX_SOCKET_NUMBER = 0xFFFF;

  // Minimum socket number managed by this manager.
  private final int minSocket;

  // Maximum socket number managed by this manager.
  private final int maxSocket;

  // All sockets that are currently leased through this manager.
  private final Set<Integer> leasedSockets = Sets.newHashSet();

  /**
   * Creates a new socket manager with a managed socket range.
   *
   * @param minSocket Minimum socket number to manage.
   * @param maxSocket Maximum socket number to manage.
   */
  public SocketManagerImpl(int minSocket, int maxSocket) {
    Preconditions.checkArgument(minSocket > MIN_SOCKET_NUMBER);
    Preconditions.checkArgument(maxSocket > minSocket);
    Preconditions.checkArgument(maxSocket < MAX_SOCKET_NUMBER);

    this.minSocket = minSocket;
    this.maxSocket = maxSocket;
  }

  /**
   * Attempts to lease an available socket. The socket manager will look for an unleased socket and
   * also make an effort to verify that it can be opened before returning.
   *
   * @return An unused socket.
   * @throws SocketLeaseException If no available sockets were found.
   */
  @Override
  public synchronized int leaseSocket() throws SocketLeaseException {
    for (int i = minSocket; i <= maxSocket; i++) {
      if (!leasedSockets.contains(i) && available(i)) {
        leasedSockets.add(i);
        return i;
      }
    }

    throw new SocketLeaseException("No available sockets!");
  }

  /**
   * Returns a leased socket to the manager. Returned sockets are available immediately for leasing.
   *
   * @param socket The socket to return.
   */
  @Override
  public synchronized void returnSocket(int socket) {
    leasedSockets.remove(socket);
  }

  /**
   * Tests whether a socket is available by opening it.
   *
   * @param socket The socket to test for availability.
   * @return {@code true} if the socket is available, {@code false} otherwise.
   */
  private static boolean available(int socket) {
    Preconditions.checkArgument(socket > MIN_SOCKET_NUMBER);
    Preconditions.checkArgument(socket < MAX_SOCKET_NUMBER);

    ServerSocket ss = null;
    try {
      ss = new ServerSocket();
      ss.setReuseAddress(true);
      ss.bind(new InetSocketAddress(socket));
      return true;
    } catch (IOException e) {
      // Unable to open socket.
    } finally {
      if (ss != null) {
        try {
            ss.close();
        } catch (IOException e) {
          LOG.log(Level.WARNING, "Failed to close socket while testing availability.", e);
        }
      }
    }

    return false;
  }
}

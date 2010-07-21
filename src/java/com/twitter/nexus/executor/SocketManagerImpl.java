package com.twitter.nexus.executor;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
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

  // Default interval on which to run a reaper thread to reclaim leased/unused sockets.
  private static final Amount<Long, Time> DEFAULT_RECLAIM_INTERVAL = Amount.of(30L, Time.SECONDS);

  // Minimum socket number managed by this manager.
  private final int minSocket;

  // Maximum socket number managed by this manager.
  private final int maxSocket;

  // All sockets that are currently leased through this manager.
  private final Set<Integer> leasedSockets = Sets.newHashSet();

  /**
   * Creates a new socket manager with a managed socket range and a socket reclaim interval.
   *
   * @param minSocket Minimum socket number to manage.
   * @param maxSocket Maximum socket number to manage.
   * @param reclaimInterval Interval on which to reclaim unused sockets.
   */
  public SocketManagerImpl(int minSocket, int maxSocket, Amount<Long, Time> reclaimInterval) {
    Preconditions.checkArgument(minSocket > MIN_SOCKET_NUMBER);
    Preconditions.checkArgument(maxSocket > minSocket);
    Preconditions.checkArgument(maxSocket < MAX_SOCKET_NUMBER);
    Preconditions.checkNotNull(reclaimInterval);

    this.minSocket = minSocket;
    this.maxSocket = maxSocket;

    new Timer("SocketManagerReaper", true).scheduleAtFixedRate(new TimerTask() {
      @Override public void run() {
        reclaimUnusedSockets();
      }
    }, reclaimInterval.as(Time.MILLISECONDS), reclaimInterval.as(Time.MILLISECONDS));
  }

  /**
   * Creates a new socket manager with a managed socket range and the default socket reclaim
   * interval.
   *
   * @param minSocket Minimum socket number to manage.
   * @param maxSocket Maximum socket number to manage.
   */
  public SocketManagerImpl(int minSocket, int maxSocket) {
    this(minSocket, maxSocket, DEFAULT_RECLAIM_INTERVAL);
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
   * Looks through all leased sockets to find any that appear to be unused.  Unused socket are
   * reclaimed and will be available again for leasing.
   */
  private synchronized void reclaimUnusedSockets() {
    Iterables.removeIf(leasedSockets, new Predicate<Integer>() {
      @Override public boolean apply(Integer socket) {
        if (available(socket)) {
          LOG.info("Reclaiming leased but unused socket " + socket);
          return true;
        }

        return false;
      }
    });
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

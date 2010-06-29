package com.twitter.nexus.executor;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages a range of TCP sockets that can be leased and returned.
 *
 * @author wfarner
 */
public class SocketManager {
  private static Logger LOG = Logger.getLogger(SocketManager.class.getName());

  // The minimum allowed port number.
  private static final int MIN_PORT_NUMBER = 1;
  private static final int MAX_PORT_NUMBER = 0xFFFF;

  // Default interval on which to run a reaper thread to reclaim leased/unused ports.
  private static final Amount<Long, Time> DEFAULT_RECLAIM_INTERVAL = Amount.of(30L, Time.SECONDS);

  // Minimum port number managed by this manager.
  private final int minPort;

  // Maximum port number managed by this manager.
  private final int maxPort;

  // All sockets that are currently leased through this manager.
  private final Set<Integer> leasedSockets = Sets.newHashSet();

  /**
   * Creates a new socket manager with a managed port range and a port reclaim interval.
   *
   * @param minPort Minimum port number to manage.
   * @param maxPort Maximum port number to manage.
   * @param reclaimInterval Interval on which to reclaim unused ports.
   */
  public SocketManager(int minPort, int maxPort, Amount<Long, Time> reclaimInterval) {
    Preconditions.checkArgument(minPort > MIN_PORT_NUMBER);
    Preconditions.checkArgument(maxPort > minPort);
    Preconditions.checkArgument(maxPort < MAX_PORT_NUMBER);
    Preconditions.checkNotNull(reclaimInterval);

    this.minPort = minPort;
    this.maxPort = maxPort;

    new Timer("SocketManagerReaper", true).scheduleAtFixedRate(new TimerTask() {
      @Override public void run() {
        reclaimUnusedPorts();
      }
    }, reclaimInterval.as(Time.MILLISECONDS), reclaimInterval.as(Time.MILLISECONDS));
  }

  /**
   * Creates a new socket manager with a managed port range and the default port reclaim interval.
   *
   * @param minPort Minimum port number to manage.
   * @param maxPort Maximum port number to manage.
   */
  public SocketManager(int minPort, int maxPort) {
    this(minPort, maxPort, DEFAULT_RECLAIM_INTERVAL);
  }

  /**
   * Attempts to lease an available socket. The socket manager will look for an unleased port and
   * also make an effort to verify that it can be opened before returning.
   *
   * @return An unused socket.
   * @throws SocketLeaseException If no available sockets were found.
   */
  public synchronized int leaseSocket() throws SocketLeaseException {
    for (int i = minPort; i <= maxPort; i++) {
      if (!leasedSockets.contains(i) && available(i)) {
        leasedSockets.add(i);
        return i;
      }
    }

    throw new SocketLeaseException("No available sockets!");
  }

  /**
   * Returns a leased socket to the manager. Returned ports are available immediately for leasing.
   *
   * @param portId The socket to return.
   */
  public synchronized void returnPort(int portId) {
    leasedSockets.remove(portId);
  }

  /**
   * Looks through all leased ports to find any that appear to be unused.  Unused ports are
   * reclaimed and will be available again for leasing.
   */
  private synchronized void reclaimUnusedPorts() {
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
   * Tests whether a port is available by opening it.
   *
   * @param port The port to test for availability.
   * @return {@code true} if the port is available, {@code false} otherwise.
   */
  private static boolean available(int port) {
    Preconditions.checkArgument(port > MIN_PORT_NUMBER);
    Preconditions.checkArgument(port < MAX_PORT_NUMBER);

    ServerSocket ss = null;
    try {
      ss = new ServerSocket();
      ss.setReuseAddress(true);
      ss.bind(new InetSocketAddress(port));
      return true;
    } catch (IOException e) {
      // Unable to open port.
    } finally {
      if (ss != null) {
        try {
            ss.close();
        } catch (IOException e) {
          LOG.log(Level.WARNING, "Failed to close port while testing availability.", e);
        }
      }
    }

    return false;
  }

  public static class SocketLeaseException extends Exception {
    public SocketLeaseException(String msg) {
      super(msg);
    }
  }
}

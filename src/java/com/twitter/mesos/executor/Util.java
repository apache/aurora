package com.twitter.mesos.executor;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.base.Throwables;

/**
 * @author William Farner
 */
public final class Util {

  private static final Logger LOG = Logger.getLogger(Util.class.getName());

  private Util() {
    // Utility.
  }

  /**
   * Gets the local hostname.
   *
   * @return The machine hostname, or {@code null} if the hostname could not be retrieved.
   */
  @Nullable
  static String getHostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.log(Level.SEVERE, "Failed to look up own hostname.", e);
      throw Throwables.propagate(e);
    }
  }
}

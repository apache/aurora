package com.twitter.mesos.scheduler;

import com.google.common.base.Preconditions;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.twitter.common.logging.RootLogConfig;
import com.twitter.common.logging.RootLogConfig.Configuration;
import com.twitter.common.logging.log4j.GlogLayout;

/**
 * Configures log4j logging.
 */
final class Log4jConfigurator {
  private static final java.util.logging.Logger LOG =
      java.util.logging.Logger.getLogger(Log4jConfigurator.class.getName());

  /**
   * Configures log4j to log to stderr with a glog format.
   *
   * @param glogConfig The glog configuration in effect.
   * @throws IllegalArgumentException If the {@code glogConfig} is not configured for stderr
   *     logging.
   */
  static void configureConsole(Configuration glogConfig) {
    Preconditions.checkNotNull(glogConfig);
    Preconditions.checkArgument(glogConfig.isLogToStderr() || glogConfig.isAlsoLogToStderr());

    BasicConfigurator.configure(
        new ConsoleAppender(new GlogLayout(), ConsoleAppender.SYSTEM_ERR));
    Logger.getRootLogger().setLevel(getLevel(glogConfig));
  }

  private static Level getLevel(RootLogConfig.Configuration logConfig) {
    switch (logConfig.getVlog()) {
      case FINEST: // fall through
      case FINER: // fall through
      case FINE: // fall through
      case CONFIG:
        return Level.TRACE;
      case INFO:
        return Level.INFO;
      case WARNING:
        return Level.WARN;
      case SEVERE:
        return Level.ERROR;
      default:
        LOG.warning("Mapping unexpected vlog value of " + logConfig.getVlog() + " to log4j TRACE");
        return Level.TRACE;
    }
  }

  private Log4jConfigurator() {
    // Utility class.
  }
}

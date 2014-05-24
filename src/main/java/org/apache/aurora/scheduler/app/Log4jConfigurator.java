/**
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
package org.apache.aurora.scheduler.app;

import com.google.common.base.Preconditions;
import com.twitter.common.logging.RootLogConfig;
import com.twitter.common.logging.RootLogConfig.Configuration;
import com.twitter.common.logging.log4j.GlogLayout;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

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
   */
  static void configureConsole(Configuration glogConfig) {
    Preconditions.checkNotNull(glogConfig);

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

/**
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
package org.apache.aurora.common.application.modules;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.inject.AbstractModule;

import org.apache.aurora.common.application.AppLauncher;
import org.apache.aurora.common.stats.Stats;

/**
 * Binding module for the bare minimum requirements for the
 * {@link AppLauncher}.
 *
 * @author William Farner
 */
public class AppLauncherModule extends AbstractModule {

  private static final Logger LOG = Logger.getLogger(AppLauncherModule.class.getName());
  private static final AtomicLong UNCAUGHT_EXCEPTIONS = Stats.exportLong("uncaught_exceptions");

  @Override
  protected void configure() {
    bind(UncaughtExceptionHandler.class).to(LoggingExceptionHandler.class);
  }

  public static class LoggingExceptionHandler implements UncaughtExceptionHandler {
    @Override public void uncaughtException(Thread t, Throwable e) {
      UNCAUGHT_EXCEPTIONS.incrementAndGet();
      LOG.log(Level.SEVERE, "Uncaught exception from " + t + ":" + e, e);
    }
  }
}

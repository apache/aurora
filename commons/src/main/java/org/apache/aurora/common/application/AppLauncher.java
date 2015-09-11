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
package org.apache.aurora.common.application;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.inject.Guice;
import com.google.inject.Injector;

import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.ArgFilters;
import org.apache.aurora.common.args.ArgScanner;
import org.apache.aurora.common.args.ArgScanner.ArgScanException;
import org.apache.aurora.common.args.CmdLine;

/**
 * An application launcher that sets up a framework for pluggable binding modules.  This class
 * should be called directly as the main class, with a command line argument {@code -app_class}
 * which is the canonical class name of the application to execute.
 *
 * If your application uses command line arguments all {@link Arg} fields annotated with
 * {@link CmdLine} will be discovered and command line arguments will be validated against this set,
 * parsed and applied.
 */
public final class AppLauncher {

  private static final Logger LOG = Logger.getLogger(AppLauncher.class.getName());

  private AppLauncher() {
    // This should not be invoked directly.
  }

  private void run(Application application) {
    Lifecycle lifecycle = null;
    try {
      Injector injector = Guice.createInjector(application.getModules());
      lifecycle = injector.getInstance(Lifecycle.class);
      injector.injectMembers(application);
      try {
        application.run();
      } finally {
        LOG.info("Application run() exited.");
      }
    } finally {
      if (lifecycle != null) {
        lifecycle.shutdown();
      }
    }
  }

  /**
   * Used to launch an application with a restricted set of {@literal @CmdLine} {@link Arg}s
   * considered for parsing.  This is useful if the classpath includes annotated fields you do not
   * wish arguments to be parsed for.
   *
   * @param appClass The application class to instantiate and launch.
   * @param args The command line arguments to parse.
   * @see ArgFilters
   */
  public static void launch(Class<? extends Application> appClass, String... args) {
    Preconditions.checkNotNull(appClass);
    Preconditions.checkNotNull(args);

    try {
      if (!new ArgScanner().parse(Arrays.asList(args))) {
        System.exit(0);
      }
    } catch (ArgScanException e) {
      exit("Failed to scan arguments", e);
    } catch (IllegalArgumentException e) {
      exit("Failed to apply arguments", e);
    }

    try {
      new AppLauncher().run(appClass.newInstance());
    } catch (InstantiationException e) {
      throw new IllegalStateException(e);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void exit(String message, Exception error) {
    LOG.log(Level.SEVERE, message + "\n" + error, error);
    System.exit(1);
  }
}

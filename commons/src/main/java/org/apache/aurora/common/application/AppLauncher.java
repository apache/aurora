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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.util.Modules;

import org.apache.aurora.common.application.modules.AppLauncherModule;
import org.apache.aurora.common.application.modules.LifecycleModule;
import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.ArgFilters;
import org.apache.aurora.common.args.ArgScanner;
import org.apache.aurora.common.args.ArgScanner.ArgScanException;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.common.base.ExceptionalCommand;

/**
 * An application launcher that sets up a framework for pluggable binding modules.  This class
 * should be called directly as the main class, with a command line argument {@code -app_class}
 * which is the canonical class name of the application to execute.
 *
 * If your application uses command line arguments all {@link Arg} fields annotated with
 * {@link CmdLine} will be discovered and command line arguments will be validated against this set,
 * parsed and applied.
 *
 * A bootstrap module will be automatically applied ({@link AppLauncherModule}), which provides
 * overridable default bindings for things like quit/abort hooks and a health check function.
 * A {@link LifecycleModule} is also automatically applied to perform startup and shutdown
 * actions.
 */
public final class AppLauncher {

  private static final Logger LOG = Logger.getLogger(AppLauncher.class.getName());

  private AppLauncher() {
    // This should not be invoked directly.
  }

  private void run(Application application) {
    Lifecycle lifecycle = null;
    try {
      Iterable<Module> modules = ImmutableList.<Module>builder()
          .add(new LifecycleModule())
          .add(new AppLauncherModule())
          .addAll(application.getModules())
          .build();

      Injector injector = Guice.createInjector(Modules.combine(modules));

      ExceptionalCommand startupCommand =
          injector.getInstance(Key.get(ExceptionalCommand.class, StartupStage.class));
      lifecycle = injector.getInstance(Lifecycle.class);

      injector.injectMembers(application);

      LOG.info("Executing startup actions.");
      try {
        startupCommand.execute();
      } catch (Exception e) {
        LOG.log(Level.SEVERE, "Startup action failed, quitting.", e);
        throw Throwables.propagate(e);
      }

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

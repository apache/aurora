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
package org.apache.aurora.scheduler.app;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Set;

import com.google.common.collect.FluentIterable;
import com.google.inject.Module;

import org.apache.aurora.scheduler.config.CliOptions;

/**
 * A utility class for managing guice modules.
 */
public final class MoreModules {
  private MoreModules() {
    // Utility class
  }

  /**
   * Instantiate a module, supplying command line options if accepted by the module class.
   * <p>
   * Reflectively instantiates a module, first invoking a constructor that accepts
   * {@link CliOptions} as the only parameter, falling back to a default constructor if options are
   * not accepted by the class.
   *
   * @param moduleClass Module to instantiate
   * @param options Options to provide the module.
   * @return An instance of the module class.
   */
  public static Module instantiate(Class<?> moduleClass, CliOptions options) {
    try {
      // If it exists, use the constructor accepting CliOptions.
      try {
        Constructor<?> constructor = moduleClass.getConstructor(CliOptions.class);
        return (Module) constructor.newInstance(options);
      } catch (NoSuchMethodException e) {
        // Fall back to default constructor.
        return (Module) moduleClass.newInstance();
      } catch (InvocationTargetException e) {
        throw new IllegalArgumentException(
            String.format("Failed to invoke %s(CliOption)", moduleClass.getName()),
            e);
      }
    } catch (InstantiationException e) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to instantiate module %s."
                  + "Dynamic modules must have a default constructor or accept CliOptions",
              moduleClass.getName()),
          e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to instantiate module %s. Are you sure it's public?",
              moduleClass.getName()),
          e);
    }
  }

  /**
   * Identical to {@link #instantiate(Class, CliOptions)} for multiple module classes.
   */
  @SuppressWarnings("rawtypes")
  public static Set<Module> instantiateAll(List<Class> moduleClasses, CliOptions options) {
    return FluentIterable.from(moduleClasses).transform(c -> instantiate(c, options)).toSet();
  }
}

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

import com.google.inject.AbstractModule;
import com.google.inject.Module;

/**
 * A utility class for managing guice modules.
 */
public final class MoreModules {
  private MoreModules() {
    // Utility class
  }

  private static Module instantiateModule(final Class<? extends Module> moduleClass) {
    try {
      return moduleClass.newInstance();
    } catch (InstantiationException e) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to instantiate module %s. Are you sure it has a no-arg constructor?",
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

  static Module getModule(Class<? extends Module> moduleClass) {
    return instantiateModule(moduleClass);
  }

  /**
   * Creates a module that will lazily instantiate and install another module.
   * <p/>
   * This serves as an indirection between module procurement and installation, which is necessary
   * in cases where a module is referenced within a static initializer.  In this scenario, a module
   * must not be instantiated if it reads command line arguments, as the args system has not yet
   * had a chance to populate them.
   *
   * @param moduleClass Module to install.
   * @return An installer that will install {@code moduleClass}.
   */
  public static Module lazilyInstantiated(final Class<? extends Module> moduleClass) {
    return new AbstractModule() {
      @Override
      protected void configure() {
        install(getModule(moduleClass));
      }

      @Override
      public String toString() {
        return moduleClass.toString();
      }
    };
  }
}

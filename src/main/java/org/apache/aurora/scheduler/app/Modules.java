/*
 * Copyright 2013 Twitter, Inc.
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
package com.twitter.aurora.scheduler.app;

import com.google.inject.Module;
import com.google.inject.PrivateModule;

/**
 * A utility class for managing guice modules.
 */
final class Modules {

  private Modules() {
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

  // Defensively wrap each module provided on the command-line in a PrivateModule that only
  // exposes requested classes to ensure that we don't depend on surprise extra bindings across
  // different implementations.
  static Module wrapInPrivateModule(
      Class<? extends Module> moduleClass,
      final Iterable<Class<?>> exposedClasses) {

    final Module module = instantiateModule(moduleClass);
    return new PrivateModule() {
      @Override protected void configure() {
        install(module);
        for (Class<?> klass : exposedClasses) {
          expose(klass);
        }
      }
    };
  }

  static Module getModule(Class<? extends Module> moduleClass) {
    return instantiateModule(moduleClass);
  }
}

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
package org.apache.aurora.scheduler.storage.db;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.twitter.common.stats.StatsProvider;

import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;

/**
 * Utility class for creating ad-hoc storage instances.
 */
public final class DbUtil {

  private DbUtil() {
    // Utility class.
  }

  /**
   * Creates a new, empty storage system.  Identical to {@link #createStorage()}, except this
   * returns the {@link Injector} that has bindings for the new storage.
   *
   * @return An injector with bindings necessary for a storage system.
   */
  public static Injector createStorageInjector() {
    Injector injector = Guice.createInjector(
        DbModule.testModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            FakeStatsProvider stats = new FakeStatsProvider();
            bind(StatsProvider.class).toInstance(stats);
            bind(FakeStatsProvider.class).toInstance(stats);
          }
        });
    Storage storage = injector.getInstance(Storage.class);
    storage.prepare();
    return injector;
  }

  /**
   * Creates a new, empty storage system.
   *
   * @return A new storage instance.
   */
  public static Storage createStorage() {
    return createStorageInjector().getInstance(Storage.class);
  }
}

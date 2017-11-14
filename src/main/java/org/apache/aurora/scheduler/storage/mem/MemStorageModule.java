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
package org.apache.aurora.scheduler.storage.mem;

import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;

import org.apache.aurora.common.inject.Bindings.KeyFactory;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.CronJobStore;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.SchedulerStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.Volatile;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.mem.MemTaskStore.SlowQueryThreshold;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;

import static java.util.Objects.requireNonNull;

/**
 * Binding module for in-memory stores.
 * <p>
 * NOTE: These stores are being phased out in favor of database-backed stores.
 */
public final class MemStorageModule extends PrivateModule {

  private final KeyFactory keyFactory;

  public MemStorageModule() {
    this(KeyFactory.PLAIN);
  }

  public MemStorageModule(KeyFactory keyFactory) {
    this.keyFactory = requireNonNull(keyFactory);
  }

  private <T> void bindStore(Class<T> binding, Class<? extends T> impl) {
    bind(binding).to(impl);
    bind(impl).in(Singleton.class);
    Key<T> key = Key.get(binding, Volatile.class);
    bind(key).to(impl);
    expose(key);
    expose(binding);
  }

  @Override
  protected void configure() {
    bind(new TypeLiteral<Amount<Long, Time>>() { }).annotatedWith(SlowQueryThreshold.class)
        .toInstance(Amount.of(25L, Time.MILLISECONDS));
    bindStore(TaskStore.Mutable.class, MemTaskStore.class);
    bindStore(CronJobStore.Mutable.class, MemCronJobStore.class);
    bindStore(AttributeStore.Mutable.class, MemAttributeStore.class);
    bindStore(QuotaStore.Mutable.class, MemQuotaStore.class);
    bindStore(SchedulerStore.Mutable.class, MemSchedulerStore.class);
    bindStore(JobUpdateStore.Mutable.class, MemJobUpdateStore.class);

    Key<Storage> storageKey = keyFactory.create(Storage.class);
    bind(storageKey).to(MemStorage.class);
    bind(MemStorage.class).in(Singleton.class);
    expose(storageKey);
  }

  /**
   * Creates a new empty in-memory storage for use in testing.
   */
  @VisibleForTesting
  public static Storage newEmptyStorage() {
    Injector injector = Guice.createInjector(
        new MemStorageModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(StatsProvider.class).to(FakeStatsProvider.class);
            bind(FakeStatsProvider.class).in(Singleton.class);
          }
        });

    Storage storage = injector.getInstance(Storage.class);
    storage.prepare();
    return storage;
  }
}

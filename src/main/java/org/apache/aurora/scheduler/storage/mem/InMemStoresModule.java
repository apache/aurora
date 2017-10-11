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

import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;

import org.apache.aurora.common.inject.Bindings.KeyFactory;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.scheduler.storage.CronJobStore;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.db.DbModule;
import org.apache.aurora.scheduler.storage.db.DbModule.Options;
import org.apache.aurora.scheduler.storage.mem.MemTaskStore.SlowQueryThreshold;

import static java.util.Objects.requireNonNull;

/**
 * Binding module for in-memory stores.
 * <p>
 * NOTE: These stores are being phased out in favor of database-backed stores.
 */
public final class InMemStoresModule extends PrivateModule {

  private final Options options;
  private final KeyFactory keyFactory;

  public InMemStoresModule(DbModule.Options options, KeyFactory keyFactory) {
    this.options = requireNonNull(options);
    this.keyFactory = requireNonNull(keyFactory);
  }

  private <T> void bindStore(Class<T> binding, Class<? extends T> impl) {
    bind(binding).to(impl);
    bind(impl).in(Singleton.class);
    Key<T> key = keyFactory.create(binding);
    bind(key).to(impl);
    expose(key);
  }

  @Override
  protected void configure() {
    bind(new TypeLiteral<Amount<Long, Time>>() { }).annotatedWith(SlowQueryThreshold.class)
        .toInstance(options.slowQueryLogThreshold);
    bindStore(TaskStore.Mutable.class, MemTaskStore.class);
    expose(TaskStore.Mutable.class);
    bindStore(CronJobStore.Mutable.class, MemCronJobStore.class);
    expose(CronJobStore.Mutable.class);
  }
}

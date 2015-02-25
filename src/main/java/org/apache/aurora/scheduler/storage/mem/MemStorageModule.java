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
import com.twitter.common.inject.Bindings.KeyFactory;

import org.apache.aurora.scheduler.storage.CronJobStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.Volatile;
import org.apache.aurora.scheduler.storage.TaskStore;

import static java.util.Objects.requireNonNull;

/**
 * Binding module for an in-memory storage system.
 * <p>
 * Exposes bindings for storage components:
 * <ul>
 *   <li>{@link org.apache.aurora.scheduler.storage.Storage}</li>
 *   <li>Keyed with keys provided by the provided{@code keyFactory}:</li>
 *     <ul>
 *       <li>{@link org.apache.aurora.scheduler.storage.SchedulerStore}</li>
 *       <li>{@link org.apache.aurora.scheduler.storage.CronJobStore}</li>
 *       <li>{@link org.apache.aurora.scheduler.storage.TaskStore}</li>
 *       <li>{@link org.apache.aurora.scheduler.storage.LockStore}</li>
 *       <li>{@link org.apache.aurora.scheduler.storage.QuotaStore}</li>
 *       <li>{@link org.apache.aurora.scheduler.storage.AttributeStore}</li>
 *     </ul>
 * </ul>
 */
public final class MemStorageModule extends PrivateModule {

  private final KeyFactory keyFactory;

  public MemStorageModule(KeyFactory keyFactory) {
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
    Key<Storage> storageKey = keyFactory.create(Storage.class);
    bind(storageKey).to(MemStorage.class);
    expose(storageKey);
    Key<Storage> exposedMemStorageKey = Key.get(Storage.class, Volatile.class);
    bind(exposedMemStorageKey).to(MemStorage.class);
    expose(exposedMemStorageKey);
    bind(MemStorage.class).in(Singleton.class);

    bindStore(CronJobStore.Mutable.class, MemJobStore.class);
    bindStore(TaskStore.Mutable.class, MemTaskStore.class);
  }
}

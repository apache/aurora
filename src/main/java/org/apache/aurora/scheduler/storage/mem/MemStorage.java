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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.inject.Inject;
import javax.inject.Qualifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.twitter.common.inject.Bindings;
import com.twitter.common.inject.TimedInterceptor.Timed;

import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.JobStore;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.LockStore;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.SchedulerStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.db.DbModule;

import static java.util.Objects.requireNonNull;

/**
 * A storage implementation comprised of individual in-memory store implementations.
 */
public class MemStorage implements Storage {
  private final MutableStoreProvider storeProvider;
  private final Storage delegatedStore;

  /**
   * Identifies a storage layer to be delegated to instead of mem storage.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.PARAMETER, ElementType.METHOD })
  @Qualifier
  public @interface Delegated { }

  @VisibleForTesting
  static final String THREADS_WAITING_GAUGE = "storage_lock_threads_waiting";

  @Inject
  MemStorage(
      @Delegated final SchedulerStore.Mutable schedulerStore,
      final JobStore.Mutable jobStore,
      final TaskStore.Mutable taskStore,
      @Delegated final LockStore.Mutable lockStore,
      @Delegated final Storage delegated,
      @Delegated final QuotaStore.Mutable quotaStore,
      @Delegated final AttributeStore.Mutable attributeStore,
      @Delegated final JobUpdateStore.Mutable updateStore) {

    this.delegatedStore = requireNonNull(delegated);
    storeProvider = new MutableStoreProvider() {
      @Override
      public SchedulerStore.Mutable getSchedulerStore() {
        return schedulerStore;
      }

      @Override
      public JobStore.Mutable getJobStore() {
        return jobStore;
      }

      @Override
      public TaskStore getTaskStore() {
        return taskStore;
      }

      @Override
      public TaskStore.Mutable getUnsafeTaskStore() {
        return taskStore;
      }

      @Override
      public LockStore.Mutable getLockStore() {
        return lockStore;
      }

      @Override
      public QuotaStore.Mutable getQuotaStore() {
        return quotaStore;
      }

      @Override
      public AttributeStore.Mutable getAttributeStore() {
        return attributeStore;
      }

      @Override
      public JobUpdateStore.Mutable getJobUpdateStore() {
        return updateStore;
      }
    };
  }

  /**
   * Creates a new empty in-memory storage for use in testing.
   */
  @VisibleForTesting
  public static Storage newEmptyStorage() {
    Injector injector = Guice.createInjector(
        DbModule.testModule(Bindings.annotatedKeyFactory(Delegated.class)),
        new MemStorageModule(Bindings.annotatedKeyFactory(Volatile.class)));
    Storage storage = injector.getInstance(Key.get(Storage.class, Volatile.class));
    storage.prepare();
    return storage;
  }

  @Timed("mem_storage_read_operation")
  @Override
  public <T, E extends Exception> T read(final Work<T, E> work)
          throws StorageException, E {
    return delegatedStore.read(new Work<T, E>() {
      @Override
      public T apply(StoreProvider provider) throws E {
        return work.apply(storeProvider);
      }
    });
  }

  @Timed("mem_storage_write_operation")
  @Override
  public <T, E extends Exception> T write(final MutateWork<T, E> work) throws StorageException, E {
    return delegatedStore.write(new MutateWork<T, E>() {
      @Override
      public T apply(MutableStoreProvider provider) throws E {
        return work.apply(storeProvider);
      }
    });
  }

  @Override
  public void prepare() throws StorageException {
    delegatedStore.prepare();
  }
}

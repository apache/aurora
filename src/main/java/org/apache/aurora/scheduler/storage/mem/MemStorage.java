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

import javax.inject.Inject;

import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.CronJobStore;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.SchedulerStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.TaskStore;

/**
 * A storage implementation comprised of individual in-memory store implementations.
 */
public class MemStorage implements Storage {
  private final MutableStoreProvider storeProvider;

  @Inject
  MemStorage(
      @Volatile final SchedulerStore.Mutable schedulerStore,
      @Volatile final CronJobStore.Mutable jobStore,
      @Volatile final TaskStore.Mutable taskStore,
      @Volatile final QuotaStore.Mutable quotaStore,
      @Volatile final AttributeStore.Mutable attributeStore,
      @Volatile final JobUpdateStore.Mutable updateStore) {

    storeProvider = new MutableStoreProvider() {
      @Override
      public SchedulerStore.Mutable getSchedulerStore() {
        return schedulerStore;
      }

      @Override
      public CronJobStore.Mutable getCronJobStore() {
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

  @Timed("mem_storage_read_operation")
  @Override
  public <T, E extends Exception> T read(final Work<T, E> work) throws StorageException, E {
    return work.apply(storeProvider);
  }

  @Timed("mem_storage_write_operation")
  @Override
  public <T, E extends Exception> T write(final MutateWork<T, E> work) throws StorageException, E {
    return work.apply(storeProvider);
  }

  @Override
  public void prepare() {
    // No-op.
  }
}

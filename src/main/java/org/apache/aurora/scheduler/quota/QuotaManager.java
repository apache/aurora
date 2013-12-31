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
package org.apache.aurora.scheduler.quota;

import com.google.common.collect.Iterables;
import com.google.inject.Inject;

import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.Storage.Work;
import org.apache.aurora.scheduler.storage.Storage.Work.Quiet;
import org.apache.aurora.scheduler.storage.entities.IQuota;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.common.base.MorePreconditions.checkNotBlank;

/**
 * Allows access to resource quotas, and tracks quota consumption.
 */
public interface QuotaManager {
  /**
   * Fetches the current resource usage for the role.
   *
   * @param role to fetch quota usage for.
   * @return Resource quota used by {@code role}.
   */
  IQuota getConsumption(String role);

  /**
   * Quota provider that stores quotas in the canonical {@link Storage} system.
   */
  static class QuotaManagerImpl implements QuotaManager {
    private final Storage storage;

    @Inject
    public QuotaManagerImpl(Storage storage) {
      this.storage = checkNotNull(storage);
    }

    @Override
    public IQuota getConsumption(final String role) {
      checkNotBlank(role);

      final Query.Builder query = Query.roleScoped(role).active();

      return storage.consistentRead(
          new Work.Quiet<IQuota>() {
            @Override public IQuota apply(StoreProvider storeProvider) {
              return Quotas.fromProductionTasks(Iterables.transform(
                  storeProvider.getTaskStore().fetchTasks(query), Tasks.SCHEDULED_TO_INFO));
            }
          });
    }

    /**
     * Tests whether the role has at least the specified amount of quota available.
     *
     * @param role Role to consume quota for.
     * @param quota Quota amount to check for availability.
     * @return QuotaComparisonResult with {@code result()} returning {@code true} if the role
     * currently has at least {@code quota} quota remaining, {@code false} otherwise.
     */
    QuotaComparisonResult checkQuota(final String role, final IQuota quota) {
      checkNotBlank(role);
      checkNotNull(quota);

      return storage.consistentRead(new Quiet<QuotaComparisonResult>() {
        @Override public QuotaComparisonResult apply(StoreProvider storeProvider) {
          IQuota reserved = storeProvider.getQuotaStore().fetchQuota(role).or(Quotas.noQuota());
          return Quotas.greaterOrEqual(reserved, Quotas.add(getConsumption(role), quota));
        }
      });
    }
  }
}

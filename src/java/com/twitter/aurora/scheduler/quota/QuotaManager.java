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
package com.twitter.aurora.scheduler.quota;

import java.util.Collection;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

import com.twitter.aurora.gen.JobUpdateConfiguration;
import com.twitter.aurora.gen.TaskUpdateConfiguration;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.Shards;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.aurora.scheduler.storage.Storage.StoreProvider;
import com.twitter.aurora.scheduler.storage.Storage.Work;
import com.twitter.aurora.scheduler.storage.Storage.Work.Quiet;
import com.twitter.aurora.scheduler.storage.entities.IQuota;
import com.twitter.aurora.scheduler.storage.entities.ITaskConfig;

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
   * Tests whether the role has at least the specified amount of quota available.
   *
   * @param role Role to consume quota for.
   * @param quota Quota amount to check for availability.
   * @return {@code true} if the role currently has at least {@code quota} quota remaining,
   *     {@code false} otherwise.
   */
  boolean hasRemaining(String role, IQuota quota);

  /**
   * Quota provider that stores quotas in the canonical {@link Storage} system.
   */
  static class QuotaManagerImpl implements QuotaManager {

    private final Storage storage;

    @Inject
    public QuotaManagerImpl(Storage storage) {
      this.storage = checkNotNull(storage);
    }

    private static IQuota getUpdateQuota(
        Collection<TaskUpdateConfiguration> configs,
        Function<TaskUpdateConfiguration, ITaskConfig> taskExtractor) {

      FluentIterable<ITaskConfig> tasks =
          FluentIterable
          .from(configs)
          .transform(taskExtractor)
          .filter(Predicates.notNull());
      return Quotas.fromProductionTasks(tasks);
    }

    @Override
    public IQuota getConsumption(final String role) {
      checkNotBlank(role);

      final Query.Builder query = Query.roleScoped(role).active();

      return storage.consistentRead(
          new Work.Quiet<IQuota>() {
            @Override public IQuota apply(StoreProvider storeProvider) {
              IQuota quota = Quotas.fromProductionTasks(Iterables.transform(
                  storeProvider.getTaskStore().fetchTasks(query), Tasks.SCHEDULED_TO_INFO));

              for (JobUpdateConfiguration updateConfig
                  : storeProvider.getUpdateStore().fetchUpdateConfigs(role)) {
                // If the user is performing an update that increases the quota for the job,
                // bill them for the updated job.
                IQuota additionalQuota = Quotas.subtract(
                    getUpdateQuota(updateConfig.getConfigs(), Shards.GET_NEW_CONFIG),
                    getUpdateQuota(updateConfig.getConfigs(), Shards.GET_ORIGINAL_CONFIG)
                );
                if (Quotas.greaterThan(additionalQuota, Quotas.noQuota())) {
                  quota = Quotas.add(quota, additionalQuota);
                }
              }

              return quota;
            }
          });
    }

    @Override
    public boolean hasRemaining(final String role, final IQuota quota) {
      checkNotBlank(role);
      checkNotNull(quota);

      return storage.consistentRead(new Quiet<Boolean>() {
        @Override public Boolean apply(StoreProvider storeProvider) {
          IQuota reserved = storeProvider.getQuotaStore().fetchQuota(role).or(Quotas.noQuota());
          return Quotas.geq(reserved, Quotas.add(getConsumption(role), quota));
        }
      });
    }
  }
}

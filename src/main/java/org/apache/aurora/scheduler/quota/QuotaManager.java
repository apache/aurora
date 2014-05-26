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
package org.apache.aurora.scheduler.quota;

import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.ResourceAggregates;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.Storage.Work;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static com.google.common.base.Preconditions.checkNotNull;

import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.SUFFICIENT_QUOTA;

/**
 * Allows access to resource quotas, and tracks quota consumption.
 */
public interface QuotaManager {
  /**
   * Saves a new quota for the provided role or overrides the existing one.
   *
   * @param role Quota owner.
   * @param quota Quota to save.
   * @throws QuotaException If provided quota specification is invalid.
   */
  void saveQuota(String role, IResourceAggregate quota) throws QuotaException;

  /**
   * Gets {@code QuotaInfo} for the specified role.
   *
   * @param role Quota owner.
   * @return {@code QuotaInfo} instance.
   */
  QuotaInfo getQuotaInfo(String role);

  /**
   * Checks if there is enough resource quota available for adding {@code instances}
   * of {@code template} tasks. The quota is defined at the task owner (role) level.
   *
   * @param template Single task resource requirements.
   * @param instances Number of task instances.
   * @return {@code QuotaComparisonResult} instance with quota check result details.
   */
  QuotaCheckResult checkQuota(ITaskConfig template, int instances);

  /**
   * Thrown when quota related operation failed.
   */
  class QuotaException extends Exception {
    public QuotaException(String msg) {
      super(msg);
    }
  }

  /**
   * Quota provider that stores quotas in the canonical {@link Storage} system.
   */
  class QuotaManagerImpl implements QuotaManager {
    private final Storage storage;

    @Inject
    QuotaManagerImpl(Storage storage) {
      this.storage = checkNotNull(storage);
    }

    @Override
    public void saveQuota(final String ownerRole, final IResourceAggregate quota)
        throws QuotaException {

      if (!quota.isSetNumCpus() || !quota.isSetRamMb() || !quota.isSetDiskMb()) {
        throw new QuotaException("Missing quota specification(s) in: " + quota.toString());
      }

      if (quota.getNumCpus() < 0.0 || quota.getRamMb() < 0 || quota.getDiskMb() < 0) {
        throw new QuotaException("Negative values in: " + quota.toString());
      }

      storage.write(new Storage.MutateWork.NoResult.Quiet() {
        @Override
        protected void execute(Storage.MutableStoreProvider storeProvider) {
          storeProvider.getQuotaStore().saveQuota(ownerRole, quota);
        }
      });
    }

    @Override
    public QuotaInfo getQuotaInfo(final String role) {
      return storage.consistentRead(new Work.Quiet<QuotaInfo>() {
        @Override
        public QuotaInfo apply(StoreProvider storeProvider) {
          FluentIterable<ITaskConfig> tasks = FluentIterable
              .from(storeProvider.getTaskStore().fetchTasks(Query.roleScoped(role).active()))
              .transform(Tasks.SCHEDULED_TO_INFO);

          IResourceAggregate prodConsumed = fromTasks(tasks.filter(Tasks.IS_PRODUCTION));
          IResourceAggregate nonProdConsumed =
              fromTasks(tasks.filter(Predicates.not(Tasks.IS_PRODUCTION)));

          IResourceAggregate quota =
              storeProvider.getQuotaStore().fetchQuota(role).or(ResourceAggregates.none());

          return new QuotaInfo(quota, prodConsumed, nonProdConsumed);
        }
      });
    }

    @Override
    public QuotaCheckResult checkQuota(ITaskConfig template, int instances) {
      if (!template.isProduction()) {
        return new QuotaCheckResult(SUFFICIENT_QUOTA);
      }

      QuotaInfo quotaInfo = getQuotaInfo(JobKeys.from(template).getRole());

      IResourceAggregate additionalRequested =
          ResourceAggregates.scale(fromTasks(ImmutableSet.of(template)), instances);

      return QuotaCheckResult.greaterOrEqual(
          quotaInfo.guota(),
          add(quotaInfo.prodConsumption(), additionalRequested));
    }

    private static IResourceAggregate fromTasks(Iterable<ITaskConfig> tasks) {
      double cpu = 0;
      int ramMb = 0;
      int diskMb = 0;
      for (ITaskConfig task : tasks) {
        cpu += task.getNumCpus();
        ramMb += task.getRamMb();
        diskMb += task.getDiskMb();
      }

      return IResourceAggregate.build(new ResourceAggregate()
          .setNumCpus(cpu)
          .setRamMb(ramMb)
          .setDiskMb(diskMb));
    }

    private static IResourceAggregate add(IResourceAggregate a, IResourceAggregate b) {
      return IResourceAggregate.build(new ResourceAggregate()
          .setNumCpus(a.getNumCpus() + b.getNumCpus())
          .setRamMb(a.getRamMb() + b.getRamMb())
          .setDiskMb(a.getDiskMb() + b.getDiskMb()));
    }
  }
}

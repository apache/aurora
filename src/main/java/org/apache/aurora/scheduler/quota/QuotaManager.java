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

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.ResourceAggregates;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.Storage.Work;
import org.apache.aurora.scheduler.storage.entities.IInstanceTaskConfig;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;
import org.apache.aurora.scheduler.storage.entities.IRange;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.updater.JobUpdateController;

import static java.util.Objects.requireNonNull;

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
   * Checks if there is enough resource quota available for adding production resources specified
   * in {@code requestedProdResources}. The quota is defined at the task owner (role) level.
   *
   * @param role Role to check quota for.
   * @param requestedProdResources Additional production resources requested.
   * @return {@code QuotaComparisonResult} instance with quota check result details.
   */
  QuotaCheckResult checkQuota(String role, IResourceAggregate requestedProdResources);

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
      this.storage = requireNonNull(storage);
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
          FluentIterable<IScheduledTask> tasks = FluentIterable.from(
              storeProvider.getTaskStore().fetchTasks(Query.roleScoped(role).active()));

          IResourceAggregate prodConsumed =
              getProdConsumption(storeProvider.getJobUpdateStore(), role, tasks);

          IResourceAggregate nonProdConsumed = QuotaUtil.fromTasks(
              tasks.transform(Tasks.SCHEDULED_TO_INFO).filter(Predicates.not(Tasks.IS_PRODUCTION)));

          IResourceAggregate quota =
              storeProvider.getQuotaStore().fetchQuota(role).or(ResourceAggregates.none());

          return new QuotaInfo(quota, prodConsumed, nonProdConsumed);
        }
      });
    }

    @Override
    public QuotaCheckResult checkQuota(String role, IResourceAggregate requestedProdResources) {
      if (ResourceAggregates.EMPTY.equals(requestedProdResources)) {
        return new QuotaCheckResult(SUFFICIENT_QUOTA);
      }

      QuotaInfo quotaInfo = getQuotaInfo(role);

      return QuotaCheckResult.greaterOrEqual(
          quotaInfo.getQuota(),
          add(quotaInfo.getProdConsumption(), requestedProdResources));
    }

    private IResourceAggregate getProdConsumption(
        JobUpdateStore jobUpdateStore,
        String role,
        FluentIterable<IScheduledTask> tasks) {

      // The algorithm here is as follows:
      // 1. Load all production active tasks that belong to jobs without active updates OR
      //    unaffected by an active update working set. An example of the latter would be instances
      //    not updated by the update due to being already in desired state or outside of update
      //    range (e.g. not in JobUpdateInstructions.updateOnlyTheseInstances).
      //    Calculate consumed resources as "nonUpdateConsumption".
      //
      // 2. Calculate consumed resources from instances affected by the active job updates as
      //    "updateConsumption".
      //
      // 3. Add up the two to yield total prod consumption.

      final Map<IJobKey, IJobUpdate> roleJobUpdates =
          fetchActiveJobUpdates(jobUpdateStore, role).uniqueIndex(UPDATE_TO_JOB_KEY);

      IResourceAggregate nonUpdateConsumption = QuotaUtil.prodResourcesFromTasks(tasks
          .filter(buildNonUpdatingTasksFilter(roleJobUpdates))
          .transform(Tasks.SCHEDULED_TO_INFO));

      IResourceAggregate updateConsumption = ResourceAggregates.EMPTY;
      for (IJobUpdate update : roleJobUpdates.values()) {
        updateConsumption = add(updateConsumption, QuotaUtil.prodResourcesFromJobUpdate(update));
      }

      return add(nonUpdateConsumption, updateConsumption);
    }

    private static Predicate<IScheduledTask> buildNonUpdatingTasksFilter(
        final Map<IJobKey, IJobUpdate> roleJobUpdates) {

      return new Predicate<IScheduledTask>() {
        @Override
        public boolean apply(IScheduledTask input) {
          Optional<IJobUpdate> update = Optional.fromNullable(
              roleJobUpdates.get(JobKeys.from(input.getAssignedTask().getTask())));

          if (update.isPresent()) {
            IInstanceTaskConfig configs =
                update.get().getInstructions().getDesiredState();
            RangeSet<Integer> desiredInstances = rangesToRangeSet(configs.getInstances());

            return !desiredInstances.contains(input.getAssignedTask().getInstanceId());
          }
          return true;
        }
      };
    }

    private static final Function<IJobUpdate, IJobKey> UPDATE_TO_JOB_KEY =
        new Function<IJobUpdate, IJobKey>() {
          @Override
          public IJobKey apply(IJobUpdate input) {
            return input.getSummary().getJobKey();
          }
        };

    private static FluentIterable<IJobUpdate> fetchActiveJobUpdates(
        JobUpdateStore jobUpdateStore,
        String role) {

      List<IJobUpdateSummary> summaries = jobUpdateStore.fetchJobUpdateSummaries(updateQuery(role));

      Set<IJobUpdate> updates = Sets.newHashSet();
      for (IJobUpdateSummary summary : summaries) {
        updates.add(jobUpdateStore.fetchJobUpdate(summary.getUpdateId()).get());
      }

      return FluentIterable.from(updates);
    }

    @VisibleForTesting
    static IJobUpdateQuery updateQuery(String role) {
      return IJobUpdateQuery.build(new JobUpdateQuery()
          .setRole(role)
          .setUpdateStatuses(JobUpdateController.ACTIVE_JOB_UPDATE_STATES));
    }

    private static RangeSet<Integer> rangesToRangeSet(Set<IRange> ranges) {
      ImmutableRangeSet.Builder<Integer> builder = ImmutableRangeSet.builder();
      for (IRange range : ranges) {
        builder.add(Range.closed(range.getFirst(), range.getLast()));
      }

      return builder.build();
    }

    private static IResourceAggregate add(IResourceAggregate a, IResourceAggregate b) {
      return IResourceAggregate.build(new ResourceAggregate()
          .setNumCpus(a.getNumCpus() + b.getNumCpus())
          .setRamMb(a.getRamMb() + b.getRamMb())
          .setDiskMb(a.getDiskMb() + b.getDiskMb()));
    }
  }
}

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
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
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
import org.apache.aurora.scheduler.storage.entities.IJobUpdateInstructions;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;
import org.apache.aurora.scheduler.storage.entities.IRange;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
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
   * Checks if there is enough resource quota available for adding {@code instances} of
   * {@code template} tasks provided resources consumed by {@code releasedTemplates} tasks
   * are released. The quota is defined at the task owner (role) level.
   *
   * @param template Task resource requirement.
   * @param instances Number of additional instances requested.
   * @return {@code QuotaComparisonResult} instance with quota check result details.
   */
  QuotaCheckResult checkInstanceAddition(ITaskConfig template, int instances);

  /**
   * Checks if there is enough resource quota available for performing a job update represented
   * by the {@code jobUpdate}. The quota is defined at the task owner (role) level.
   *
   * @param jobUpdate Job update to check quota for.
   * @return {@code QuotaComparisonResult} instance with quota check result details.
   */
  QuotaCheckResult checkJobUpdate(IJobUpdate jobUpdate);

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
      return getQuotaInfo(role, Optional.<IJobUpdate>absent());
    }

    @Override
    public QuotaCheckResult checkInstanceAddition(ITaskConfig template, int instances) {
      Preconditions.checkArgument(instances >= 0);
      if (!template.isProduction()) {
        return new QuotaCheckResult(SUFFICIENT_QUOTA);
      }

      QuotaInfo quotaInfo = getQuotaInfo(template.getOwner().getRole());

      return QuotaCheckResult.greaterOrEqual(
          quotaInfo.getQuota(),
          add(quotaInfo.getProdConsumption(), ResourceAggregates.scale(
              prodResourcesFromTasks(ImmutableSet.of(template)), instances)));
    }

    @Override
    public QuotaCheckResult checkJobUpdate(IJobUpdate jobUpdate) {
      requireNonNull(jobUpdate);
      if (!jobUpdate.getInstructions().isSetDesiredState()
          || !jobUpdate.getInstructions().getDesiredState().getTask().isProduction()) {

        return new QuotaCheckResult(SUFFICIENT_QUOTA);
      }

      QuotaInfo quotaInfo = getQuotaInfo(
          jobUpdate.getSummary().getJobKey().getRole(),
          Optional.of(jobUpdate));

      return QuotaCheckResult.greaterOrEqual(quotaInfo.getQuota(), quotaInfo.getProdConsumption());
    }

    /**
     * Gets QuotaInfo with currently allocated quota and actual consumption data.
     * <p>
     * In case an optional {@code requestedUpdate} is specified, the production consumption returned
     * also includes an estimated resources share of that update as if it was already in progress.
     *
     * @param role Role to get quota info for.
     * @param requestedUpdate An optional {@code IJobUpdate} to forecast the prod consumption.
     * @return {@code QuotaInfo} with quota and consumption details.
     */
    private QuotaInfo getQuotaInfo(final String role, final Optional<IJobUpdate> requestedUpdate) {
      return storage.consistentRead(new Work.Quiet<QuotaInfo>() {
        @Override
        public QuotaInfo apply(StoreProvider storeProvider) {
          FluentIterable<IScheduledTask> tasks = FluentIterable.from(
              storeProvider.getTaskStore().fetchTasks(Query.roleScoped(role).active()));

          IResourceAggregate prodConsumed =
              getProdConsumption(storeProvider.getJobUpdateStore(), role, tasks, requestedUpdate);

          // TODO(maxim): Consider a similar update-aware approach for computing nonProdConsumed.
          IResourceAggregate nonProdConsumed = fromTasks(
              tasks.transform(Tasks.SCHEDULED_TO_INFO).filter(Predicates.not(Tasks.IS_PRODUCTION)));

          IResourceAggregate quota =
              storeProvider.getQuotaStore().fetchQuota(role).or(ResourceAggregates.none());

          return new QuotaInfo(quota, prodConsumed, nonProdConsumed);
        }
      });
    }

    private IResourceAggregate getProdConsumption(
        JobUpdateStore jobUpdateStore,
        String role,
        FluentIterable<IScheduledTask> tasks,
        Optional<IJobUpdate> requestedUpdate) {

      // The algorithm here is as follows:
      // 1. Load all production active tasks that belong to jobs without active updates OR
      //    unaffected by an active update working set. An example of the latter would be instances
      //    not updated by the update due to being already in desired state or outside of update
      //    range (e.g. not in JobUpdateInstructions.updateOnlyTheseInstances).
      //    Calculate consumed resources as "nonUpdateConsumption".
      //
      // 2. Mix in a requested job update (if present) to correctly calculate prod consumption.
      //    This would be an update that is not saved in the store yet (i.e. the one quota is
      //    checked for).
      //
      // 3. Calculate consumed resources from instances affected by the active job updates as
      //    "updateConsumption".
      //
      // 4. Add up the two to yield total prod consumption.

      Map<IJobKey, IJobUpdate> updatesByKey = Maps.newHashMap(
          fetchActiveJobUpdates(jobUpdateStore, role).uniqueIndex(UPDATE_TO_JOB_KEY));

      if (requestedUpdate.isPresent()) {
        updatesByKey.put(requestedUpdate.get().getSummary().getJobKey(), requestedUpdate.get());
      }

      IResourceAggregate nonUpdateConsumption = prodResourcesFromTasks(tasks
          .filter(buildNonUpdatingTasksFilter(updatesByKey))
          .transform(Tasks.SCHEDULED_TO_INFO));

      IResourceAggregate updateConsumption = ResourceAggregates.EMPTY;
      for (IJobUpdate update : updatesByKey.values()) {
        updateConsumption = add(updateConsumption, toProdResources(update.getInstructions()));
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
            IJobUpdateInstructions instructions = update.get().getInstructions();
            RangeSet<Integer> initialInstances = instanceRangeSet(instructions.getInitialState());
            RangeSet<Integer> desiredInstances = instanceRangeSet(instructions.isSetDesiredState()
                ? ImmutableSet.of(instructions.getDesiredState())
                : ImmutableSet.<IInstanceTaskConfig>of());

            int instanceId = input.getAssignedTask().getInstanceId();
            return !initialInstances.contains(instanceId) && !desiredInstances.contains(instanceId);
          }
          return true;
        }
      };
    }

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

    private static RangeSet<Integer> instanceRangeSet(Set<IInstanceTaskConfig> configs) {
      ImmutableRangeSet.Builder<Integer> builder = ImmutableRangeSet.builder();
      for (IInstanceTaskConfig config : configs) {
        for (IRange range : config.getInstances()) {
          builder.add(Range.closed(range.getFirst(), range.getLast()));
        }
      }

      return builder.build();
    }

    private static IResourceAggregate add(IResourceAggregate a, IResourceAggregate b) {
      return IResourceAggregate.build(new ResourceAggregate()
          .setNumCpus(a.getNumCpus() + b.getNumCpus())
          .setRamMb(a.getRamMb() + b.getRamMb())
          .setDiskMb(a.getDiskMb() + b.getDiskMb()));
    }

    /**
     * This function calculates max aggregate production resources consumed by the job update
     * {@code instructions}. The max is calculated between existing and desired task configs on per
     * resource basis. This means max CPU, RAM and DISK values are computed individually and may
     * come from different task configurations. While it may not be the most accurate
     * representation of job update resources during the update, it does guarantee none of the
     * individual resource values is exceeded during the forward/back roll.
     *
     * @param instructions Update instructions with resource definitions.
     * @return Resources consumed by the update.
     */
    private static IResourceAggregate toProdResources(IJobUpdateInstructions instructions) {
      double existingCpu = 0;
      int existingRamMb = 0;
      int existingDiskMb = 0;
      for (IInstanceTaskConfig group : instructions.getInitialState()) {
        ITaskConfig task = group.getTask();
        if (task.isProduction()) {
          for (IRange range : group.getInstances()) {
            int numInstances = range.getLast() - range.getFirst() + 1;
            existingCpu += task.getNumCpus() * numInstances;
            existingRamMb += task.getRamMb() * numInstances;
            existingDiskMb += task.getDiskMb() * numInstances;
          }
        }
      }

      // Calculate desired prod task consumption.
      IResourceAggregate desired = Optional.fromNullable(instructions.getDesiredState())
          .transform(TO_PROD_RESOURCES).or(ResourceAggregates.EMPTY);

      // Calculate result as max(existing, desired) per resource.
      return IResourceAggregate.build(new ResourceAggregate()
          .setNumCpus(Math.max(existingCpu, desired.getNumCpus()))
          .setRamMb(Math.max(existingRamMb, desired.getRamMb()))
          .setDiskMb(Math.max(existingDiskMb, desired.getDiskMb())));
    }

    private static IResourceAggregate prodResourcesFromTasks(Iterable<ITaskConfig> tasks) {
      return fromTasks(FluentIterable.from(tasks).filter(Tasks.IS_PRODUCTION));
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

    private static final Function<IInstanceTaskConfig, IResourceAggregate> TO_PROD_RESOURCES =
        new Function<IInstanceTaskConfig, IResourceAggregate>() {
          @Override
          public IResourceAggregate apply(IInstanceTaskConfig input) {
            return input.getTask().isProduction()
                ? ResourceAggregates.scale(
                prodResourcesFromTasks(ImmutableSet.of(input.getTask())),
                getUpdateInstanceCount(input.getInstances()))
                : ResourceAggregates.EMPTY;
          }
        };

    private static final Function<IJobUpdate, IJobKey> UPDATE_TO_JOB_KEY =
        new Function<IJobUpdate, IJobKey>() {
          @Override
          public IJobKey apply(IJobUpdate input) {
            return input.getSummary().getJobKey();
          }
        };

    private static int getUpdateInstanceCount(Set<IRange> ranges) {
      int instanceCount = 0;
      for (IRange range : ranges) {
        instanceCount += range.getLast() - range.getFirst() + 1;
      }

      return instanceCount;
    }
  }
}

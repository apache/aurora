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
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;

import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.ResourceAggregates;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.entities.IInstanceTaskConfig;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateInstructions;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;
import org.apache.aurora.scheduler.storage.entities.IRange;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.updater.Updates;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.base.ResourceAggregates.EMPTY;
import static org.apache.aurora.scheduler.base.Tasks.INFO_TO_JOB_KEY;
import static org.apache.aurora.scheduler.base.Tasks.IS_PRODUCTION;
import static org.apache.aurora.scheduler.base.Tasks.SCHEDULED_TO_INFO;
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
   * @param quoteStore A quota store.
   * @throws QuotaException If provided quota specification is invalid.
   */
  void saveQuota(String role, IResourceAggregate quota, QuotaStore.Mutable quoteStore)
      throws QuotaException;

  /**
   * Gets {@code QuotaInfo} for the specified role.
   *
   * @param role Quota owner.
   * @param storeProvider A store provider to access quota data.
   * @return {@code QuotaInfo} instance.
   */
  QuotaInfo getQuotaInfo(String role, StoreProvider storeProvider);

  /**
   * Checks if there is enough resource quota available for adding {@code instances} of
   * {@code template} tasks provided resources consumed by {@code releasedTemplates} tasks
   * are released. The quota is defined at the task owner (role) level.
   *
   * @param template Task resource requirement.
   * @param instances Number of additional instances requested.
   * @param storeProvider A store provider to access quota data.
   * @return {@code QuotaComparisonResult} instance with quota check result details.
   */
  QuotaCheckResult checkInstanceAddition(
      ITaskConfig template,
      int instances,
      StoreProvider storeProvider);

  /**
   * Checks if there is enough resource quota available for performing a job update represented
   * by the {@code jobUpdate}. The quota is defined at the task owner (role) level.
   *
   * @param jobUpdate Job update to check quota for.
   * @param storeProvider A store provider to access quota data.
   * @return {@code QuotaComparisonResult} instance with quota check result details.
   */
  QuotaCheckResult checkJobUpdate(IJobUpdate jobUpdate, StoreProvider storeProvider);

  /**
   * Check if there is enough resource quota available for creating or updating a cron job
   * represented by the {@code cronConfig}. The quota is defined at the task owner (role) level.
   *
   * @param cronConfig Cron job configuration.
   * @param storeProvider A store provider to access quota data.
   * @return{@code QuotaComparisonResult} instance with quota check result details.
   */
  QuotaCheckResult checkCronUpdate(IJobConfiguration cronConfig, StoreProvider storeProvider);

  /**
   * Thrown when quota related operation failed.
   */
  class QuotaException extends Exception {
    public QuotaException(String msg) {
      super(msg);
    }
  }

  /**
   * Quota provider that stores quotas in the canonical store.
   */
  class QuotaManagerImpl implements QuotaManager {

    @Override
    public void saveQuota(
        final String ownerRole,
        final IResourceAggregate quota,
        QuotaStore.Mutable quoteStore) throws QuotaException {

      if (!quota.isSetNumCpus() || !quota.isSetRamMb() || !quota.isSetDiskMb()) {
        throw new QuotaException("Missing quota specification(s) in: " + quota.toString());
      }

      if (quota.getNumCpus() < 0.0 || quota.getRamMb() < 0 || quota.getDiskMb() < 0) {
        throw new QuotaException("Negative values in: " + quota.toString());
      }

      quoteStore.saveQuota(ownerRole, quota);
    }

    @Override
    public QuotaInfo getQuotaInfo(String role, StoreProvider storeProvider) {
      return getQuotaInfo(role, Optional.<IJobUpdate>absent(), storeProvider);
    }

    @Override
    public QuotaCheckResult checkInstanceAddition(
        ITaskConfig template,
        int instances,
        StoreProvider storeProvider) {

      Preconditions.checkArgument(instances >= 0);
      if (!template.isProduction()) {
        return new QuotaCheckResult(SUFFICIENT_QUOTA);
      }

      QuotaInfo quotaInfo = getQuotaInfo(template.getJob().getRole(), storeProvider);
      IResourceAggregate requestedTotal =
          add(quotaInfo.getProdConsumption(), scale(template, instances));

      return QuotaCheckResult.greaterOrEqual(quotaInfo.getQuota(), requestedTotal);
    }

    @Override
    public QuotaCheckResult checkJobUpdate(IJobUpdate jobUpdate, StoreProvider storeProvider) {
      requireNonNull(jobUpdate);
      if (!jobUpdate.getInstructions().isSetDesiredState()
          || !jobUpdate.getInstructions().getDesiredState().getTask().isProduction()) {

        return new QuotaCheckResult(SUFFICIENT_QUOTA);
      }

      QuotaInfo quotaInfo = getQuotaInfo(
          jobUpdate.getSummary().getKey().getJob().getRole(),
          Optional.of(jobUpdate),
          storeProvider);

      return QuotaCheckResult.greaterOrEqual(quotaInfo.getQuota(), quotaInfo.getProdConsumption());
    }

    @Override
    public QuotaCheckResult checkCronUpdate(
        IJobConfiguration cronConfig,
        StoreProvider storeProvider) {

      if (!cronConfig.getTaskConfig().isProduction()) {
        return new QuotaCheckResult(SUFFICIENT_QUOTA);
      }

      QuotaInfo quotaInfo =
          getQuotaInfo(cronConfig.getKey().getRole(), Optional.<IJobUpdate>absent(), storeProvider);

      Optional<IJobConfiguration> oldCron =
          storeProvider.getCronJobStore().fetchJob(cronConfig.getKey());

      IResourceAggregate oldResource = oldCron.isPresent() ? scale(oldCron.get()) : EMPTY;

      // Calculate requested total as a sum of current prod consumption and a delta between
      // new and old cron templates.
      IResourceAggregate requestedTotal = add(
          quotaInfo.getProdConsumption(),
          subtract(scale(cronConfig), oldResource));

      return QuotaCheckResult.greaterOrEqual(quotaInfo.getQuota(), requestedTotal);
    }

    /**
     * Gets QuotaInfo with currently allocated quota and actual consumption data.
     * <p>
     * In case an optional {@code requestedUpdate} is specified, the consumption returned also
     * includes an estimated resources share of that update as if it was already in progress.
     *
     * @param role Role to get quota info for.
     * @param requestedUpdate An optional {@code IJobUpdate} to forecast the consumption.
     * @param storeProvider A store provider to access quota data.
     * @return {@code QuotaInfo} with quota and consumption details.
     */
    private QuotaInfo getQuotaInfo(
        String role,
        Optional<IJobUpdate> requestedUpdate,
        StoreProvider storeProvider) {

      FluentIterable<IScheduledTask> tasks = FluentIterable.from(
          storeProvider.getTaskStore().fetchTasks(Query.roleScoped(role).active()));

      Map<IJobKey, IJobUpdate> updates = Maps.newHashMap(
          fetchActiveJobUpdates(storeProvider.getJobUpdateStore(), role)
              .uniqueIndex(UPDATE_TO_JOB_KEY));

      // Mix in a requested job update (if present) to correctly calculate consumption.
      // This would be an update that is not saved in the store yet (i.e. the one quota is
      // checked for).
      if (requestedUpdate.isPresent()) {
        updates.put(requestedUpdate.get().getSummary().getKey().getJob(), requestedUpdate.get());
      }

      Map<IJobKey, IJobConfiguration> cronTemplates =
          FluentIterable.from(storeProvider.getCronJobStore().fetchJobs())
              .filter(Predicates.compose(Predicates.equalTo(role), JobKeys.CONFIG_TO_ROLE))
              .uniqueIndex(JobKeys.FROM_CONFIG);

      IResourceAggregate prodConsumed = getConsumption(tasks, updates, cronTemplates, true);

      IResourceAggregate nonProdConsumed = getConsumption(tasks, updates, cronTemplates, false);

      IResourceAggregate quota =
          storeProvider.getQuotaStore().fetchQuota(role).or(ResourceAggregates.none());

      return new QuotaInfo(quota, prodConsumed, nonProdConsumed);
    }

    private IResourceAggregate getConsumption(
        FluentIterable<IScheduledTask> tasks,
        Map<IJobKey, IJobUpdate> updatesByKey,
        Map<IJobKey, IJobConfiguration> cronTemplatesByKey,
        boolean isProd) {

      Predicate<ITaskConfig> prodFilter = isProd ? IS_PRODUCTION : Predicates.not(IS_PRODUCTION);

      FluentIterable<IScheduledTask> filteredTasks =
          tasks.filter(Predicates.compose(prodFilter, SCHEDULED_TO_INFO));

      IResourceAggregate nonCronConsumption = getNonCronConsumption(
          updatesByKey,
          excludeCronTasks(filteredTasks, cronTemplatesByKey),
          isProd);

      IResourceAggregate cronConsumption =
          getCronConsumption(cronTemplatesByKey, filteredTasks, isProd);

      return add(nonCronConsumption, cronConsumption);
    }

    private static IResourceAggregate getNonCronConsumption(
        Map<IJobKey, IJobUpdate> updatesByKey,
        FluentIterable<IScheduledTask> tasks,
        boolean isProd) {

      // 1. Get all active tasks that belong to jobs without active updates OR unaffected by an
      //    active update working set. An example of the latter would be instances not updated by
      //    the update due to being already in desired state or outside of update range (e.g.
      //    not in JobUpdateInstructions.updateOnlyTheseInstances). Calculate consumed resources
      //    as "nonUpdateConsumption".
      //
      // 2. Calculate consumed resources from instances affected by the active job updates as
      //    "updateConsumption".
      //
      // 3. Add up the two to yield total consumption.

      IResourceAggregate nonUpdateConsumption = fromTasks(tasks
          .filter(buildNonUpdatingTasksFilter(updatesByKey))
          .transform(SCHEDULED_TO_INFO));

      IResourceAggregate updateConsumption = EMPTY;
      for (IJobUpdate update : updatesByKey.values()) {
        updateConsumption =
            add(updateConsumption, instructionsToResources(update.getInstructions(), isProd));
      }

      return add(nonUpdateConsumption, updateConsumption);
    }

    private static IResourceAggregate getCronConsumption(
        Map<IJobKey, IJobConfiguration> cronTemplates,
        FluentIterable<IScheduledTask> tasks,
        boolean isProd) {

      // Calculate the overall cron consumption as MAX between cron template resources and active
      // cron tasks. This is required to account for a case when a running cron task has higher
      // resource requirements than its updated template.
      //
      // While this is the "worst case" calculation that does not account for a possible "staggered"
      // cron scheduling, it's the simplest approach possible given the system constraints (e.g.:
      // lack of enforcement on a cron job run duration).

      Multimap<IJobKey, ITaskConfig> taskConfigsByKey =
          tasks.transform(SCHEDULED_TO_INFO).index(INFO_TO_JOB_KEY);

      IResourceAggregate totalConsumption = EMPTY;
      for (IJobConfiguration config : cronTemplates.values()) {
        if (isProd == config.getTaskConfig().isProduction()) {
          IResourceAggregate templateConsumption =
              scale(config.getTaskConfig(), config.getInstanceCount());

          IResourceAggregate taskConsumption = fromTasks(taskConfigsByKey.get(config.getKey()));

          totalConsumption = add(totalConsumption, max(templateConsumption, taskConsumption));
        }
      }
      return totalConsumption;
    }

    private static FluentIterable<IScheduledTask> excludeCronTasks(
        FluentIterable<IScheduledTask> tasks,
        final Map<IJobKey, IJobConfiguration> cronJobs) {

      return tasks.filter(new Predicate<IScheduledTask>() {
        @Override
        public boolean apply(IScheduledTask input) {
          return !cronJobs.containsKey(input.getAssignedTask().getTask().getJob());
        }
      });
    }

    private static Predicate<IScheduledTask> buildNonUpdatingTasksFilter(
        final Map<IJobKey, IJobUpdate> roleJobUpdates) {

      return new Predicate<IScheduledTask>() {
        @Override
        public boolean apply(IScheduledTask input) {
          Optional<IJobUpdate> update = Optional.fromNullable(
              roleJobUpdates.get(input.getAssignedTask().getTask().getJob()));

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
        updates.add(jobUpdateStore.fetchJobUpdate(summary.getKey()).get());
      }

      return FluentIterable.from(updates);
    }

    @VisibleForTesting
    static IJobUpdateQuery updateQuery(String role) {
      return IJobUpdateQuery.build(new JobUpdateQuery()
          .setRole(role)
          .setUpdateStatuses(Updates.ACTIVE_JOB_UPDATE_STATES));
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

    /**
     * This function calculates max aggregate resources consumed by the job update
     * {@code instructions}. The max is calculated between existing and desired task configs on per
     * resource basis. This means max CPU, RAM and DISK values are computed individually and may
     * come from different task configurations. While it may not be the most accurate
     * representation of job update resources during the update, it does guarantee none of the
     * individual resource values is exceeded during the forward/back roll.
     *
     * NOTE: In case of a job update converting the job production bit (i.e. prod -> non-prod or
     *       non-prod -> prod), only the matching state is counted towards consumption. For example,
     *       prod -> non-prod AND {@code prodConsumption=True}: only the initial state is accounted.
     *
     * @param instructions Update instructions with resource definitions.
     * @param isProd Flag indicating whether the prod or non-prod calculation requested.
     * @return Resources consumed by the update.
     */
    private static IResourceAggregate instructionsToResources(
        IJobUpdateInstructions instructions,
        final boolean isProd) {

      // Calculate initial state consumption.
      IResourceAggregate initial = EMPTY;
      for (IInstanceTaskConfig group : instructions.getInitialState()) {
        ITaskConfig task = group.getTask();
        if (isProd == task.isProduction()) {
          for (IRange range : group.getInstances()) {
            initial = add(initial, scale(task, instanceCountFromRange(range)));
          }
        }
      }

      // Calculate desired state consumption.
      IResourceAggregate desired = Optional.fromNullable(instructions.getDesiredState())
          .transform(new Function<IInstanceTaskConfig, IResourceAggregate>() {
            @Override
            public IResourceAggregate apply(IInstanceTaskConfig input) {
              return isProd == input.getTask().isProduction()
                  ? scale(input.getTask(), getUpdateInstanceCount(input.getInstances()))
                  : EMPTY;
            }
          }).or(EMPTY);

      // Calculate result as max(existing, desired) per resource type.
      return max(initial, desired);
    }

    private static IResourceAggregate add(IResourceAggregate a, IResourceAggregate b) {
      return IResourceAggregate.build(new ResourceAggregate()
          .setNumCpus(a.getNumCpus() + b.getNumCpus())
          .setRamMb(a.getRamMb() + b.getRamMb())
          .setDiskMb(a.getDiskMb() + b.getDiskMb()));
    }

    private static IResourceAggregate subtract(IResourceAggregate a, IResourceAggregate b) {
      return IResourceAggregate.build(new ResourceAggregate()
          .setNumCpus(a.getNumCpus() - b.getNumCpus())
          .setRamMb(a.getRamMb() - b.getRamMb())
          .setDiskMb(a.getDiskMb() - b.getDiskMb()));
    }

    private static IResourceAggregate max(IResourceAggregate a, IResourceAggregate b) {
      return IResourceAggregate.build(new ResourceAggregate()
          .setNumCpus(Math.max(a.getNumCpus(), b.getNumCpus()))
          .setRamMb(Math.max(a.getRamMb(), b.getRamMb()))
          .setDiskMb(Math.max(a.getDiskMb(), b.getDiskMb())));
    }

    private static IResourceAggregate scale(ITaskConfig taskConfig, int instanceCount) {
      return ResourceAggregates.scale(fromTasks(ImmutableSet.of(taskConfig)), instanceCount);
    }

    private static IResourceAggregate scale(IJobConfiguration jobConfiguration) {
      return scale(jobConfiguration.getTaskConfig(), jobConfiguration.getInstanceCount());
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

    private static final Function<IJobUpdate, IJobKey> UPDATE_TO_JOB_KEY =
        new Function<IJobUpdate, IJobKey>() {
          @Override
          public IJobKey apply(IJobUpdate input) {
            return input.getSummary().getKey().getJob();
          }
        };

    private static int getUpdateInstanceCount(Set<IRange> ranges) {
      int instanceCount = 0;
      for (IRange range : ranges) {
        instanceCount += instanceCountFromRange(range);
      }

      return instanceCount;
    }

    private static int instanceCountFromRange(IRange range) {
      return range.getLast() - range.getFirst() + 1;
    }
  }
}

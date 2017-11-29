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
package org.apache.aurora.scheduler.thrift;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

import org.apache.aurora.GuavaUtils;
import org.apache.aurora.common.base.MorePreconditions;
import org.apache.aurora.gen.ConfigGroup;
import org.apache.aurora.gen.ConfigSummary;
import org.apache.aurora.gen.ConfigSummaryResult;
import org.apache.aurora.gen.GetJobUpdateDetailsResult;
import org.apache.aurora.gen.GetJobUpdateDiffResult;
import org.apache.aurora.gen.GetJobUpdateSummariesResult;
import org.apache.aurora.gen.GetJobsResult;
import org.apache.aurora.gen.GetPendingReasonResult;
import org.apache.aurora.gen.GetQuotaResult;
import org.apache.aurora.gen.GetTierConfigResult;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobSummary;
import org.apache.aurora.gen.JobSummaryResult;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateRequest;
import org.apache.aurora.gen.PendingReason;
import org.apache.aurora.gen.PopulateJobResult;
import org.apache.aurora.gen.ReadOnlyScheduler;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.Result;
import org.apache.aurora.gen.RoleSummary;
import org.apache.aurora.gen.RoleSummaryResult;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduleStatusResult;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.gen.TierConfig;
import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Jobs;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import org.apache.aurora.scheduler.configuration.SanitizedConfiguration;
import org.apache.aurora.scheduler.cron.CronPredictor;
import org.apache.aurora.scheduler.cron.CrontabEntry;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.metadata.NearestFit;
import org.apache.aurora.scheduler.quota.QuotaInfo;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateRequest;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;
import org.apache.aurora.scheduler.storage.entities.IRange;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.updater.JobDiff;
import org.apache.thrift.TException;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.gen.ResponseCode.INVALID_REQUEST;
import static org.apache.aurora.scheduler.base.Numbers.convertRanges;
import static org.apache.aurora.scheduler.base.Numbers.toRanges;
import static org.apache.aurora.scheduler.resources.ResourceManager.aggregateFromBag;
import static org.apache.aurora.scheduler.thrift.Responses.addMessage;
import static org.apache.aurora.scheduler.thrift.Responses.error;
import static org.apache.aurora.scheduler.thrift.Responses.invalidRequest;
import static org.apache.aurora.scheduler.thrift.Responses.ok;

class ReadOnlySchedulerImpl implements ReadOnlyScheduler.Iface {
  private static final Function<Entry<ITaskConfig, Collection<Integer>>, ConfigGroup> TO_GROUP =
      input -> new ConfigGroup()
          .setConfig(input.getKey().newBuilder())
          .setInstances(IRange.toBuildersSet(convertRanges(toRanges(input.getValue()))));

  private final ConfigurationManager configurationManager;
  private final Storage storage;
  private final NearestFit nearestFit;
  private final CronPredictor cronPredictor;
  private final QuotaManager quotaManager;
  private final TierManager tierManager;

  @Inject
  ReadOnlySchedulerImpl(
      ConfigurationManager configurationManager,
      Storage storage,
      NearestFit nearestFit,
      CronPredictor cronPredictor,
      QuotaManager quotaManager,
      TierManager tierManager) {

    this.configurationManager = requireNonNull(configurationManager);
    this.storage = requireNonNull(storage);
    this.nearestFit = requireNonNull(nearestFit);
    this.cronPredictor = requireNonNull(cronPredictor);
    this.quotaManager = requireNonNull(quotaManager);
    this.tierManager = requireNonNull(tierManager);
  }

  @Override
  public Response populateJobConfig(JobConfiguration description) {
    requireNonNull(description);

    try {
      ITaskConfig populatedTaskConfig = SanitizedConfiguration.fromUnsanitized(
          configurationManager,
          IJobConfiguration.build(description)).getJobConfig().getTaskConfig();
      return ok(Result.populateJobResult(
          new PopulateJobResult().setTaskConfig(populatedTaskConfig.newBuilder())));
    } catch (TaskDescriptionException e) {
      return invalidRequest("Invalid configuration: " + e.getMessage());
    }
  }

  // TODO(William Farner): Provide status information about cron jobs here.
  @Override
  public Response getTasksStatus(TaskQuery query) {
    return ok(Result.scheduleStatusResult(
        new ScheduleStatusResult().setTasks(getTasks(query))));
  }

  @Override
  public Response getTasksWithoutConfigs(TaskQuery query) {
    List<ScheduledTask> tasks = Lists.transform(
        getTasks(query),
        task -> {
          task.getAssignedTask().getTask().unsetExecutorConfig();
          return task;
        });

    return ok(Result.scheduleStatusResult(new ScheduleStatusResult().setTasks(tasks)));
  }

  @Override
  public Response getPendingReason(TaskQuery query) throws TException {
    requireNonNull(query);

    if (query.isSetSlaveHosts() && !query.getSlaveHosts().isEmpty()) {
      return invalidRequest(
          "SlaveHosts are not supported in " + query.toString());
    }
    if (query.isSetStatuses() && !query.getStatuses().isEmpty()) {
      return invalidRequest(
          "Statuses is not supported in " + query.toString());
    }

    // Only PENDING tasks should be considered.
    query.setStatuses(ImmutableSet.of(ScheduleStatus.PENDING));

    Set<PendingReason> reasons = FluentIterable.from(getTasks(query))
        .transform(scheduledTask -> {
          TaskGroupKey groupKey = TaskGroupKey.from(
              ITaskConfig.build(scheduledTask.getAssignedTask().getTask()));

          String reason = Joiner.on(',').join(Iterables.transform(
              nearestFit.getNearestFit(groupKey),
              Veto::getReason));

          return new PendingReason()
              .setTaskId(scheduledTask.getAssignedTask().getTaskId())
              .setReason(reason);
        }).toSet();

    return ok(Result.getPendingReasonResult(new GetPendingReasonResult(reasons)));
  }

  @Override
  public Response getConfigSummary(JobKey job) throws TException {
    IJobKey jobKey = JobKeys.assertValid(IJobKey.build(job));

    Iterable<IAssignedTask> assignedTasks = Iterables.transform(
        Storage.Util.fetchTasks(storage, Query.jobScoped(jobKey).active()),
        IScheduledTask::getAssignedTask);
    Map<Integer, ITaskConfig> tasksByInstance = Maps.transformValues(
        Maps.uniqueIndex(assignedTasks, IAssignedTask::getInstanceId),
        IAssignedTask::getTask);
    Set<ConfigGroup> groups = instancesToConfigGroups(tasksByInstance);

    return ok(Result.configSummaryResult(
        new ConfigSummaryResult().setSummary(new ConfigSummary(job, groups))));
  }

  @Override
  public Response getRoleSummary() {
    Multimap<String, IJobKey> jobsByRole = storage.read(
        storeProvider ->
            Multimaps.index(storeProvider.getTaskStore().getJobKeys(), IJobKey::getRole));

    Multimap<String, IJobKey> cronJobsByRole = Multimaps.index(
        Iterables.transform(Storage.Util.fetchCronJobs(storage), IJobConfiguration::getKey),
        IJobKey::getRole);

    Set<RoleSummary> summaries = FluentIterable.from(
        Sets.union(jobsByRole.keySet(), cronJobsByRole.keySet()))
        .transform(role -> new RoleSummary(
            role,
            jobsByRole.get(role).size(),
            cronJobsByRole.get(role).size()))
        .toSet();

    return ok(Result.roleSummaryResult(new RoleSummaryResult(summaries)));
  }

  @Override
  public Response getJobSummary(@Nullable String maybeNullRole) {
    Optional<String> ownerRole = Optional.fromNullable(maybeNullRole);

    Multimap<IJobKey, IScheduledTask> tasks = getTasks(maybeRoleScoped(ownerRole));
    Map<IJobKey, IJobConfiguration> jobs = getJobs(ownerRole, tasks);

    Function<IJobKey, JobSummary> makeJobSummary = jobKey -> {
      IJobConfiguration job = jobs.get(jobKey);
      JobSummary summary = new JobSummary()
          .setJob(job.newBuilder())
          .setStats(Jobs.getJobStats(tasks.get(jobKey)).newBuilder());

      if (job.isSetCronSchedule()) {
        CrontabEntry crontabEntry = CrontabEntry.parse(job.getCronSchedule());
        Optional<Date> nextRun = cronPredictor.predictNextRun(crontabEntry);
        return nextRun.transform(date -> summary.setNextCronRunMs(date.getTime())).or(summary);
      } else {
        return summary;
      }
    };

    ImmutableSet<JobSummary> jobSummaries =
        FluentIterable.from(jobs.keySet()).transform(makeJobSummary).toSet();

    return ok(Result.jobSummaryResult(new JobSummaryResult().setSummaries(jobSummaries)));
  }

  @Override
  public Response getJobs(@Nullable String maybeNullRole) {
    Optional<String> ownerRole = Optional.fromNullable(maybeNullRole);

    return ok(Result.getJobsResult(
        new GetJobsResult()
            .setConfigs(IJobConfiguration.toBuildersSet(
                getJobs(ownerRole, getTasks(maybeRoleScoped(ownerRole).active())).values()))));
  }

  @Override
  public Response getQuota(String ownerRole) {
    MorePreconditions.checkNotBlank(ownerRole);
    return storage.read(storeProvider -> {
      QuotaInfo quotaInfo = quotaManager.getQuotaInfo(ownerRole, storeProvider);
      GetQuotaResult result = new GetQuotaResult()
          .setQuota(aggregateFromBag(quotaInfo.getQuota()).newBuilder())
          .setProdSharedConsumption(aggregateFromBag(
              quotaInfo.getProdSharedConsumption()).newBuilder())
          .setProdDedicatedConsumption(aggregateFromBag(
              quotaInfo.getProdDedicatedConsumption()).newBuilder())
          .setNonProdSharedConsumption(aggregateFromBag(
              quotaInfo.getNonProdSharedConsumption()).newBuilder())
          .setNonProdDedicatedConsumption(aggregateFromBag(
              quotaInfo.getNonProdDedicatedConsumption()).newBuilder());

      return ok(Result.getQuotaResult(result));
    });
  }

  @Override
  public Response getJobUpdateSummaries(JobUpdateQuery mutableQuery) {
    IJobUpdateQuery query = IJobUpdateQuery.build(requireNonNull(mutableQuery));

    List<IJobUpdateSummary> summaries = storage.read(
        storeProvider -> storeProvider.getJobUpdateStore()
            .fetchJobUpdates(query)
            .stream()
            .map(u -> u.getUpdate().getSummary()).collect(Collectors.toList()));

    return ok(Result.getJobUpdateSummariesResult(new GetJobUpdateSummariesResult()
        .setUpdateSummaries(IJobUpdateSummary.toBuildersList(summaries))));
  }

  @Override
  public Response getJobUpdateDetails(JobUpdateKey mutableKey, JobUpdateQuery mutableQuery) {
    if (mutableKey == null && mutableQuery == null)  {
      return error("Either key or query must be set.");
    }

    if (mutableQuery != null) {
      IJobUpdateQuery query = IJobUpdateQuery.build(mutableQuery);

      List<IJobUpdateDetails> details =
          storage.read(storeProvider -> storeProvider.getJobUpdateStore().fetchJobUpdates(query));

      return ok(Result.getJobUpdateDetailsResult(new GetJobUpdateDetailsResult()
          .setDetailsList(IJobUpdateDetails.toBuildersList(details))));
    }

    // TODO(zmanji): Remove this code once `mutableKey` is removed in AURORA-1765
    IJobUpdateKey key = IJobUpdateKey.build(mutableKey);
    Optional<IJobUpdateDetails> details = storage.read(storeProvider ->
        storeProvider.getJobUpdateStore().fetchJobUpdate(key));

    if (details.isPresent()) {
      return addMessage(ok(Result.getJobUpdateDetailsResult(
          new GetJobUpdateDetailsResult().setDetails(details.get().newBuilder()))),
          "The key argument is deprecated, use the query argument instead");
    } else {
      return invalidRequest("Invalid update: " + key);
    }
  }

  @Override
  public Response getJobUpdateDiff(JobUpdateRequest mutableRequest) {
    IJobUpdateRequest request;
    try {
      request = IJobUpdateRequest.build(new JobUpdateRequest(mutableRequest).setTaskConfig(
          configurationManager.validateAndPopulate(
              ITaskConfig.build(mutableRequest.getTaskConfig())).newBuilder()));
    } catch (TaskDescriptionException e) {
      return error(INVALID_REQUEST, e);
    }

    IJobKey job = request.getTaskConfig().getJob();

    return storage.read(storeProvider -> {
      if (storeProvider.getCronJobStore().fetchJob(job).isPresent()) {
        return invalidRequest(NO_CRON);
      }

      JobDiff diff = JobDiff.compute(
          storeProvider.getTaskStore(),
          job,
          JobDiff.asMap(request.getTaskConfig(), request.getInstanceCount()),
          request.getSettings().getUpdateOnlyTheseInstances());

      Map<Integer, ITaskConfig> replaced = diff.getReplacedInstances();
      Map<Integer, ITaskConfig> replacements = Maps.asMap(
          diff.getReplacementInstances(),
          Functions.constant(request.getTaskConfig()));

      Map<Integer, ITaskConfig> add = Maps.filterKeys(
          replacements,
          Predicates.in(Sets.difference(replacements.keySet(), replaced.keySet())));
      Map<Integer, ITaskConfig> remove = Maps.filterKeys(
          replaced,
          Predicates.in(Sets.difference(replaced.keySet(), replacements.keySet())));
      Map<Integer, ITaskConfig> update = Maps.filterKeys(
          replaced,
          Predicates.in(Sets.intersection(replaced.keySet(), replacements.keySet())));

      return ok(Result.getJobUpdateDiffResult(new GetJobUpdateDiffResult()
          .setAdd(instancesToConfigGroups(add))
          .setRemove(instancesToConfigGroups(remove))
          .setUpdate(instancesToConfigGroups(update))
          .setUnchanged(instancesToConfigGroups(diff.getUnchangedInstances()))));
    });
  }

  @Override
  public Response getTierConfigs() throws TException {
    return ok(Result.getTierConfigResult(
        new GetTierConfigResult(
            tierManager.getDefaultTierName(),
            tierManager.getTiers()
                .entrySet()
                .stream()
                .map(entry -> new TierConfig(entry.getKey(), entry.getValue().toMap()))
                .collect(GuavaUtils.toImmutableSet()))));
  }

  private static Set<ConfigGroup> instancesToConfigGroups(Map<Integer, ITaskConfig> tasks) {
    Multimap<ITaskConfig, Integer> instancesByDetails = Multimaps.invertFrom(
        Multimaps.forMap(tasks),
        HashMultimap.create());
    return ImmutableSet.copyOf(
        Iterables.transform(instancesByDetails.asMap().entrySet(), TO_GROUP));
  }

  private List<ScheduledTask> getTasks(TaskQuery query) {
    requireNonNull(query);

    Iterable<IScheduledTask> tasks = Storage.Util.fetchTasks(storage, Query.arbitrary(query));
    if (query.getOffset() > 0) {
      tasks = Iterables.skip(tasks, query.getOffset());
    }
    if (query.getLimit() > 0) {
      tasks = Iterables.limit(tasks, query.getLimit());
    }

    return IScheduledTask.toBuildersList(tasks);
  }

  private Query.Builder maybeRoleScoped(Optional<String> ownerRole) {
    return ownerRole.isPresent()
        ? Query.roleScoped(ownerRole.get())
        : Query.unscoped();
  }

  private Map<IJobKey, IJobConfiguration> getJobs(
      Optional<String> ownerRole,
      Multimap<IJobKey, IScheduledTask> tasks) {

    // We need to synthesize the JobConfiguration from the the current tasks because the
    // ImmediateJobManager doesn't store jobs directly and ImmediateJobManager#getJobs always
    // returns an empty Collection.
    Map<IJobKey, IJobConfiguration> jobs = Maps.newHashMap();

    jobs.putAll(Maps.transformEntries(tasks.asMap(),
        (jobKey, tasks1) -> {

          // Pick the latest transitioned task for each immediate job since the job can be in the
          // middle of an update or some shards have been selectively created.
          TaskConfig mostRecentTaskConfig =
              Tasks.getLatestActiveTask(tasks1).getAssignedTask().getTask().newBuilder();

          return IJobConfiguration.build(new JobConfiguration()
              .setKey(jobKey.newBuilder())
              .setOwner(mostRecentTaskConfig.getOwner())
              .setTaskConfig(mostRecentTaskConfig)
              .setInstanceCount(tasks1.size()));
        }));

    // Get cron jobs directly from the manager. Do this after querying the task store so the real
    // template JobConfiguration for a cron job will overwrite the synthesized one that could have
    // been created above.
    Predicate<IJobConfiguration> configFilter = ownerRole.isPresent()
        ? Predicates.compose(Predicates.equalTo(ownerRole.get()), JobKeys::getRole)
        : Predicates.alwaysTrue();
    jobs.putAll(Maps.uniqueIndex(
        FluentIterable.from(Storage.Util.fetchCronJobs(storage)).filter(configFilter),
        IJobConfiguration::getKey));

    return jobs;
  }

  private Multimap<IJobKey, IScheduledTask> getTasks(Query.Builder query) {
    return Tasks.byJobKey(Storage.Util.fetchTasks(storage, query));
  }

  @VisibleForTesting
  static final String NO_CRON = "Cron jobs are not supported.";
}

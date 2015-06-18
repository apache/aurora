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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nullable;
import javax.inject.Inject;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.twitter.common.base.MorePreconditions;

import org.apache.aurora.gen.ConfigGroup;
import org.apache.aurora.gen.ConfigSummary;
import org.apache.aurora.gen.ConfigSummaryResult;
import org.apache.aurora.gen.GetJobUpdateDetailsResult;
import org.apache.aurora.gen.GetJobUpdateSummariesResult;
import org.apache.aurora.gen.GetJobsResult;
import org.apache.aurora.gen.GetLocksResult;
import org.apache.aurora.gen.GetPendingReasonResult;
import org.apache.aurora.gen.GetQuotaResult;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobSummary;
import org.apache.aurora.gen.JobSummaryResult;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateQuery;
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
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Jobs;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import org.apache.aurora.scheduler.configuration.SanitizedConfiguration;
import org.apache.aurora.scheduler.cron.CronPredictor;
import org.apache.aurora.scheduler.cron.CrontabEntry;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.metadata.NearestFit;
import org.apache.aurora.scheduler.quota.QuotaInfo;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.state.LockManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.Storage.Work.Quiet;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.thrift.TException;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.thrift.Responses.invalidRequest;
import static org.apache.aurora.scheduler.thrift.Responses.ok;

class ReadOnlySchedulerImpl implements ReadOnlyScheduler.Iface {
  private static final Function<Entry<ITaskConfig, Collection<Integer>>, ConfigGroup> TO_GROUP =
      new Function<Entry<ITaskConfig, Collection<Integer>>, ConfigGroup>() {
        @Override
        public ConfigGroup apply(Entry<ITaskConfig, Collection<Integer>> input) {
          return new ConfigGroup(
              input.getKey().newBuilder(),
              ImmutableSet.copyOf(input.getValue()));
        }
      };

  private final Storage storage;
  private final NearestFit nearestFit;
  private final CronPredictor cronPredictor;
  private final QuotaManager quotaManager;
  private final LockManager lockManager;

  @Inject
  ReadOnlySchedulerImpl(
      Storage storage,
      NearestFit nearestFit,
      CronPredictor cronPredictor,
      QuotaManager quotaManager,
      LockManager lockManager) {

    this.storage = requireNonNull(storage);
    this.nearestFit = requireNonNull(nearestFit);
    this.cronPredictor = requireNonNull(cronPredictor);
    this.quotaManager = requireNonNull(quotaManager);
    this.lockManager = requireNonNull(lockManager);
  }

  @Override
  public Response populateJobConfig(JobConfiguration description) {
    requireNonNull(description);

    try {
      ITaskConfig populatedTaskConfig = SanitizedConfiguration.fromUnsanitized(
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
        new Function<ScheduledTask, ScheduledTask>() {
          @Override
          public ScheduledTask apply(ScheduledTask task) {
            task.getAssignedTask().getTask().unsetExecutorConfig();
            return task;
          }
        });

    return ok(Result.scheduleStatusResult(new ScheduleStatusResult().setTasks(tasks)));
  }

  @Override
  public Response getPendingReason(TaskQuery query) throws TException {
    requireNonNull(query);

    if (query.isSetSlaveHosts() || query.isSetStatuses()) {
      return invalidRequest(
          "Statuses or slaveHosts are not supported in " + query.toString());
    }

    // Only PENDING tasks should be considered.
    query.setStatuses(ImmutableSet.of(ScheduleStatus.PENDING));

    Set<PendingReason> reasons = FluentIterable.from(getTasks(query))
        .transform(new Function<ScheduledTask, PendingReason>() {
          @Override
          public PendingReason apply(ScheduledTask scheduledTask) {
            TaskGroupKey groupKey = TaskGroupKey.from(
                ITaskConfig.build(scheduledTask.getAssignedTask().getTask()));

            String reason = Joiner.on(',').join(Iterables.transform(
                nearestFit.getNearestFit(groupKey),
                new Function<Veto, String>() {
                  @Override
                  public String apply(Veto veto) {
                    return veto.getReason();
                  }
                }));

            return new PendingReason()
                .setTaskId(scheduledTask.getAssignedTask().getTaskId())
                .setReason(reason);
          }
        }).toSet();

    return ok(Result.getPendingReasonResult(new GetPendingReasonResult(reasons)));
  }

  @Override
  public Response getConfigSummary(JobKey job) throws TException {
    IJobKey jobKey = JobKeys.assertValid(IJobKey.build(job));

    Iterable<IAssignedTask> assignedTasks = Iterables.transform(
        Storage.Util.fetchTasks(storage, Query.jobScoped(jobKey).active()),
        Tasks.SCHEDULED_TO_ASSIGNED);
    Map<Integer, ITaskConfig> tasksByInstance = Maps.transformValues(
        Maps.uniqueIndex(assignedTasks, Tasks.ASSIGNED_TO_INSTANCE_ID),
        Tasks.ASSIGNED_TO_INFO);
    Multimap<ITaskConfig, Integer> instancesByDetails = Multimaps.invertFrom(
        Multimaps.forMap(tasksByInstance),
        HashMultimap.<ITaskConfig, Integer>create());
    Iterable<ConfigGroup> groups = Iterables.transform(
        instancesByDetails.asMap().entrySet(), TO_GROUP);

    ConfigSummary summary = new ConfigSummary(job, ImmutableSet.copyOf(groups));
    return ok(Result.configSummaryResult(new ConfigSummaryResult().setSummary(summary)));
  }

  @Override
  public Response getRoleSummary() {
    Multimap<String, IJobKey> jobsByRole = storage.read(new Quiet<Multimap<String, IJobKey>>() {
      @Override
      public Multimap<String, IJobKey> apply(StoreProvider storeProvider) {
        return Multimaps.index(storeProvider.getTaskStore().getJobKeys(), JobKeys.TO_ROLE);
      }
    });

    Multimap<String, IJobKey> cronJobsByRole = Multimaps.index(
        Iterables.transform(Storage.Util.fetchCronJobs(storage), JobKeys.FROM_CONFIG),
        JobKeys.TO_ROLE);

    Set<RoleSummary> summaries = FluentIterable.from(
        Sets.union(jobsByRole.keySet(), cronJobsByRole.keySet()))
        .transform(new Function<String, RoleSummary>() {
          @Override
          public RoleSummary apply(String role) {
            return new RoleSummary(
                role,
                jobsByRole.get(role).size(),
                cronJobsByRole.get(role).size());
          }
        })
        .toSet();

    return ok(Result.roleSummaryResult(new RoleSummaryResult(summaries)));
  }

  @Override
  public Response getJobSummary(@Nullable String maybeNullRole) {
    Optional<String> ownerRole = Optional.fromNullable(maybeNullRole);

    final Multimap<IJobKey, IScheduledTask> tasks = getTasks(maybeRoleScoped(ownerRole));
    final Map<IJobKey, IJobConfiguration> jobs = getJobs(ownerRole, tasks);

    Function<IJobKey, JobSummary> makeJobSummary = new Function<IJobKey, JobSummary>() {
      @Override
      public JobSummary apply(IJobKey jobKey) {
        IJobConfiguration job = jobs.get(jobKey);
        JobSummary summary = new JobSummary()
            .setJob(job.newBuilder())
            .setStats(Jobs.getJobStats(tasks.get(jobKey)).newBuilder());

        return Strings.isNullOrEmpty(job.getCronSchedule())
            ? summary
            : summary.setNextCronRunMs(
            cronPredictor.predictNextRun(CrontabEntry.parse(job.getCronSchedule())).getTime());
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
  public Response getQuota(final String ownerRole) {
    MorePreconditions.checkNotBlank(ownerRole);
    return storage.read(new Quiet<Response>() {
      @Override
      public Response apply(StoreProvider storeProvider) {
        QuotaInfo quotaInfo = quotaManager.getQuotaInfo(ownerRole, storeProvider);
        GetQuotaResult result = new GetQuotaResult(quotaInfo.getQuota().newBuilder())
            .setProdConsumption(quotaInfo.getProdConsumption().newBuilder())
            .setNonProdConsumption(quotaInfo.getNonProdConsumption().newBuilder());

        return ok(Result.getQuotaResult(result));
      }
    });
  }

  @Override
  public Response getLocks() {
    return ok(Result.getLocksResult(
        new GetLocksResult().setLocks(ILock.toBuildersSet(lockManager.getLocks()))));
  }

  @Override
  public Response getJobUpdateSummaries(final JobUpdateQuery mutableQuery) {
    final IJobUpdateQuery query = IJobUpdateQuery.build(requireNonNull(mutableQuery));
    return ok(Result.getJobUpdateSummariesResult(
        new GetJobUpdateSummariesResult().setUpdateSummaries(IJobUpdateSummary.toBuildersList(
            storage.read(new Quiet<List<IJobUpdateSummary>>() {
              @Override
              public List<IJobUpdateSummary> apply(StoreProvider storeProvider) {
                return storeProvider.getJobUpdateStore().fetchJobUpdateSummaries(query);
              }
            })))));
  }

  @Override
  public Response getJobUpdateDetails(JobUpdateKey mutableKey) {
    final IJobUpdateKey key = IJobUpdateKey.build(mutableKey);
    Optional<IJobUpdateDetails> details =
        storage.read(new Quiet<Optional<IJobUpdateDetails>>() {
          @Override
          public Optional<IJobUpdateDetails> apply(StoreProvider storeProvider) {
            return storeProvider.getJobUpdateStore().fetchJobUpdateDetails(key);
          }
        });

    if (details.isPresent()) {
      return ok(Result.getJobUpdateDetailsResult(
          new GetJobUpdateDetailsResult().setDetails(details.get().newBuilder())));
    } else {
      return invalidRequest("Invalid update: " + key);
    }
  }

  private List<ScheduledTask> getTasks(TaskQuery query) {
    requireNonNull(query);

    Iterable<IScheduledTask> tasks = Storage.Util.fetchTasks(storage, Query.arbitrary(query));
    if (query.isSetOffset()) {
      tasks = Iterables.skip(tasks, query.getOffset());
    }
    if (query.isSetLimit()) {
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
        new Maps.EntryTransformer<IJobKey, Collection<IScheduledTask>, IJobConfiguration>() {
          @Override
          public IJobConfiguration transformEntry(
              IJobKey jobKey,
              Collection<IScheduledTask> tasks) {

            // Pick the latest transitioned task for each immediate job since the job can be in the
            // middle of an update or some shards have been selectively created.
            TaskConfig mostRecentTaskConfig =
                Tasks.getLatestActiveTask(tasks).getAssignedTask().getTask().newBuilder();

            return IJobConfiguration.build(new JobConfiguration()
                .setKey(jobKey.newBuilder())
                .setOwner(mostRecentTaskConfig.getOwner())
                .setTaskConfig(mostRecentTaskConfig)
                .setInstanceCount(tasks.size()));
          }
        }));

    // Get cron jobs directly from the manager. Do this after querying the task store so the real
    // template JobConfiguration for a cron job will overwrite the synthesized one that could have
    // been created above.
    Predicate<IJobConfiguration> configFilter = ownerRole.isPresent()
        ? Predicates.compose(Predicates.equalTo(ownerRole.get()), JobKeys.CONFIG_TO_ROLE)
        : Predicates.<IJobConfiguration>alwaysTrue();
    jobs.putAll(Maps.uniqueIndex(
        FluentIterable.from(Storage.Util.fetchCronJobs(storage)).filter(configFilter),
        JobKeys.FROM_CONFIG));

    return jobs;
  }

  private Multimap<IJobKey, IScheduledTask> getTasks(Query.Builder query) {
    return Tasks.byJobKey(Storage.Util.fetchTasks(storage, query));
  }
}

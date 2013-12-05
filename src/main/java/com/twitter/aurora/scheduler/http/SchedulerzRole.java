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
package com.twitter.aurora.scheduler.http;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import org.antlr.stringtemplate.StringTemplate;

import com.twitter.aurora.gen.CronCollisionPolicy;
import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.apiConstants;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.cron.CronPredictor;
import com.twitter.aurora.scheduler.quota.QuotaManager;
import com.twitter.aurora.scheduler.quota.Quotas;
import com.twitter.aurora.scheduler.state.CronJobManager;
import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.aurora.scheduler.storage.entities.IJobConfiguration;
import com.twitter.aurora.scheduler.storage.entities.IJobKey;
import com.twitter.aurora.scheduler.storage.entities.IQuota;
import com.twitter.aurora.scheduler.storage.entities.IScheduledTask;
import com.twitter.aurora.scheduler.storage.entities.ITaskConfig;
import com.twitter.common.base.Closure;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.aurora.scheduler.base.Tasks.GET_STATUS;
import static com.twitter.aurora.scheduler.base.Tasks.LATEST_ACTIVITY;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;

/**
 * HTTP interface to provide information about jobs for a specific role.
 */
@Path("/scheduler/{role}")
public class SchedulerzRole extends JerseyTemplateServlet {

  private static final List<ScheduleStatus> STATUSES = ImmutableList.<ScheduleStatus>builder()
      .addAll(apiConstants.TERMINAL_STATES)
      .addAll(apiConstants.ACTIVE_STATES)
      .build();

  // The freshest task is the latest active task
  // or the latest inactive task if no active task exists.
  private static final Ordering<IScheduledTask> FRESH_TASK_ORDER =
      Ordering.explicit(STATUSES).onResultOf(GET_STATUS).compound(LATEST_ACTIVITY);

  @VisibleForTesting
  static IScheduledTask getFreshestTask(Iterable<IScheduledTask> tasks) {
    return FRESH_TASK_ORDER.max(tasks);
  }

  private final Storage storage;
  private final CronJobManager cronJobManager;
  private final CronPredictor cronPredictor;
  private final String clusterName;
  private final QuotaManager quotaManager;

  @Inject
  SchedulerzRole(
      Storage storage,
      CronJobManager cronJobManager,
      CronPredictor cronPredictor,
      @ClusterName String clusterName,
      QuotaManager quotaManager) {

    super("schedulerzrole");
    this.storage = checkNotNull(storage);
    this.cronJobManager = checkNotNull(cronJobManager);
    this.cronPredictor = checkNotNull(cronPredictor);
    this.clusterName = checkNotBlank(clusterName);
    this.quotaManager = checkNotNull(quotaManager);
  }

  /**
   * Fetches the landing page for a role.
   *
   * @return HTTP response.
   */
  @GET
  @Produces(MediaType.TEXT_HTML)
  public Response get(@PathParam("role") final String role) {
    return processRequest(Optional.of(role), Optional.<String>absent());
  }

  private Response processRequest(final Optional<String> role, final Optional<String> environment) {
    return fillTemplate(new Closure<StringTemplate>() {
      @Override public void execute(StringTemplate template) {

        if (!role.isPresent()) {
          template.setAttribute("exception", "Please specify a user.");
          return;
        }

        Map<IJobKey, Map<?, ?>> cronJobs = fetchCronJobsBy(role.get(), environment);
        List<Job> jobs = fetchJobsBy(role.get(), environment, cronJobs);
        if (jobs.isEmpty() && cronJobs.isEmpty()) {
          String msg = "No jobs found for role " + role.get()
              + (environment.isPresent() ? (" and environment " + environment.get()) : "");
          throw new WebApplicationException(Response.status(Status.NOT_FOUND).entity(msg).build());
        }

        template.setAttribute("cluster_name", clusterName);
        template.setAttribute("role", role.get());
        template.setAttribute("environment", environment.orNull());
        template.setAttribute("jobs", jobs);
        template.setAttribute("cronJobs", cronJobs.values());

        // TODO(Suman Karumuri): In future compute consumption for role and environment.
        template.setAttribute("prodResourcesUsed", quotaManager.getConsumption(role.get()));
        template.setAttribute("nonProdResourcesUsed", getNonProdConsumption(role.get()));
        template.setAttribute("resourceQuota", getQuota(role.get()));
      }
    });
  }

  private IQuota getQuota(final String role) {
    return Storage.Util.consistentFetchQuota(storage, role).or(Quotas.noQuota());
  }

  private IQuota getNonProdConsumption(String role) {
    FluentIterable<ITaskConfig> tasks = FluentIterable
        .from(Storage.Util.weaklyConsistentFetchTasks(storage, Query.roleScoped(role).active()))
        .transform(Tasks.SCHEDULED_TO_INFO)
        .filter(Predicates.not(Tasks.IS_PRODUCTION));

    return Quotas.fromTasks(tasks);
  }

  /**
   * Display jobs for a role and environment.
   */
  @Path("/{environment}")
  @GET
  @Produces(MediaType.TEXT_HTML)
  public Response get(
      @PathParam("role") final String role,
      @PathParam("environment") final String environment) {

    Optional<String> env = Optional.of(environment);
    if (env.isPresent() && env.get().isEmpty()) {
      env = Optional.absent();
    }

    return processRequest(Optional.of(role), env);
  }

  private Map<IJobKey, Map<?, ?>> fetchCronJobsBy(
      final String role,
      final Optional<String> environment) {

    Predicate<IJobConfiguration> byRoleEnv = new Predicate<IJobConfiguration>() {
      @Override public boolean apply(IJobConfiguration job) {
        boolean roleMatch = job.getOwner().getRole().equals(role);
        boolean envMatch = !environment.isPresent()
            || job.getKey().getEnvironment().equals(environment.get());
        return roleMatch && envMatch;
      }
    };

    Iterable<IJobConfiguration> jobs = FluentIterable
        .from(cronJobManager.getJobs())
        .filter(byRoleEnv);

    return Maps.transformValues(Maps.uniqueIndex(jobs, JobKeys.FROM_CONFIG),
        new Function<IJobConfiguration, Map<?, ?>>() {
          @Override public Map<?, ?> apply(IJobConfiguration job) {
            return ImmutableMap.<Object, Object>builder()
                .put("jobKey", job.getKey())
                .put("name", job.getKey().getName())
                .put("environment", job.getKey().getEnvironment())
                .put("pendingTaskCount", job.getInstanceCount())
                .put("cronSchedule", job.getCronSchedule())
                .put("nextRun", cronPredictor.predictNextRun(job.getCronSchedule()).getTime())
                .put("cronCollisionPolicy", cronCollisionPolicy(job))
                .put("packages", getPackages(job))
                .build();
          }
        });
  }

  private static CronCollisionPolicy cronCollisionPolicy(IJobConfiguration jobConfiguration) {
    return CronJobManager.orDefault(jobConfiguration.getCronCollisionPolicy());
  }

  private static String getPackages(IJobConfiguration job) {
    Set<String> packages = Sets.newHashSet();

    // Insert all packages for all tasks in the set to eliminate duplicates
    ITaskConfig task = job.getTaskConfig();
    if (!task.getPackages().isEmpty()) {
      packages.addAll(Lists.newArrayList(
          Iterables.transform(task.getPackages(), TransformationUtils.PACKAGE_TOSTRING)));
    }
    return Joiner.on(',').join(packages);
  }

  private List<Job> fetchJobsBy(
      final String role,
      final Optional<String> environment,
      final Map<IJobKey, Map<?, ?>> cronJobs) {

    final Function<Map.Entry<IJobKey, Collection<IScheduledTask>>, Job> toJob =
        new Function<Map.Entry<IJobKey, Collection<IScheduledTask>>, Job>() {
          @Override public Job apply(Map.Entry<IJobKey, Collection<IScheduledTask>> tasksByJobKey) {
            IJobKey jobKey = tasksByJobKey.getKey();
            Collection<IScheduledTask> tasks = tasksByJobKey.getValue();

            Job job = new Job();
            job.environment = jobKey.getEnvironment();
            job.name = jobKey.getName();

            // Pick the freshest task's config and associate it with the job.
            ITaskConfig freshestConfig = getFreshestTask(tasks).getAssignedTask().getTask();
            job.production = freshestConfig.isProduction();

            // TODO(Suman Karumuri): Add a source/job type to TaskConfig and replace logic below
            if (freshestConfig.isIsService()) {
              job.type = JobType.SERVICE;
            } else if (cronJobs.containsKey(jobKey)) {
              job.type = JobType.CRON;
            } else {
              job.type = JobType.ADHOC;
            }

            for (IScheduledTask task : tasks) {
              switch (task.getStatus()) {
                case INIT:
                case PENDING:
                  job.pendingTaskCount++;
                  break;

                case ASSIGNED:
                case STARTING:
                case RESTARTING:
                case RUNNING:
                case KILLING:
                case PREEMPTING:
                  job.activeTaskCount++;
                  break;

                case KILLED:
                case FINISHED:
                  job.finishedTaskCount++;
                  break;

                case LOST:
                case FAILED:
                case UNKNOWN:
                  job.failedTaskCount++;
                  Date now = new Date();
                  long elapsedMillis = now.getTime()
                      - Iterables.getLast(task.getTaskEvents()).getTimestamp();

                  if (Amount.of(elapsedMillis, Time.MILLISECONDS).as(Time.HOURS) < 6) {
                    job.recentlyFailedTaskCount++;
                  }
                  break;

                default:
                  throw new IllegalArgumentException("Unsupported status: " + task.getStatus());
              }
            }

            return job;
          }
        };

    Query.Builder query = environment.isPresent()
        ? Query.envScoped(role, environment.get())
        : Query.roleScoped(role);

    Multimap<IJobKey, IScheduledTask> tasks =
        Tasks.byJobKey(Storage.Util.weaklyConsistentFetchTasks(storage, query));

    Iterable<Job> jobs = FluentIterable
        .from(tasks.asMap().entrySet())
        .transform(toJob);

    return DisplayUtils.JOB_ORDERING.sortedCopy(jobs);
  }

  /**
   * Template object to represent a job.
   */
  static class Job {
    private String name;
    private String environment;
    private int pendingTaskCount = 0;
    private int activeTaskCount = 0;
    private int finishedTaskCount = 0;
    private int failedTaskCount = 0;
    private int recentlyFailedTaskCount = 0;
    private boolean production = false;
    private JobType type;

    public String getName() {
      return name;
    }

    public String getEnvironment() {
      return environment;
    }

    public int getPendingTaskCount() {
      return pendingTaskCount;
    }

    public int getActiveTaskCount() {
      return activeTaskCount;
    }

    public int getFinishedTaskCount() {
      return finishedTaskCount;
    }

    public int getFailedTaskCount() {
      return failedTaskCount;
    }

    public int getRecentlyFailedTaskCount() {
      return recentlyFailedTaskCount;
    }

    public boolean getProduction() {
      return production;
    }

    public String getType() {
      return type.toString();
    }
  }

  static enum JobType {
    ADHOC("adhoc"), CRON("cron"), SERVICE("service");

    private String jobType;

    private JobType(String jobType) {
      this.jobType = jobType;
    }

    public String toString() {
      return jobType;
    }
  }
}

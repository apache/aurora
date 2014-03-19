/**
 * Copyright 2013 Apache Software Foundation
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
package org.apache.aurora.scheduler.http;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.twitter.common.base.Closure;

import org.antlr.stringtemplate.StringTemplate;
import org.apache.aurora.gen.CronCollisionPolicy;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Jobs;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.cron.CronPredictor;
import org.apache.aurora.scheduler.quota.QuotaInfo;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.state.CronJobManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobStats;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.IServerInfo;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;

import static org.apache.aurora.scheduler.base.Tasks.getLatestActiveTask;

/**
 * HTTP interface to provide information about jobs for a specific role.
 */
@Path("/scheduler/{role}")
public class SchedulerzRole extends JerseyTemplateServlet {

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
      IServerInfo serverInfo,
      QuotaManager quotaManager) {

    super("schedulerzrole");
    this.storage = checkNotNull(storage);
    this.cronJobManager = checkNotNull(cronJobManager);
    this.cronPredictor = checkNotNull(cronPredictor);
    this.clusterName = checkNotBlank(serverInfo.getClusterName());
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
      @Override
      public void execute(StringTemplate template) {

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
        QuotaInfo quotaInfo = quotaManager.getQuotaInfo(role.get());
        template.setAttribute("prodResourcesUsed", quotaInfo.prodConsumption());
        template.setAttribute("nonProdResourcesUsed", quotaInfo.nonProdConsumption());
        template.setAttribute("resourceQuota", quotaInfo.guota());
      }
    });
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
      @Override
      public boolean apply(IJobConfiguration job) {
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
          @Override
          public Map<?, ?> apply(IJobConfiguration job) {
            return ImmutableMap.<Object, Object>builder()
                .put("jobKey", job.getKey())
                .put("name", job.getKey().getName())
                .put("environment", job.getKey().getEnvironment())
                .put("pendingTaskCount", job.getInstanceCount())
                .put("cronSchedule", job.getCronSchedule())
                .put("nextRun", cronPredictor.predictNextRun(job.getCronSchedule()).getTime())
                .put("cronCollisionPolicy", cronCollisionPolicy(job))
                .put("metadata", getMetadata(job))
                .build();
          }
        });
  }

  private static CronCollisionPolicy cronCollisionPolicy(IJobConfiguration jobConfiguration) {
    return CronJobManager.orDefault(jobConfiguration.getCronCollisionPolicy());
  }

  private static String getMetadata(IJobConfiguration job) {
    Optional<String> metadata = TransformationUtils.getMetadata(job.getTaskConfig());
    return metadata.isPresent() ? metadata.get() : "";
  }

  private List<Job> fetchJobsBy(
      final String role,
      final Optional<String> environment,
      final Map<IJobKey, Map<?, ?>> cronJobs) {

    final Function<Map.Entry<IJobKey, Collection<IScheduledTask>>, Job> toJob =
        new Function<Map.Entry<IJobKey, Collection<IScheduledTask>>, Job>() {
          @Override
          public Job apply(Map.Entry<IJobKey, Collection<IScheduledTask>> tasksByJobKey) {
            IJobKey jobKey = tasksByJobKey.getKey();
            Collection<IScheduledTask> tasks = tasksByJobKey.getValue();

            // Pick the freshest task's config and associate it with the job.
            ITaskConfig mostRecentTaskConfig =
                getLatestActiveTask(tasks).getAssignedTask().getTask();

            JobType jobType;
            // TODO(Suman Karumuri): Add a source/job type to TaskConfig and replace logic below
            if (mostRecentTaskConfig.isIsService()) {
              jobType = JobType.SERVICE;
            } else if (cronJobs.containsKey(jobKey)) {
              jobType = JobType.CRON;
            } else {
              jobType = JobType.ADHOC;
            }

            IJobStats stats = Jobs.getJobStats(tasks);

            return new Job(jobKey.getName(),
                jobKey.getEnvironment(),
                stats.getPendingTaskCount(),
                stats.getActiveTaskCount(),
                stats.getFinishedTaskCount(),
                stats.getFailedTaskCount(),
                0,
                mostRecentTaskConfig.isProduction(),
                jobType);
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
    private final String name;
    private final String environment;
    private final int pendingTaskCount;
    private final int activeTaskCount;
    private final int finishedTaskCount;
    private final int failedTaskCount;
    private final int recentlyFailedTaskCount;
    private final boolean production;
    private final JobType type;

    Job(String name,
        String environment,
        int pendingTaskCount,
        int activeTaskCount,
        int finishedTaskCount,
        int failedTaskCount,
        int recentlyFailedTaskCount,
        boolean production,
        JobType type) {

      this.name = name;
      this.environment = environment;
      this.pendingTaskCount = pendingTaskCount;
      this.activeTaskCount = activeTaskCount;
      this.finishedTaskCount = finishedTaskCount;
      this.failedTaskCount = failedTaskCount;
      this.recentlyFailedTaskCount = recentlyFailedTaskCount;
      this.production = production;
      this.type = type;
    }

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

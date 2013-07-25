package com.twitter.aurora.scheduler.http;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import org.antlr.stringtemplate.StringTemplate;

import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.JobKey;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.TwitterTaskInfo;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.quota.QuotaManager;
import com.twitter.aurora.scheduler.state.CronJobManager;
import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.common.base.Closure;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.common.base.MorePreconditions.checkNotBlank;

/**
 * HTTP interface to provide information about jobs for a specific mesos role.
 */
@Path("/scheduler/{role}")
public class SchedulerzRole extends JerseyTemplateServlet {

  private final Storage storage;
  private final CronJobManager cronScheduler;
  private final String clusterName;
  private final QuotaManager quotaManager;

  /**
   * Creates a new role servlet.
   *
   * @param storage Backing store to fetch tasks from.
   * @param cronScheduler Cron scheduler.
   * @param clusterName Name of the serving cluster.
   * @param quotaManager Resource quota manager.
   */
  @Inject
  public SchedulerzRole(
      Storage storage,
      CronJobManager cronScheduler,
      @ClusterName String clusterName,
      QuotaManager quotaManager) {

    super("schedulerzrole");
    this.storage = checkNotNull(storage);
    this.cronScheduler = checkNotNull(cronScheduler);
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
    return fillTemplate(new Closure<StringTemplate>() {
      @Override public void execute(StringTemplate template) {
        template.setAttribute("cluster_name", clusterName);

        if (role == null) {
          template.setAttribute("exception", "Please specify a user.");
          return;
        }
        template.setAttribute("role", role);

        LoadingCache<JobKey, Job> jobs = CacheBuilder.newBuilder().build(
            new CacheLoader<JobKey, Job>() {
              @Override public Job load(JobKey key) {
                Job job = new Job();
                job.environment = key.getEnvironment();
                job.name = key.getName();
                return job;
              }
            });

        for (ScheduledTask task
            : Storage.Util.weaklyConsistentFetchTasks(storage, Query.roleScoped(role))) {

          TwitterTaskInfo config = task.getAssignedTask().getTask();
          Job job =
              jobs.getUnchecked(JobKeys.from(role, config.getEnvironment(), config.getJobName()));

          switch (task.getStatus()) {
            case INIT:
            case PENDING:
              job.pendingTaskCount++;
              break;

            case ASSIGNED:
            case STARTING:
            case RESTARTING:
            case UPDATING:
            case RUNNING:
            case KILLING:
            case PREEMPTING:
            case ROLLBACK:
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

        List<Job> sortedJobs = DisplayUtils.JOB_ORDERING.sortedCopy(jobs.asMap().values());
        template.setAttribute("jobs", sortedJobs);

        Iterable<JobConfiguration> cronJobs = Iterables.filter(
            cronScheduler.getJobs(), new Predicate<JobConfiguration>() {
          @Override public boolean apply(JobConfiguration job) {
            return job.getOwner().getRole().equals(role);
          }
        });

        if (sortedJobs.isEmpty() && Iterables.isEmpty(cronJobs)) {
          throw new WebApplicationException(
              Response.status(Status.NOT_FOUND).entity("No jobs found for role " + role).build());
        }

        cronJobs = DisplayUtils.JOB_CONFIG_ORDERING.sortedCopy(cronJobs);
        Iterable<Map<?, ?>> cronJobObjs = Iterables.transform(cronJobs,
            new Function<JobConfiguration, Map<?, ?>>() {
              @Override public Map<?, ?> apply(JobConfiguration job) {
                return ImmutableMap.<Object, Object>builder()
                    .put("name", job.getKey().getName())
                    .put("environment", job.getKey().getEnvironment())
                    .put("pendingTaskCount", job.getShardCount())
                    .put("cronSchedule", job.getCronSchedule())
                    .put("nextRun", CronJobManager.predictNextRun(job.cronSchedule).getTime())
                    .put("cronCollisionPolicy",
                        CronJobManager.orDefault(job.getCronCollisionPolicy()))
                    .put("packages", getPackages(job))
                    .build();
              }
            });

        template.setAttribute("cronJobs", Lists.newArrayList(cronJobObjs));
        template.setAttribute("resourcesUsed", quotaManager.getConsumption(role));
        template.setAttribute("resourceQuota", quotaManager.getQuota(role));
      }
    });
  }

  private String getPackages(JobConfiguration job) {
    Set<String> packages = Sets.newHashSet();

    // Insert all packages for all tasks in the set to eliminate duplicates
    TwitterTaskInfo task = job.getTaskConfig();
    if (task.getPackagesSize() > 0) {
      packages.addAll(Lists.newArrayList(
          Iterables.transform(task.getPackages(), TransformationUtils.PACKAGE_TOSTRING)));
    }
    return Joiner.on(',').join(packages);
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
  }
}

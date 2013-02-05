package com.twitter.mesos.scheduler.httphandlers;

import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import org.antlr.stringtemplate.StringTemplate;

import com.twitter.common.base.Closure;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.scheduler.ClusterName;
import com.twitter.mesos.scheduler.CronJobManager;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.quota.QuotaManager;
import com.twitter.mesos.scheduler.storage.Storage;

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

        LoadingCache<String, Job> jobs = CacheBuilder.newBuilder().build(
            new CacheLoader<String, Job>() {
              @Override public Job load(String jobName) {
                Job job = new Job();
                job.name = jobName;
                return job;
              }
            });

        for (ScheduledTask task : Storage.Util.fetchTasks(storage, Query.byRole(role))) {
          Job job = jobs.getUnchecked(task.getAssignedTask().getTask().getJobName());

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
                    .put("name", job.getName())
                    .put("pendingTaskCount", job.getTaskConfigsSize())
                    .put("cronSchedule", job.getCronSchedule())
                    .put("nextRun", CronJobManager.predictNextRun(job.cronSchedule).getTime())
                    .put("cronCollisionPolicy",
                        CronJobManager.orDefault(job.getCronCollisionPolicy()))
                    .build();
              }
            });

        template.setAttribute("cronJobs", Lists.newArrayList(cronJobObjs));
        template.setAttribute("resourcesUsed", quotaManager.getConsumption(role));
        template.setAttribute("resourceQuota", quotaManager.getQuota(role));
      }
    });
  }

  /**
   * Template object to represent a job.
   */
  static class Job {
    String name;
    int pendingTaskCount = 0;
    int activeTaskCount = 0;
    int finishedTaskCount = 0;
    int failedTaskCount = 0;

    public String getName() {
      return name;
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
  }
}

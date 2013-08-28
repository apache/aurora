package com.twitter.aurora.scheduler.http;

import java.util.Collection;
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
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import org.antlr.stringtemplate.StringTemplate;

import com.twitter.aurora.gen.CronCollisionPolicy;
import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.JobKey;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.cron.CronPredictor;
import com.twitter.aurora.scheduler.quota.QuotaManager;
import com.twitter.aurora.scheduler.state.CronJobManager;
import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.common.base.Closure;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.common.base.MorePreconditions.checkNotBlank;

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

        Map<JobKey, Map<?, ?>> cronJobs = fetchCronJobsBy(role.get(), environment);
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
        template.setAttribute("resourcesUsed", quotaManager.getConsumption(role.get()));
        template.setAttribute("resourceQuota", quotaManager.getQuota(role.get()));
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
      env = Optional.<String>absent();
    }

    return processRequest(Optional.of(role), env);
  }

  private Map<JobKey, Map<?, ?>> fetchCronJobsBy(
      final String role,
      final Optional<String> environment) {

    Predicate<JobConfiguration> byRoleEnv = new Predicate<JobConfiguration>() {
      @Override public boolean apply(JobConfiguration job) {
        boolean roleMatch = job.getOwner().getRole().equals(role);
        boolean envMatch =  environment.isPresent()
            ? job.getKey().getEnvironment().equals(environment.get())
            : true;
        return roleMatch && envMatch;
      }
    };

    Iterable<JobConfiguration> jobs = FluentIterable
        .from(cronJobManager.getJobs())
        .filter(byRoleEnv);

    return Maps.transformValues(Maps.uniqueIndex(jobs, JobKeys.FROM_CONFIG),
        new Function<JobConfiguration, Map<?, ?>>() {
          @Override public Map<?, ?> apply(JobConfiguration job) {
            return ImmutableMap.<Object, Object>builder()
                .put("jobKey", job.getKey())
                .put("name", job.getKey().getName())
                .put("environment", job.getKey().getEnvironment())
                .put("pendingTaskCount", job.getShardCount())
                .put("cronSchedule", job.getCronSchedule())
                .put("nextRun", cronPredictor.predictNextRun(job.cronSchedule).getTime())
                .put("cronCollisionPolicy", cronCollisionPolicy(job))
                .put("packages", getPackages(job))
                .build();
          }
        });
  }

  private static CronCollisionPolicy cronCollisionPolicy(JobConfiguration jobConfiguration) {
    return CronJobManager.orDefault(jobConfiguration.getCronCollisionPolicy());
  }

  private static String getPackages(JobConfiguration job) {
    Set<String> packages = Sets.newHashSet();

    // Insert all packages for all tasks in the set to eliminate duplicates
    TaskConfig task = job.getTaskConfig();
    if (task.getPackagesSize() > 0) {
      packages.addAll(Lists.newArrayList(
          Iterables.transform(task.getPackages(), TransformationUtils.PACKAGE_TOSTRING)));
    }
    return Joiner.on(',').join(packages);
  }

  private List<Job> fetchJobsBy(
      final String role,
      final Optional<String> environment,
      final Map<JobKey, Map<?, ?>> cronJobs) {

    final Function<Map.Entry<JobKey, Collection<ScheduledTask>>, Job> toJob =
        new Function<Map.Entry<JobKey, Collection<ScheduledTask>>, Job>() {
          @Override public Job apply(Map.Entry<JobKey, Collection<ScheduledTask>> tasksByJobKey) {
            JobKey jobKey = tasksByJobKey.getKey();
            Collection<ScheduledTask> tasks = tasksByJobKey.getValue();

            Job job = new Job();
            job.environment = jobKey.getEnvironment();
            job.name = jobKey.getName();

            TaskConfig config = Iterables.get(tasks, 0).getAssignedTask().getTask();
            job.production = config.isProduction();

            // TODO(Suman Karumuri): Add a source/job type to TaskConfig and replace logic below
            if (config.isIsService()) {
              job.type = JobType.SERVICE;
            } else if (cronJobs.containsKey(jobKey)) {
              job.type = JobType.CRON;
            } else {
              job.type = JobType.ADHOC;
            }

            for (ScheduledTask task : tasks) {
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

            return job;
          }
        };

    Query.Builder query = environment.isPresent()
        ? Query.envScoped(role, environment.get())
        : Query.roleScoped(role);

    Multimap<JobKey, ScheduledTask> tasks =
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

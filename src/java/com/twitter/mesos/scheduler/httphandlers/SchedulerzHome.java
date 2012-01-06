package com.twitter.mesos.scheduler.httphandlers;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import org.antlr.stringtemplate.StringTemplate;

import com.twitter.common.base.Closure;
import com.twitter.common.net.http.handlers.StringTemplateServlet;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.scheduler.ClusterName;
import com.twitter.mesos.scheduler.CronJobManager;
import com.twitter.mesos.scheduler.LeaderRedirect;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.SchedulerCore;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;

/**
 * HTTP interface to serve as a HUD for the mesos scheduler.
 *
 * @author William Farner
 */
public class SchedulerzHome extends StringTemplateServlet {

  private final SchedulerCore scheduler;
  private final CronJobManager cronScheduler;
  private final String clusterName;
  private final LeaderRedirect redirector;

  @Inject
  public SchedulerzHome(@CacheTemplates boolean cacheTemplates,
      SchedulerCore scheduler,
      CronJobManager cronScheduler,
      @ClusterName String clusterName,
      LeaderRedirect redirector) {
    super("schedulerzhome", cacheTemplates);
    this.scheduler = checkNotNull(scheduler);
    this.cronScheduler = checkNotNull(cronScheduler);
    this.clusterName = checkNotBlank(clusterName);
    this.redirector = checkNotNull(redirector);
  }

  private static final Function<String, Role> CREATE_ROLE = new Function<String, Role>() {
    @Override public Role apply(String ownerRole) {
      Role role = new Role();
      role.role = ownerRole;
      return role;
    }
  };

  @Override
  protected void doGet(final HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    Optional<String> leaderRedirect = redirector.getRedirectTarget(req);
    if (leaderRedirect.isPresent()) {
      resp.sendRedirect(leaderRedirect.get());
      return;
    }

    writeTemplate(resp, new Closure<StringTemplate>() {
      @Override public void execute(StringTemplate template) {
        template.setAttribute("cluster_name", clusterName);

        LoadingCache<String, Role> owners =
            CacheBuilder.newBuilder().build(CacheLoader.from(CREATE_ROLE));
        Multimap<String, ScheduledTask> ownerJobs = HashMultimap.create();

        for (ScheduledTask task : scheduler.getTasks(Query.GET_ALL)) {
          Role role = owners.getUnchecked(task.getAssignedTask().getTask().getOwner().getRole());
          switch (task.getStatus()) {
            case INIT:
            case PENDING:
              role.pendingTaskCount++;
              break;

            case ASSIGNED:
            case STARTING:
            case RESTARTING:
            case UPDATING:
            case RUNNING:
              role.activeTaskCount++;
              break;

            case KILLING:
            case KILLED:
            case FINISHED:
            case PREEMPTING:
            case ROLLBACK:
              role.finishedTaskCount++;
              break;

            case LOST:
            case FAILED:
            case UNKNOWN:
              role.failedTaskCount++;
              break;

            default:
              throw new IllegalArgumentException("Unsupported status: " + task.getStatus());
          }

          ownerJobs.put(role.role, task);
        }

        Collection<Role> roles = owners.asMap().values();
        for (Role role : roles) {
          Iterable<ScheduledTask> activeRoleTasks =
              Iterables.filter(ownerJobs.get(role.role), Tasks.ACTIVE_FILTER);
          role.jobCount = Sets.newHashSet(Iterables.transform(
              activeRoleTasks, GET_JOB_NAME)).size();
        }

        template.setAttribute("owners",
            DisplayUtils.sort(roles, DisplayUtils.SORT_USERS_BY_NAME));

        // Add cron job counts for each role.
        for (Map.Entry<String, Collection<JobConfiguration>> entry
            : Multimaps.index(cronScheduler.getJobs(), GET_CRON_OWNER).asMap().entrySet()) {
          owners.getUnchecked(entry.getKey()).cronJobCount = entry.getValue().size();
        }
      }
    });
  }

  private static final Function<ScheduledTask, String> GET_JOB_NAME =
      new Function<ScheduledTask, String>() {
        @Override public String apply(ScheduledTask task) {
          return task.getAssignedTask().getTask().getJobName();
        }
      };

  private static final Function<JobConfiguration, String> GET_CRON_OWNER =
      new Function<JobConfiguration, String>() {
        @Override public String apply(JobConfiguration job) {
          return job.getOwner().getRole();
        }
      };

  static class Role {
    String role;
    int jobCount;
    int cronJobCount;
    private int pendingTaskCount = 0;
    private int activeTaskCount = 0;
    private int finishedTaskCount = 0;
    private int failedTaskCount = 0;

    public String getRole() {
      return role;
    }

    public int getJobCount() {
      return jobCount;
    }

    public int getCronJobCount() {
      return cronJobCount;
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

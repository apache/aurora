package com.twitter.aurora.scheduler.http;

import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import org.antlr.stringtemplate.StringTemplate;

import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.TwitterTaskInfo;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.state.CronJobManager;
import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.common.base.Closure;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.common.base.MorePreconditions.checkNotBlank;

/**
 * HTTP interface to serve as a HUD for the aurora scheduler.
 */
@Path("/scheduler")
public class SchedulerzHome extends JerseyTemplateServlet {

  private static final Function<String, Role> CREATE_ROLE = new Function<String, Role>() {
    @Override public Role apply(String ownerRole) {
      Role role = new Role();
      role.role = ownerRole;
      return role;
    }
  };

  private final Storage storage;
  private final CronJobManager cronScheduler;
  private final String clusterName;

  /**
   * Creates a new scheduler home servlet.
   *
   * @param storage Backing store to fetch tasks from.
   * @param cronScheduler Cron scheduler.
   * @param clusterName Name of the serving cluster.
   */
  @Inject
  public SchedulerzHome(
      Storage storage,
      CronJobManager cronScheduler,
      @ClusterName String clusterName) {

    super("schedulerzhome");
    this.storage = checkNotNull(storage);
    this.cronScheduler = checkNotNull(cronScheduler);
    this.clusterName = checkNotBlank(clusterName);
  }

  /**
   * Fetches the scheduler landing page.
   *
   * @return HTTP response.
   */
  @GET
  @Produces(MediaType.TEXT_HTML)
  public Response get() {
    return fillTemplate(new Closure<StringTemplate>() {
      @Override public void execute(StringTemplate template) {
        template.setAttribute("cluster_name", clusterName);

        LoadingCache<String, Role> owners =
            CacheBuilder.newBuilder().build(CacheLoader.from(CREATE_ROLE));

        // TODO(William Farner): Render this page without an expensive query.
        Set<ScheduledTask> tasks =
            Storage.Util.weaklyConsistentFetchTasks(storage, Query.unscoped());
        for (TwitterTaskInfo task : Iterables.transform(tasks, Tasks.SCHEDULED_TO_INFO)) {
          owners.getUnchecked(task.getOwner().getRole()).accumulate(task);
        }

        // Add cron job counts for each role.
        for (JobConfiguration job : cronScheduler.getJobs()) {
          owners.getUnchecked(job.getOwner().getRole()).accumulate(job);
        }

        template.setAttribute(
            "owners",
            DisplayUtils.ROLE_ORDERING.sortedCopy(owners.asMap().values()));
      }
    });
  }

  /**
   * Template object to represent a role.
   */
  static class Role {
    private String role;
    private Set<String> jobs = Sets.newHashSet();
    private Set<String> cronJobs = Sets.newHashSet();

    private void accumulate(TwitterTaskInfo task) {
      jobs.add(task.getJobName());
    }

    private void accumulate(JobConfiguration job) {
      cronJobs.add(job.getName());
    }

    public String getRole() {
      return role;
    }

    public int getJobCount() {
      return jobs.size();
    }

    public int getCronJobCount() {
      return cronJobs.size();
    }
  }
}

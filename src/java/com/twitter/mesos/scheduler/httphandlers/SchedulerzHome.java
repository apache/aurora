package com.twitter.mesos.scheduler.httphandlers;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import org.antlr.stringtemplate.StringTemplate;

import com.twitter.common.base.Closure;
import com.twitter.common.net.http.handlers.StringTemplateServlet;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.scheduler.ClusterName;
import com.twitter.mesos.scheduler.CronJobManager;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.SchedulerCore;
import com.twitter.mesos.scheduler.TaskState;

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

  @Inject
  public SchedulerzHome(@CacheTemplates boolean cacheTemplates,
      SchedulerCore scheduler,
      CronJobManager cronScheduler,
      @ClusterName String clusterName) {
    super("schedulerzhome", cacheTemplates);
    this.scheduler = checkNotNull(scheduler);
    this.cronScheduler = checkNotNull(cronScheduler);
    this.clusterName = checkNotBlank(clusterName);
  }

  @Override
  protected void doGet(final HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    writeTemplate(resp, new Closure<StringTemplate>() {
      @Override public void execute(StringTemplate template) {
        template.setAttribute("cluster_name", clusterName);

        Map<String, User> users = new MapMaker().makeComputingMap(new Function<String, User>() {
          @Override public User apply(String userName) {
            User user = new User();
            user.name = userName;
            return user;
          }
        });

        Multimap<String, TaskState> userJobs = HashMultimap.create();

        Set<TaskState> tasks = scheduler.getTasks(Query.GET_ALL);

        for (TaskState state : tasks) {
          User user = users.get(state.task.getAssignedTask().getTask().getOwner());
          switch (state.task.getStatus()) {
            case PENDING:
              user.pendingTaskCount++;
              break;

            case STARTING:
            case RUNNING:
              user.activeTaskCount++;
              break;

            case KILLED:
            case KILLED_BY_CLIENT:
            case FINISHED:
              user.finishedTaskCount++;
              break;

            case LOST:
            case NOT_FOUND:
            case FAILED:
              user.failedTaskCount++;
              break;

            default:
              throw new IllegalArgumentException("Unsupported status: " + state.task.getStatus());
          }

          userJobs.put(user.name, state);
        }

        for (User user : users.values()) {
          Iterable<ScheduledTask> activeUserTasks = Iterables.filter(
              Iterables.transform(userJobs.get(user.name), TaskState.STATE_TO_SCHEDULED),
              Tasks.ACTIVE_FILTER);
          user.jobCount = Sets.newHashSet(Iterables.transform(
              activeUserTasks, GET_JOB_NAME)).size();
        }

        template.setAttribute("users",
            DisplayUtils.sort(users.values(), DisplayUtils.SORT_USERS_BY_NAME));

        template.setAttribute("cronJobs",
            DisplayUtils.sort(cronScheduler.getJobs(), DisplayUtils.SORT_JOB_CONFIG_BY_NAME));
      }
    });
  }

  private static final Function<ScheduledTask, String> GET_JOB_NAME =
      new Function<ScheduledTask, String>() {
        @Override public String apply(ScheduledTask task) {
          return task.getAssignedTask().getTask().getJobName();
        }
      };

  static class User {
    String name;
    int jobCount;
    private int pendingTaskCount = 0;
    private int activeTaskCount = 0;
    private int finishedTaskCount = 0;
    private int failedTaskCount = 0;

    public String getName() {
      return name;
    }

    public int getJobCount() {
      return jobCount;
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

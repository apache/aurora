package com.twitter.mesos.scheduler.httphandlers;

import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.twitter.common.base.Closure;
import com.twitter.common.net.http.handlers.StringTemplateServlet;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TrackedTask;
import com.twitter.mesos.scheduler.CronJobManager;
import com.twitter.mesos.scheduler.SchedulerCore;
import org.antlr.stringtemplate.StringTemplate;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * HTTP interface to serve as a HUD for the mesos scheduler.
 *
 * @author wfarner
 */
public class SchedulerzHome extends StringTemplateServlet {

  @Inject private SchedulerCore scheduler;
  @Inject private CronJobManager cronScheduler;

  @Inject
  public SchedulerzHome(@CacheTemplates boolean cacheTemplates) {
    super("schedulerzhome", cacheTemplates);
  }

  @Override
  protected void doGet(final HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    writeTemplate(resp, new Closure<StringTemplate>() {
      @Override public void execute(StringTemplate template) {
        Map<String, User> users = Maps.newHashMap();
        Multimap<String, TrackedTask> userJobs = HashMultimap.create();

        Iterable<TrackedTask> tasks = scheduler.getTasks(new TaskQuery());

        for (TrackedTask task : tasks) {
          User user = users.get(task.getOwner());
          if (user == null) {
            user = new User();
            user.name = task.getOwner();
            users.put(user.name, user);
          }
          user.taskCount++;
          userJobs.put(user.name, task);
        }

        for (User user : users.values()) {
          Iterable<TrackedTask> activeUserTasks = Iterables.filter(userJobs.get(user.name),
              SchedulerCore.ACTIVE_FILTER);
          user.jobCount =  Iterables.size(activeUserTasks);
        }

        template.setAttribute("users", users.values());

        template.setAttribute("cronJobs", Lists.newArrayList(cronScheduler.getJobs()));
      }
    });
  }

  class User {
    String name;
    int jobCount;
    int taskCount;

    public String getName() {
      return name;
    }

    public int getJobCount() {
      return jobCount;
    }

    public int getTaskCount() {
      return taskCount;
    }
  }
}

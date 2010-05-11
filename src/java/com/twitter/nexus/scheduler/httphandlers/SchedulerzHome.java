package com.twitter.nexus.scheduler.httphandlers;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import com.twitter.common.base.Closure;
import com.twitter.common.net.http.handlers.StringTemplateServlet;
import com.twitter.nexus.gen.JobConfiguration;
import com.twitter.nexus.gen.TrackedTask;
import com.twitter.nexus.scheduler.SchedulerCore;
import org.antlr.stringtemplate.StringTemplate;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

/**
 * HTTP interface to serve as a HUD for the nexus scheduler.
 *
 * @author wfarner
 */
public class SchedulerzHome extends StringTemplateServlet {
  @Inject
  private SchedulerCore scheduler;

  @Inject
  public SchedulerzHome(@CacheTemplates boolean cacheTemplates) {
    super("schedulerzhome", cacheTemplates);
  }

  @Override
  protected void doGet(final HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    writeTemplate(resp, new Closure<StringTemplate>() {
      @Override public void execute(StringTemplate template) {
        List<User> users = Lists.newArrayList();
        for (String user : scheduler.getUsers()) {
          Multimap<JobConfiguration, TrackedTask> jobs = scheduler.getUserJobs(user);
          User userObj = new User();
          userObj.name = user;
          userObj.jobCount = jobs.keySet().size();
          userObj.taskCount = jobs.values().size();
          users.add(userObj);
        }

        template.setAttribute("users", users);
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

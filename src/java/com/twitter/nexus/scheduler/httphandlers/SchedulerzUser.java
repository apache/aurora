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
import java.util.regex.Pattern;

/**
 * HTTP interface to provide information about jobs for a specific nexus user.
 *
 * @author wfarner
 */
public class SchedulerzUser extends StringTemplateServlet {
  @Inject
  private SchedulerCore scheduler;

  private static final String USER_PARAM = "user";

  @Inject
  public SchedulerzUser(@CacheTemplates boolean cacheTemplates) {
    super("schedulerzuser", cacheTemplates);
  }

  @Override
  protected void doGet(final HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    writeTemplate(resp, new Closure<StringTemplate>() {
      @Override public void execute(StringTemplate template) {
        String user = req.getParameter(USER_PARAM);
        if (user == null) {
          template.setAttribute("exception", "Please specify a user.");
          return;
        }

        template.setAttribute("user", user);

        Multimap<JobConfiguration, TrackedTask> jobs = scheduler.getUserJobs(user);

        List<Job> jobObjs = Lists.newArrayList();

        for (JobConfiguration job : jobs.keySet()) {
          Job jobObj = new Job();
          jobObj.name = job.getName();
          jobObj.taskCount = jobs.get(job).size();
          jobObjs.add(jobObj);
        }

        template.setAttribute("jobs", jobObjs);
      }
    });
  }

  class Job {
    String name;
    int taskCount;

    public String getName() {
      return name;
    }

    public int getTaskCount() {
      return taskCount;
    }
  }
}

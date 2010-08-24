package com.twitter.nexus.scheduler.httphandlers;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.twitter.common.base.Closure;
import com.twitter.common.net.http.handlers.StringTemplateServlet;
import com.twitter.nexus.gen.TaskQuery;
import com.twitter.nexus.scheduler.SchedulerCore;
import org.antlr.stringtemplate.StringTemplate;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * HTTP interface to view information about a job in the nexus scheduler.
 *
 * @author wfarner
 */
public class SchedulerzJob extends StringTemplateServlet {
  @Inject
  private SchedulerCore scheduler;

  private static final String USER_PARAM = "user";
  private static final String JOB_PARAM = "job";

  @Inject
  public SchedulerzJob(@CacheTemplates boolean cacheTemplates) {
    super("schedulerzjob", cacheTemplates);
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
        String job = req.getParameter(JOB_PARAM);
        if (job == null) {
          template.setAttribute("exception", "Please specify a job.");
          return;
        }

        template.setAttribute("user", user);
        template.setAttribute("job", job);

        TaskQuery query = new TaskQuery()
            .setOwner(user)
            .setJobName(job);

        template.setAttribute("tasks", Lists.newArrayList(scheduler.getTasks(query)));
      }
    });
  }
}

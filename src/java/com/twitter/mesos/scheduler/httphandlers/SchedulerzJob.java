package com.twitter.mesos.scheduler.httphandlers;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.twitter.common.base.Closure;
import com.twitter.common.net.http.handlers.StringTemplateServlet;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TrackedTask;
import com.twitter.mesos.scheduler.SchedulerCore;
import org.antlr.stringtemplate.StringTemplate;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Set;

/**
 * HTTP interface to view information about a job in the mesos scheduler.
 *
 * @author wfarner
 */
public class SchedulerzJob extends StringTemplateServlet {

  private static final String USER_PARAM = "user";
  private static final String JOB_PARAM = "job";

  private static final Set<ScheduleStatus> ACTIVE_STATES = Sets.newHashSet(
      ScheduleStatus.PENDING, ScheduleStatus.STARTING, ScheduleStatus.RUNNING);
  private static final Predicate<TrackedTask> ACTIVE_FILTER = new Predicate<TrackedTask>() {
      @Override public boolean apply(TrackedTask task) {
        return ACTIVE_STATES.contains(task.getStatus());
      }
    };

  @Inject private SchedulerCore scheduler;

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

        template.setAttribute("activeTasks",
            Lists.newArrayList(scheduler.getTasks(query, ACTIVE_FILTER)));

        template.setAttribute("completedTasks",
            Lists.newArrayList(scheduler.getTasks(query, Predicates.not(ACTIVE_FILTER))));
      }
    });
  }
}

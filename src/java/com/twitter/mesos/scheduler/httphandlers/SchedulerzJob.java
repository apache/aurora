package com.twitter.mesos.scheduler.httphandlers;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.twitter.common.base.Closure;
import com.twitter.common.net.http.handlers.StringTemplateServlet;
import com.twitter.mesos.gen.ScheduleStatus;
import static com.twitter.mesos.gen.ScheduleStatus.*;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TrackedTask;
import com.twitter.mesos.scheduler.SchedulerCore;
import org.antlr.stringtemplate.StringTemplate;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * HTTP interface to view information about a job in the mesos scheduler.
 *
 * @author wfarner
 */
public class SchedulerzJob extends StringTemplateServlet {

  private static final String USER_PARAM = "user";
  private static final String JOB_PARAM = "job";
  // TODO(wfarner): Allow filtering by task status.
  private static final String STATUS_FILTER_PARAM = "status";

  private static final Map<ScheduleStatus, Set<ScheduleStatus>> FILTER_MAP =
      ImmutableMap.<ScheduleStatus, Set<ScheduleStatus>>builder()
        .put(PENDING, Sets.newHashSet(PENDING))
        .put(RUNNING, Sets.newHashSet(STARTING, RUNNING))
        .put(FINISHED, Sets.newHashSet(KILLED, KILLED_BY_CLIENT, FINISHED))
        .put(FAILED, Sets.newHashSet(LOST, NOT_FOUND, FAILED))
      .build();

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

        String filterArg = req.getParameter(STATUS_FILTER_PARAM);
        ScheduleStatus statusFilter = null;
        if (filterArg != null) {
          try {
            statusFilter = ScheduleStatus.valueOf(filterArg.toUpperCase());
          } catch (IllegalArgumentException e) {
            template.setAttribute("exception", "Invalid status type: " + filterArg);
          }
        }

        template.setAttribute("user", user);
        template.setAttribute("job", job);

        TaskQuery query = new TaskQuery()
            .setOwner(user)
            .setJobName(job);

        if (statusFilter != null) {
          query.setStatuses(FILTER_MAP.get(statusFilter));
          template.setAttribute("activeTasks",
              Lists.newArrayList(scheduler.getTasks(query)));
        } else {
          template.setAttribute("activeTasks",
              Lists.newArrayList(scheduler.getTasks(query, SchedulerCore.ACTIVE_FILTER)));

          List<TrackedTask> completedTasks = Lists.newArrayList(scheduler.getTasks(query,
                  Predicates.not(SchedulerCore.ACTIVE_FILTER)));
          Collections.sort(completedTasks, new Comparator<TrackedTask>() {
              @Override public int compare(TrackedTask taskA, TrackedTask taskB) {
                // Sort in reverse chronological order.
                return taskB.getTaskId() - taskA.getTaskId();
              }
          });

          template.setAttribute("completedTasks", completedTasks);
        }
      }
    });
  }
}

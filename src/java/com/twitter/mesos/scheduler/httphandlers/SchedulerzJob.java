package com.twitter.mesos.scheduler.httphandlers;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.twitter.common.base.Closure;
import com.twitter.common.net.http.handlers.StringTemplateServlet;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.LiveTask;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.SchedulerCore;
import com.twitter.mesos.scheduler.TaskState;
import org.antlr.stringtemplate.StringTemplate;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.twitter.mesos.gen.ScheduleStatus.*;

/**
 * HTTP interface to view information about a job in the mesos scheduler.
 *
 * @author William Farner
 */
public class SchedulerzJob extends StringTemplateServlet {

  private static final String USER_PARAM = "user";
  private static final String JOB_PARAM = "job";
  // TODO(William Farner): Allow filtering by task status.
  private static final String STATUS_FILTER_PARAM = "status";

  private static final Map<ScheduleStatus, Set<ScheduleStatus>> FILTER_MAP =
      ImmutableMap.<ScheduleStatus, Set<ScheduleStatus>>builder()
        .put(PENDING, EnumSet.of(PENDING))
        .put(RUNNING, EnumSet.of(STARTING, RUNNING))
        .put(FINISHED, EnumSet.of(KILLED, KILLED_BY_CLIENT, FINISHED))
        .put(FAILED, EnumSet.of(LOST, NOT_FOUND, FAILED))
      .build();

  private static final Comparator<LiveTask> REVERSE_CHRON_COMPARATOR =
      new Comparator<LiveTask>() {
          @Override public int compare(LiveTask taskA, LiveTask taskB) {
            // Sort in reverse chronological order.
            return (int) Math.signum(
                Iterables.getLast(taskB.getScheduledTask().getTaskEvents()).getTimestamp()
                - Iterables.getLast(taskA.getScheduledTask().getTaskEvents()).getTimestamp());
          }
      };

  private static final Comparator<LiveTask> SHARD_ID_COMPARATOR =
      new Comparator<LiveTask>() {
        @Override public int compare(LiveTask taskA, LiveTask taskB) {
          return taskA.getScheduledTask().getAssignedTask().getTask().getShardId()
                 - taskB.getScheduledTask().getAssignedTask().getTask().getShardId();
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

        Set<TaskState> activeTasks;
        if (statusFilter != null) {
          query.setStatuses(FILTER_MAP.get(statusFilter));
          activeTasks = scheduler.getTasks(new Query(query));
        } else {
          activeTasks = scheduler.getTasks(new Query(query, Tasks.ACTIVE_FILTER));
          List<LiveTask> completedTasks = Lists.newArrayList(Iterables.transform(
              scheduler.getTasks(new Query(query, Predicates.not(Tasks.ACTIVE_FILTER))),
              TaskState.STATE_TO_LIVE));
          Collections.sort(completedTasks, REVERSE_CHRON_COMPARATOR);
          template.setAttribute("completedTasks", completedTasks);
        }

        List<LiveTask> liveTasks = Lists.newArrayList(Iterables.transform(activeTasks,
            TaskState.STATE_TO_LIVE));

        Collections.sort(liveTasks, SHARD_ID_COMPARATOR);
        template.setAttribute("activeTasks", liveTasks);
      }
    });
  }
}

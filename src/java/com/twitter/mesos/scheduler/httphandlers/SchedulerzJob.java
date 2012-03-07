package com.twitter.mesos.scheduler.httphandlers;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;

import org.antlr.stringtemplate.StringTemplate;

import com.twitter.common.base.Closure;
import com.twitter.common.net.http.handlers.StringTemplateServlet;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskEvent;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.scheduler.ClusterName;
import com.twitter.mesos.scheduler.LeaderRedirect;
import com.twitter.mesos.scheduler.SchedulerCore;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.common.base.MorePreconditions.checkNotBlank;
import static com.twitter.mesos.gen.ScheduleStatus.ASSIGNED;
import static com.twitter.mesos.gen.ScheduleStatus.FAILED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLING;
import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static com.twitter.mesos.gen.ScheduleStatus.STARTING;

/**
 * HTTP interface to view information about a job in the mesos scheduler.
 *
 * @author William Farner
 */
public class SchedulerzJob extends StringTemplateServlet {
  private static final String ROLE_PARAM = "role";
  private static final String USER_PARAM = "user";
  private static final String JOB_PARAM = "job";
  // TODO(William Farner): Allow filtering by task status.
  private static final String STATUS_FILTER_PARAM = "status";
  private static final String ADMIN_VIEW_PARAM = "admin";

  // Pagination controls.
  private static final String OFFSET_PARAM = "o";
  private static final int PAGE_SIZE = 50;

  private static final Ordering<ScheduledTask> SHARD_ID_COMPARATOR =
    Ordering.natural().onResultOf(Tasks.SCHEDULED_TO_SHARD_ID);

  private static final Map<ScheduleStatus, Set<ScheduleStatus>> FILTER_MAP =
      ImmutableMap.<ScheduleStatus, Set<ScheduleStatus>>builder()
        .put(PENDING, EnumSet.of(PENDING))
        .put(RUNNING, EnumSet.of(ASSIGNED, STARTING, RUNNING, KILLING))
        .put(FINISHED, EnumSet.of(KILLED, FINISHED))
        .put(FAILED, EnumSet.of(LOST, FAILED))
      .build();

  private static final Comparator<ScheduledTask> REVERSE_CHRON_COMPARATOR =
      new Comparator<ScheduledTask>() {
        @Override public int compare(ScheduledTask taskA, ScheduledTask taskB) {
          // Sort in reverse chronological order.
          Iterable<TaskEvent> taskAEvents = taskA.getTaskEvents();
          Iterable<TaskEvent> taskBEvents = taskB.getTaskEvents();

          boolean taskAHasEvents = taskAEvents != null && !Iterables.isEmpty(taskAEvents);
          boolean taskBHasEvents = taskBEvents != null && !Iterables.isEmpty(taskBEvents);
          if (taskAHasEvents && taskBHasEvents) {
            return Long.signum(Iterables.getLast(taskBEvents).getTimestamp()
                - Iterables.getLast(taskAEvents).getTimestamp());
          } else {
            return 0;
          }
        }
      };

  private static final Function<ScheduledTask, Map<String, Object>> TASK_TO_STRING_MAP =
      new Function<ScheduledTask, Map<String, Object>>() {
        @Override public Map<String, Object> apply(ScheduledTask scheduledTask) {
          AssignedTask task = scheduledTask.getAssignedTask();
          ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("taskId", task.getTaskId())
            .put("shardId", task.getTask().getShardId())
            .put("slaveHost", task.isSetSlaveHost() ? task.getSlaveHost() : "")
            .put("taskEvents", scheduledTask.isSetTaskEvents()
                ? scheduledTask.getTaskEvents() : Lists.newArrayList());
          if (task.isSetAssignedPorts()
              && task.getAssignedPorts().containsKey("health")) {
            builder.put("healthPort", task.getAssignedPorts().get("health"));
          } else {
            builder.put("healthPort", "");
          }
          if (task.getTask().isSetThermosConfig()
              && task.getTask().getThermosConfig().length != 0) {
            builder.put("executorPort", 1338);
          } else {
            builder.put("executorPort", 1337);
          }
          return builder.build();
        }
      };


  private final SchedulerCore scheduler;
  private final String clusterName;
  private final LeaderRedirect redirector;

  /**
   * Creates a new job servlet.
   *
   * @param cacheTemplates Whether to cache the template file.
   * @param scheduler Core scheduler.
   * @param clusterName Name of the serving cluster.
   * @param redirector Redirect logic.
   */
  @Inject
  public SchedulerzJob(@CacheTemplates boolean cacheTemplates,
      SchedulerCore scheduler,
      @ClusterName String clusterName,
      LeaderRedirect redirector) {
    super("schedulerzjob", cacheTemplates);
    this.scheduler = checkNotNull(scheduler);
    this.clusterName = checkNotBlank(clusterName);
    this.redirector = checkNotNull(redirector);
  }

  /**
   * Extracts the offset count from the request, and returns the number of items that should be
   * skipped to render the page.
   *
   * @param req Servlet request.
   * @return The number of items to skip to get to the requested offset.
   */
  private static int getOffset(HttpServletRequest req) {
    String offset = req.getParameter(OFFSET_PARAM);
    if (offset != null) {
      try {
        return Integer.parseInt(offset);
      } catch (NumberFormatException e) {
        // Ignore, default to zero offset.
      }
    }
    return 0;
  }

  private static <T> Iterable<T> offsetAndLimit(Iterable<T> iterable, int offset) {
    return ImmutableList.copyOf(Iterables.limit(Iterables.skip(iterable, offset), PAGE_SIZE));
  }

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

        template.setAttribute(ADMIN_VIEW_PARAM, req.getParameter(ADMIN_VIEW_PARAM) != null);

        String user = req.getParameter(USER_PARAM);
        String role = req.getParameter(ROLE_PARAM);

        if (role == null) {
          template.setAttribute("exception", "Please specify a role.");
          return;
        }
        Identity identity = new Identity(role, user);

        String job = req.getParameter(JOB_PARAM);
        if (job == null) {
          template.setAttribute("exception", "Please specify a job.");
          return;
        }

        String filterArg = req.getParameter(STATUS_FILTER_PARAM);
        ScheduleStatus statusFilter = null;
        if (filterArg != null) {
          template.setAttribute(STATUS_FILTER_PARAM, filterArg);

          try {
            statusFilter = ScheduleStatus.valueOf(filterArg.toUpperCase());
          } catch (IllegalArgumentException e) {
            template.setAttribute("exception", "Invalid status type: " + filterArg);
          }
        }

        template.setAttribute("role", role);
        template.setAttribute("job", job);

        TaskQuery query = new TaskQuery()
            .setOwner(identity)
            .setJobName(job);

        int offset = getOffset(req);
        boolean hasMore = false;

        Set<ScheduledTask> activeTasks;
        if (statusFilter != null) {
          query.setStatuses(FILTER_MAP.get(statusFilter));
          activeTasks = scheduler.getTasks(query);
        } else {
          activeTasks = scheduler.getTasks(new TaskQuery(query).setStatuses(Tasks.ACTIVE_STATES));
          List<ScheduledTask> completedTasks = Lists.newArrayList(
              scheduler.getTasks(new TaskQuery(query).setStatuses(Tasks.TERMINAL_STATES)));
          Collections.sort(completedTasks, REVERSE_CHRON_COMPARATOR);
          template.setAttribute("completedTasks",
            ImmutableList.copyOf(
              Iterables.transform(offsetAndLimit(completedTasks, offset), TASK_TO_STRING_MAP)));
          hasMore = completedTasks.size() > offset + PAGE_SIZE;
        }

        List<ScheduledTask> liveTasks = SHARD_ID_COMPARATOR.sortedCopy(activeTasks);
        template.setAttribute("activeTasks",
          ImmutableList.copyOf(
              Iterables.transform(offsetAndLimit(liveTasks, offset), TASK_TO_STRING_MAP)));
        hasMore = hasMore || liveTasks.size() > (offset + PAGE_SIZE);

        if (offset > 0) {
          template.setAttribute("prevOffset", Math.max(0, offset - PAGE_SIZE));
        }
        if (hasMore) {
          template.setAttribute("nextOffset", offset + PAGE_SIZE);
        }
      }
    });
  }
}

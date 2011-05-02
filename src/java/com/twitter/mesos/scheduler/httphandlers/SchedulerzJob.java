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

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import org.antlr.stringtemplate.StringTemplate;

import com.twitter.common.base.Closure;
import com.twitter.common.net.http.handlers.StringTemplateServlet;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.LiveTask;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.scheduler.ClusterName;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.SchedulerCore;
import com.twitter.mesos.scheduler.TaskState;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;
import static com.twitter.mesos.gen.ScheduleStatus.FAILED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED_BY_CLIENT;
import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.NOT_FOUND;
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

  // Pagination controls.
  private static final String OFFSET_PARAM = "o";
  private static final int PAGE_SIZE = 50;

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

  private final SchedulerCore scheduler;
  private final String clusterName;

  @Inject
  public SchedulerzJob(@CacheTemplates boolean cacheTemplates,
      SchedulerCore scheduler,
      @ClusterName String clusterName) {
    super("schedulerzjob", cacheTemplates);
    this.scheduler = checkNotNull(scheduler);
    this.clusterName = checkNotBlank(clusterName);
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
    writeTemplate(resp, new Closure<StringTemplate>() {
      @Override public void execute(StringTemplate template) {
        template.setAttribute("cluster_name", clusterName);

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
          template.setAttribute("completedTasks", offsetAndLimit(completedTasks, offset));
          hasMore = completedTasks.size() > offset + PAGE_SIZE;
        }

        List<LiveTask> liveTasks = Lists.newArrayList(Iterables.transform(activeTasks,
            TaskState.STATE_TO_LIVE));

        Collections.sort(liveTasks, SHARD_ID_COMPARATOR);
        template.setAttribute("activeTasks", offsetAndLimit(liveTasks, offset));
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

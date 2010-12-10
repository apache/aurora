package com.twitter.mesos.scheduler.httphandlers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.SchedulerCore;
import com.twitter.mesos.scheduler.TaskStore.TaskState;
import org.apache.commons.lang.StringUtils;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.twitter.mesos.Tasks.jobKey;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;

/**
 * Simple redirector from the canonical name of a task to its configured HTTP port.
 *
 * @author wfarner
 */
public class Mname extends HttpServlet {

  private static final Pattern TASK_PATTERN = Pattern.compile("/([^/]+)/([^/]+)/(\\d+)(/.*)?");

  private static final List<String> HTTP_PORT_NAMES = ImmutableList.of(
      "health", "http", "HTTP", "web");

  private final SchedulerCore scheduler;

  @Inject
  public Mname(SchedulerCore scheduler) {
    this.scheduler = checkNotNull(scheduler);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    if (StringUtils.isBlank(req.getPathInfo())) {
      sendUsageError(resp);
    }

    Matcher matcher = TASK_PATTERN.matcher(req.getPathInfo());
    if (matcher.matches()) {
      String owner = matcher.group(1);
      String jobName = matcher.group(2);
      String shardIdStr = matcher.group(3);
      String forwardRequest = matcher.group(4);

      int shardId;
      try {
        shardId = Integer.parseInt(shardIdStr);
      } catch (NumberFormatException e) {
        resp.sendError(SC_BAD_REQUEST, String.format("'%s' is not a valid shard ID.", shardIdStr));
        return;
      }

      Set<TaskState> states = scheduler.getTasks(Query.liveShard(jobKey(owner, jobName), shardId));

      if (states.isEmpty()) {
        resp.sendError(SC_NOT_FOUND, "No such live shard found.");
        return;
      }

      TaskState state = Iterables.getOnlyElement(states);

      if (state.task.getStatus() != RUNNING) {
        resp.sendError(SC_NOT_FOUND, "The selected shard is currently in state "
                                     + state.task.getStatus());
        return;
      }

      String slaveHost = state.task.getAssignedTask().getSlaveHost();
      Map<String, Integer> leasedPorts = state.volatileState.resources.getLeasedPorts();
      Integer httpPort = null;
      for (String portName : HTTP_PORT_NAMES) {
        if (leasedPorts.containsKey(portName)) {
          httpPort = leasedPorts.get(portName);
        }
      }

      if (httpPort == null) {
        resp.sendError(SC_NOT_FOUND, "The task does not have a registered http port.");
        return;
      }

      String queryString = req.getQueryString();

      String redirect = String.format("http://%s:%d", slaveHost, httpPort);
      if (forwardRequest != null) redirect += forwardRequest;
      if (queryString != null) redirect += "?" + queryString;
      resp.sendRedirect(redirect);
    } else {
      sendUsageError(resp);
    }
  }

  private void sendUsageError(HttpServletResponse resp) throws IOException {
    resp.sendError(SC_BAD_REQUEST, "Request must be of the format /mname/owner/job/shard.");
  }
}

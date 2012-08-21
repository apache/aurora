package com.twitter.mesos.scheduler.httphandlers;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import org.apache.commons.lang.StringUtils;

import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.SchedulerCore;

import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.mesos.Tasks.jobKey;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;

/**
 * Simple redirector from the canonical name of a task to its configured HTTP port.
 */
public class Mname extends HttpServlet {

  private static final Pattern TASK_PATTERN = Pattern.compile("/([^/]+)/([^/]+)/(\\d+)(/.*)?");

  private static final Set<String> HTTP_PORT_NAMES = ImmutableSet.of(
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
      String role = matcher.group(1);
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

      ScheduledTask task = Iterables.getOnlyElement(
          scheduler.getTasks(Query.liveShard(jobKey(role, jobName), shardId)), null);
      if (task == null) {
        resp.sendError(SC_NOT_FOUND, "No such live shard found.");
        return;
      }

      if (task.getStatus() != RUNNING) {
        resp.sendError(SC_NOT_FOUND,
            "The selected shard is currently in state " + task.getStatus());
        return;
      }

      AssignedTask assignedTask = task.getAssignedTask();
      Optional<Integer> port = getRedirectPort(assignedTask);
      if (!port.isPresent()) {
        resp.sendError(SC_NOT_FOUND, "The task does not have a registered http port.");
        return;
      }

      String queryString = req.getQueryString();
      String redirect = String.format("http://%s:%d", assignedTask.getSlaveHost(), port.get());
      if (forwardRequest != null) {
        redirect += forwardRequest;
      }
      if (queryString != null) {
        redirect += "?" + queryString;
      }
      resp.sendRedirect(redirect);
    } else {
      sendUsageError(resp);
    }
  }

  @VisibleForTesting
  static Optional<Integer> getRedirectPort(AssignedTask task) {
    Map<String, Integer> ports = task.isSetAssignedPorts()
        ? task.getAssignedPorts() : ImmutableMap.<String, Integer>of();
    String httpPortName =
        Iterables.getFirst(Sets.intersection(ports.keySet(), HTTP_PORT_NAMES), null);
    return httpPortName == null ? Optional.<Integer>absent() : Optional.of(ports.get(httpPortName));
  }

  private void sendUsageError(HttpServletResponse resp) throws IOException {
    resp.sendError(SC_BAD_REQUEST, "Request must be of the format /mname/role/job/shard.");
  }
}

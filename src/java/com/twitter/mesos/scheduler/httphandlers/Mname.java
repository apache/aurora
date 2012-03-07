package com.twitter.mesos.scheduler.httphandlers;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

import org.apache.commons.lang.StringUtils;

import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.scheduler.LeaderRedirect;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.SchedulerCore;

import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.mesos.Tasks.jobKey;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;

/**
 * Simple redirector from the canonical name of a task to its configured HTTP port.
 *
 * @author William Farner
 */
public class Mname extends HttpServlet {

  private static final Pattern TASK_PATTERN = Pattern.compile("/([^/]+)/([^/]+)/(\\d+)(/.*)?");

  private static final List<String> HTTP_PORT_NAMES = ImmutableList.of(
      "health", "http", "HTTP", "web");

  private final SchedulerCore scheduler;
  private final LeaderRedirect redirector;

  @Inject
  public Mname(SchedulerCore scheduler, LeaderRedirect redirector) {
    this.scheduler = checkNotNull(scheduler);
    this.redirector = checkNotNull(redirector);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    Optional<String> leaderRedirect = redirector.getRedirectTarget(req);
    if (leaderRedirect.isPresent()) {
      resp.sendRedirect(leaderRedirect.get());
      return;
    }

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

      String slaveHost = task.getAssignedTask().getSlaveHost();
      Map<String, Integer> assignedPorts = task.getAssignedTask().isSetAssignedPorts()
          ? task.getAssignedTask().getAssignedPorts() : ImmutableMap.<String, Integer>of();
      Integer httpPort = Iterables.getFirst(
          Iterables.transform(HTTP_PORT_NAMES, Functions.forMap(assignedPorts)), null);
      if (httpPort == null) {
        resp.sendError(SC_NOT_FOUND, "The task does not have a registered http port.");
        return;
      }

      String queryString = req.getQueryString();
      String redirect = String.format("http://%s:%d", slaveHost, httpPort);
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

  private void sendUsageError(HttpServletResponse resp) throws IOException {
    resp.sendError(SC_BAD_REQUEST, "Request must be of the format /mname/role/job/shard.");
  }
}

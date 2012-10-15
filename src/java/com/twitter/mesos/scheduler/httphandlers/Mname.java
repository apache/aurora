package com.twitter.mesos.scheduler.httphandlers;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.SchedulerCore;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.mesos.Tasks.jobKey;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;

/**
 * Simple redirector from the canonical name of a task to its configured HTTP port.
 *
 * <p>Forwards for GET, PUT, POST and DELETE requests using HTTP 307 allowing compliant clients to
 * seamlessly perform re-directed mutations.
 */
@Path("/mname")
public class Mname {

  private static final Set<String> HTTP_PORT_NAMES = ImmutableSet.of(
      "health", "http", "HTTP", "web");

  private final SchedulerCore scheduler;

  @Inject
  public Mname(SchedulerCore scheduler) {
    this.scheduler = checkNotNull(scheduler);
  }

  @GET
  @Produces(MediaType.TEXT_HTML)
  public Response getUsage() {
    return Response
        .status(Status.BAD_REQUEST)
        .entity("<html>Usage: /mname/{role}/{job}/{shard}</html>")
        .build();
  }

  @GET
  @Path("/{role}/{job}/{shard}/{forward}")
  @Produces(MediaType.TEXT_HTML)
  public Response getWithForwardRequest(
      @PathParam("role") String role,
      @PathParam("job") String job,
      @PathParam("shard") int shardId,
      @PathParam("forward") String forward,
      @Context UriInfo uriInfo) {

    return get(role, job, shardId, uriInfo, Optional.of(forward));
  }

  @PUT
  @Path("/{role}/{job}/{shard}/{forward}")
  @Produces(MediaType.TEXT_HTML)
  public Response putWithForwardRequest(
      @PathParam("role") String role,
      @PathParam("job") String job,
      @PathParam("shard") int shardId,
      @PathParam("forward") String forward,
      @Context UriInfo uriInfo) {

    return get(role, job, shardId, uriInfo, Optional.of(forward));
  }

  @POST
  @Path("/{role}/{job}/{shard}/{forward}")
  @Produces(MediaType.TEXT_HTML)
  public Response postWithForwardRequest(
      @PathParam("role") String role,
      @PathParam("job") String job,
      @PathParam("shard") int shardId,
      @PathParam("forward") String forward,
      @Context UriInfo uriInfo) {

    return get(role, job, shardId, uriInfo, Optional.of(forward));
  }

  @DELETE
  @Path("/{role}/{job}/{shard}/{forward}")
  @Produces(MediaType.TEXT_HTML)
  public Response deleteWithForwardRequest(
      @PathParam("role") String role,
      @PathParam("job") String job,
      @PathParam("shard") int shardId,
      @PathParam("forward") String forward,
      @Context UriInfo uriInfo) {

    return get(role, job, shardId, uriInfo, Optional.of(forward));
  }

  @GET
  @Path("/{role}/{job}/{shard}")
  @Produces(MediaType.TEXT_HTML)
  public Response get(
      @PathParam("role") String role,
      @PathParam("job") String job,
      @PathParam("shard") int shardId,
      @Context UriInfo uriInfo) {

    return get(role, job, shardId, uriInfo, Optional.<String>absent());
  }

  @PUT
  @Path("/{role}/{job}/{shard}")
  @Produces(MediaType.TEXT_HTML)
  public Response put(
      @PathParam("role") String role,
      @PathParam("job") String job,
      @PathParam("shard") int shardId,
      @Context UriInfo uriInfo) {

    return get(role, job, shardId, uriInfo, Optional.<String>absent());
  }

  @POST
  @Path("/{role}/{job}/{shard}")
  @Produces(MediaType.TEXT_HTML)
  public Response post(
      @PathParam("role") String role,
      @PathParam("job") String job,
      @PathParam("shard") int shardId,
      @Context UriInfo uriInfo) {

    return get(role, job, shardId, uriInfo, Optional.<String>absent());
  }

  @DELETE
  @Path("/{role}/{job}/{shard}")
  @Produces(MediaType.TEXT_HTML)
  public Response delete(
      @PathParam("role") String role,
      @PathParam("job") String job,
      @PathParam("shard") int shardId,
      @Context UriInfo uriInfo) {

    return get(role, job, shardId, uriInfo, Optional.<String>absent());
  }

  private Response get(
      String role,
      String job,
      int shardId,
      UriInfo uriInfo,
      Optional<String> forwardRequest) {

    ScheduledTask task = Iterables.getOnlyElement(
        scheduler.getTasks(Query.liveShard(jobKey(role, job), shardId)), null);
    if (task == null) {
      return respond(NOT_FOUND, "No such live shard found.");
    }

    if (task.getStatus() != RUNNING) {
      return respond(NOT_FOUND, "The selected shard is currently in state " + task.getStatus());
    }

    AssignedTask assignedTask = task.getAssignedTask();
    Optional<Integer> port = getRedirectPort(assignedTask);
    if (!port.isPresent()) {
      return respond(NOT_FOUND, "The task does not have a registered http port.");
    }

    UriBuilder redirect = UriBuilder
        .fromPath(forwardRequest.or("/"))
        .scheme("http")
        .host(assignedTask.getSlaveHost())
        .port(port.get());
    for (Entry<String, List<String>> entry : uriInfo.getQueryParameters().entrySet()) {
      for (String value : entry.getValue()) {
        redirect.queryParam(entry.getKey(), value);
      }
    }

    return Response.temporaryRedirect(redirect.build()).build();
  }

  @VisibleForTesting
  static Optional<Integer> getRedirectPort(AssignedTask task) {
    Map<String, Integer> ports = task.isSetAssignedPorts()
        ? task.getAssignedPorts() : ImmutableMap.<String, Integer>of();
    String httpPortName =
        Iterables.getFirst(Sets.intersection(ports.keySet(), HTTP_PORT_NAMES), null);
    return httpPortName == null ? Optional.<Integer>absent() : Optional.of(ports.get(httpPortName));
  }

  private Response respond(Status status, String message) {
    return Response.status(status).entity(message).build();
  }
}

/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.http;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

import static java.util.Objects.requireNonNull;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;

import static org.apache.aurora.gen.ScheduleStatus.RUNNING;

/**
 * Simple redirector from the canonical name of a task to its configured HTTP port.
 *
 * <p>Forwards for GET, PUT, POST and DELETE requests using HTTP 307 allowing compliant clients to
 * seamlessly perform re-directed mutations.
 */
@Path("/mname")
public class Mname {

  private static final Set<String> HTTP_PORT_NAMES = ImmutableSet.of(
      "health", "http", "HTTP", "web", "admin");

  private final Storage storage;

  @Inject
  public Mname(Storage storage) {
    this.storage = requireNonNull(storage);
  }

  @GET
  public Response getUsage() {
    return Response
        .status(Status.BAD_REQUEST)
        .entity("<html>Usage: /mname/{role}/{env}/{job}/{instance}</html>")
        .build();
  }

  @GET
  @Path("/{role}/{env}/{job}/{instance}/{forward:.+}")
  public Response getWithForwardRequest(
      @PathParam("role") String role,
      @PathParam("env") String env,
      @PathParam("job") String job,
      @PathParam("instance") int instanceId,
      @PathParam("forward") String forward,
      @Context UriInfo uriInfo) {

    return get(role, env, job, instanceId, uriInfo, Optional.of(forward));
  }

  @PUT
  @Path("/{role}/{env}/{job}/{instance}/{forward:.+}")
  public Response putWithForwardRequest(
      @PathParam("role") String role,
      @PathParam("env") String env,
      @PathParam("job") String job,
      @PathParam("instance") int instanceId,
      @PathParam("forward") String forward,
      @Context UriInfo uriInfo) {

    return get(role, env, job, instanceId, uriInfo, Optional.of(forward));
  }

  @POST
  @Path("/{role}/{env}/{job}/{instance}/{forward:.+}")
  public Response postWithForwardRequest(
      @PathParam("role") String role,
      @PathParam("env") String env,
      @PathParam("job") String job,
      @PathParam("instance") int instanceId,
      @PathParam("forward") String forward,
      @Context UriInfo uriInfo) {

    return get(role, env, job, instanceId, uriInfo, Optional.of(forward));
  }

  @DELETE
  @Path("/{role}/{env}/{job}/{instance}/{forward:.+}")
  public Response deleteWithForwardRequest(
      @PathParam("role") String role,
      @PathParam("env") String env,
      @PathParam("job") String job,
      @PathParam("instance") int instanceId,
      @PathParam("forward") String forward,
      @Context UriInfo uriInfo) {

    return get(role, env, job, instanceId, uriInfo, Optional.of(forward));
  }

  @GET
  @Path("/{role}/{env}/{job}/{instance}")
  public Response get(
      @PathParam("role") String role,
      @PathParam("env") String env,
      @PathParam("job") String job,
      @PathParam("instance") int instanceId,
      @Context UriInfo uriInfo) {

    return get(role, env, job, instanceId, uriInfo, Optional.<String>absent());
  }

  @PUT
  @Path("/{role}/{env}/{job}/{instance}")
  public Response put(
      @PathParam("role") String role,
      @PathParam("env") String env,
      @PathParam("job") String job,
      @PathParam("instance") int instanceId,
      @Context UriInfo uriInfo) {

    return get(role, env, job, instanceId, uriInfo, Optional.<String>absent());
  }

  @POST
  @Path("/{role}/{env}/{job}/{instance}")
  public Response post(
      @PathParam("role") String role,
      @PathParam("env") String env,
      @PathParam("job") String job,
      @PathParam("instance") int instanceId,
      @Context UriInfo uriInfo) {

    return get(role, env, job, instanceId, uriInfo, Optional.<String>absent());
  }

  @DELETE
  @Path("/{role}/{env}/{job}/{instance}")
  public Response delete(
      @PathParam("role") String role,
      @PathParam("env") String env,
      @PathParam("job") String job,
      @PathParam("instance") int instanceId,
      @Context UriInfo uriInfo) {

    return get(role, env, job, instanceId, uriInfo, Optional.<String>absent());
  }

  private Response get(
      String role,
      String env,
      String job,
      int instanceId,
      UriInfo uriInfo,
      Optional<String> forwardRequest) {

    IScheduledTask task = Iterables.getOnlyElement(
        Storage.Util.fetchTasks(storage,
            Query.instanceScoped(JobKeys.from(role, env, job), instanceId).active()),
        null);
    if (task == null) {
      return respond(NOT_FOUND, "No such live instance found.");
    }

    if (task.getStatus() != RUNNING) {
      return respond(NOT_FOUND, "The selected instance is currently in state " + task.getStatus());
    }

    IAssignedTask assignedTask = task.getAssignedTask();
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
  static Optional<Integer> getRedirectPort(IAssignedTask task) {
    Map<String, Integer> ports = task.isSetAssignedPorts()
        ? task.getAssignedPorts() : ImmutableMap.<String, Integer>of();
    for (String httpPortName : HTTP_PORT_NAMES) {
      Integer port = ports.get(httpPortName);
      if (port != null) {
        return Optional.of(port);
      }
    }
    return Optional.absent();
  }

  private Response respond(Status status, String message) {
    return Response.status(status).entity(message).build();
  }
}

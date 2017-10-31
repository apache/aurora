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

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.google.common.base.Optional;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.Work.Quiet;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.thrift.TBase;

import static java.util.Objects.requireNonNull;

/**
 * Servlet that prints out the raw configuration for a specified struct.
 */
@Path("/structdump")
public class StructDump extends JerseyTemplateServlet {

  private final Storage storage;

  @Inject
  public StructDump(Storage storage) {
    super("structdump");
    this.storage = requireNonNull(storage);
  }

  private static final String USAGE =
      "<html>Print the internal thrift structure of task or cronjob. "
      + "<p>Usage: /structdump/task/{task_id} or /structdump/cron/{role}/{env}/{job}</p></html>";

  @GET
  @Produces(MediaType.TEXT_HTML)
  public Response getUsage() {
    return Response
        .status(Status.BAD_REQUEST)
        .entity(USAGE)
        .build();
  }

  /**
   * Dumps a task struct.
   *
   * @return HTTP response.
   */
  @GET
  @Path("/task/{task}")
  @Produces(MediaType.TEXT_HTML)
  public Response dumpJob(
      @PathParam("task") final String taskId) {

    return dumpEntity(
        "Task " + taskId,
        storeProvider ->
            storeProvider.getTaskStore().fetchTask(taskId).transform(IScheduledTask::newBuilder));
  }

  /**
   * Dumps a cron job struct.
   *
   * @return HTTP response.
   */
  @GET
  @Path("/cron/{role}/{environment}/{job}")
  @Produces(MediaType.TEXT_HTML)
  public Response dump(
      @PathParam("role") final String role,
      @PathParam("environment") final String environment,
      @PathParam("job") final String job) {

    final IJobKey jobKey = JobKeys.from(role, environment, job);
    return dumpEntity("Cron job " + JobKeys.canonicalString(jobKey),
        storeProvider -> storeProvider.getCronJobStore().fetchJob(jobKey)
            .transform(IJobConfiguration::newBuilder));
  }

  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

  private Response dumpEntity(String id, Quiet<Optional<? extends TBase<?, ?>>> work) {
    return fillTemplate(template -> {
      template.setAttribute("id", id);
      Optional<? extends TBase<?, ?>> struct = storage.read(work);
      if (struct.isPresent()) {
        template.setAttribute("structPretty", GSON.toJson(struct.get()));
        template.setAttribute("exception", null);
      } else {
        template.setAttribute("exception", "Entity not found");
      }
    });
  }
}

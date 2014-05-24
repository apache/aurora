/**
 *
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
import com.google.common.collect.Iterables;
import com.twitter.common.base.Closure;
import com.twitter.common.thrift.Util;

import org.antlr.stringtemplate.StringTemplate;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.cron.CronJobManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.Storage.Work;
import org.apache.aurora.scheduler.storage.Storage.Work.Quiet;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.thrift.TBase;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Servlet that prints out the raw configuration for a specified struct.
 */
@Path("/structdump")
public class StructDump extends JerseyTemplateServlet {

  private final Storage storage;
  private final CronJobManager cronJobManager;

  @Inject
  public StructDump(Storage storage, CronJobManager cronJobManager) {
    super("structdump");
    this.storage = checkNotNull(storage);
    this.cronJobManager = checkNotNull(cronJobManager);
  }

  private static final String USAGE =
    "<html>Usage: /structdump/task/{task_id} or /structdump/cron/{role}/{env}/{job}</html>";

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

    return dumpEntity("Task " + taskId, new Work.Quiet<Optional<? extends TBase<?, ?>>>() {
      @Override
      public Optional<? extends TBase<?, ?>> apply(StoreProvider storeProvider) {
        // Deep copy the struct to sidestep any subclass trickery inside the storage system.
        return Optional.fromNullable(Iterables.getOnlyElement(
                storeProvider.getTaskStore().fetchTasks(Query.taskScoped(taskId)),
                null)
            .newBuilder());
      }
    });
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
        new Work.Quiet<Optional<? extends TBase<?, ?>>>() {
          @Override
          public Optional<JobConfiguration> apply(StoreProvider storeProvider) {
            return storeProvider.getJobStore().fetchJob(cronJobManager.getManagerKey(), jobKey)
                .transform(IJobConfiguration.TO_BUILDER);
          }
        });
  }

  private Response dumpEntity(final String id, final Quiet<Optional<? extends TBase<?, ?>>> work) {
    return fillTemplate(new Closure<StringTemplate>() {
      @Override
      public void execute(StringTemplate template) {
        template.setAttribute("id", id);
        Optional<? extends TBase<?, ?>> struct = storage.weaklyConsistentRead(work);
        if (!struct.isPresent()) {
          template.setAttribute("exception", "Entity not found");
        } else {
          template.setAttribute("structPretty", Util.prettyPrint(struct.get()));
        }
      }
    });
  }
}

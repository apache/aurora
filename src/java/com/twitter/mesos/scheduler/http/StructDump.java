package com.twitter.mesos.scheduler.http;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

import org.antlr.stringtemplate.StringTemplate;
import org.apache.thrift.TBase;

import com.twitter.common.base.Closure;
import com.twitter.common.thrift.Util;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.JobKey;
import com.twitter.mesos.scheduler.CronJobManager;
import com.twitter.mesos.scheduler.base.JobKeys;
import com.twitter.mesos.scheduler.base.Query;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.Storage.Work.Quiet;

/**
 * Servlet that prints out the raw configuration for a specified struct.
 */
@Path("/structdump")
public class StructDump extends JerseyTemplateServlet {

  private final Storage storage;

  @Inject
  public StructDump(Storage storage) {
    super("structdump");
    this.storage = Preconditions.checkNotNull(storage);
  }

  @GET
  @Produces(MediaType.TEXT_HTML)
  public Response getUsage() {
    return Response
        .status(Status.BAD_REQUEST)
        .entity("<html>Usage: /structdump/{task_id} or /structdump/job/{role}/{env}/{job}</html>")
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
      @Override public Optional<? extends TBase<?, ?>> apply(StoreProvider storeProvider) {
        // Deep copy the struct to sidestep any subclass trickery inside the storage system.
        return Optional.fromNullable(Iterables.getOnlyElement(
                storeProvider.getTaskStore().fetchTasks(Query.taskScoped(taskId)),
                null))
            .transform(Tasks.DEEP_COPY_SCHEDULED);
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

    final JobKey jobKey = JobKeys.from(role, environment, job);
    return dumpEntity("Cron job " + JobKeys.toPath(jobKey),
        new Work.Quiet<Optional<? extends TBase<?, ?>>>() {
          @Override public Optional<JobConfiguration> apply(StoreProvider storeProvider) {
            return storeProvider.getJobStore().fetchJob(CronJobManager.MANAGER_KEY, jobKey);
          }
        });
  }

  private Response dumpEntity(final String id, final Quiet<Optional<? extends TBase<?, ?>>> work) {
    return fillTemplate(new Closure<StringTemplate>() {
      @Override public void execute(StringTemplate template) {
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

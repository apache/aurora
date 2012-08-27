package com.twitter.mesos.scheduler.httphandlers;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

import org.antlr.stringtemplate.StringTemplate;
import org.apache.thrift.TBase;

import com.twitter.common.base.Closure;
import com.twitter.common.thrift.Util;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.scheduler.CronJobManager;
import com.twitter.mesos.scheduler.Query;
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
        .entity("<html>Usage: /structdump/{task_id} or /structdump/job/{role}/{job}</html>")
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

    return dumpEntity("Task " + taskId, new Work.Quiet<TBase>() {
      @Override public TBase apply(StoreProvider storeProvider) {
        // Deep copy the struct to sidestep any subclass trickery inside the storage system.
        return new ScheduledTask(Iterables.getOnlyElement(
            storeProvider.getTaskStore().fetchTasks(Query.byId(taskId)), null));
      }
    });
  }

  /**
   * Dumps a cron job struct.
   *
   * @return HTTP response.
   */
  @GET
  @Path("/cron/{role}/{job}")
  @Produces(MediaType.TEXT_HTML)
  public Response dump(
      @PathParam("role") final String role,
      @PathParam("job") final String job) {

    final String key = Tasks.jobKey(role, job);
    return dumpEntity("Cron job " + key, new Work.Quiet<TBase>() {
      @Override public TBase apply(StoreProvider storeProvider) {
        return storeProvider.getJobStore().fetchJob(CronJobManager.MANAGER_KEY, key);
      }
    });
  }

  private Response dumpEntity(final String id, final Quiet<TBase> work) {
    return fillTemplate(new Closure<StringTemplate>() {
      @Override public void execute(StringTemplate template) {
        template.setAttribute("id", id);
        TBase struct = storage.doInTransaction(work);
        if (struct == null) {
          template.setAttribute("exception", "Entity not found");
        } else {
          template.setAttribute("structPretty", Util.prettyPrint(struct));
        }
      }
    });
  }
}

package com.twitter.mesos.scheduler.http;

import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import com.twitter.mesos.scheduler.state.CronJobManager;

/**
 * HTTP interface to dump state of the internal cron scheduler.
 */
@Path("/cron")
public class Cron {
  private final CronJobManager cronManager;

  @Inject
  Cron(CronJobManager cronManager) {
    this.cronManager = cronManager;
  }

  /**
   * Dumps the state of the cron manager.
   *
   * @return An HTTP response containing the cron manager's state.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response dumpContents() {
    Map<String, Object> response = ImmutableMap.<String, Object>builder()
        .put("scheduled", cronManager.getScheduledJobs())
        .put("pending", cronManager.getPendingRuns())
        .build();

   return Response.ok(response).build();
  }
}

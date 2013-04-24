package com.twitter.mesos.scheduler.http;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import com.twitter.mesos.scheduler.MaintenanceController;

/**
 * Servlet that exposes state of {@link MaintenanceController}.
 */
@Path("/maintenance")
public class Maintenance {
  private final MaintenanceController maintenance;

  @Inject
  Maintenance(MaintenanceController maintenance) {
    this.maintenance = Preconditions.checkNotNull(maintenance);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getOffers() {
    return Response.ok(maintenance.getDrainingTasks().asMap()).build();
  }
}

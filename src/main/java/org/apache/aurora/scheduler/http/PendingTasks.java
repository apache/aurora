/*
 * Copyright 2013 Twitter, Inc.
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
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.base.Preconditions;

import org.apache.aurora.scheduler.async.TaskGroups;

/**
 * Servlet that exposes detailed information about tasks that are pending.
 */
@Path("/pendingtasks")
public class PendingTasks {

  private final TaskGroups taskGroups;

  @Inject
  PendingTasks(TaskGroups taskGroups) {
    this.taskGroups = Preconditions.checkNotNull(taskGroups);
  }

  /**
   * Returns information about pending tasks.
   *
   * @return HTTP response.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getOffers() {
    return Response.ok(taskGroups.getGroups()).build();
  }
}

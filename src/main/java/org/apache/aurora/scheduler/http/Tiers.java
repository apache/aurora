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

import java.util.Objects;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import org.apache.aurora.scheduler.TierManager;

/*
 * HTTP interface that exposes tier configurations of scheduler.
 */
@Path("/tiers")
public class Tiers {

  private final TierManager tierManager;

  @Inject
  Tiers(TierManager tierManager) {
    this.tierManager = Objects.requireNonNull(tierManager);
  }

  /**
   * Dumps tier configuration file (tiers.json).
   *
   * @return HTTP response.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTiers() {
    ImmutableMap<String, Object> tierConfigurations = ImmutableMap.of(
        "default", tierManager.getDefaultTierName(),
        "tiers", tierManager.getTiers());
    return Response.ok(tierConfigurations).build();
  }
}

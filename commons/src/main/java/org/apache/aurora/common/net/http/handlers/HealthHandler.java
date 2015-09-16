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
package org.apache.aurora.common.net.http.handlers;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * A servlet that provides a crude mechanism for monitoring a service's health.  If the servlet
 * returns {@link #IS_HEALTHY} then the containing service should be deemed healthy.
 *
 * @author John Sirois
 */
@Path("/health")
public class HealthHandler {

  /**
   * A {@literal @Named} binding key for the Healthz servlet health checker.
   */
  public static final String HEALTH_CHECKER_KEY =
      "com.twitter.common.net.http.handlers.Healthz.checker";

  /**
   * The plain text response string this servlet returns in the body of its responses to health
   * check requests when its containing service is healthy.
   */
  public static final String IS_HEALTHY = "OK";

  private static final String IS_NOT_HEALTHY = "SICK";

  private final Supplier<Boolean> healthChecker;

  /**
   * Constructs a new Healthz that uses the given {@code healthChecker} to determine current health
   * of the service for at the point in time of each GET request.  The given {@code healthChecker}
   * should return {@code true} if the service is healthy and {@code false} otherwise.  If the
   * {@code healthChecker} returns null or throws the service is considered unhealthy.
   *
   * @param healthChecker a supplier that is called to perform a health check
   */
  @Inject
  public HealthHandler(@Named(HEALTH_CHECKER_KEY) Supplier<Boolean> healthChecker) {
    this.healthChecker = Preconditions.checkNotNull(healthChecker);
  }

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String getHealth() {
    return healthChecker.get() ? IS_HEALTHY : IS_NOT_HEALTHY;
  }
}

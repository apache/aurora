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

import java.util.Map;
import java.util.Objects;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.aurora.common.collections.Pair;

import static javax.servlet.http.HttpServletResponse.SC_BAD_GATEWAY;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static javax.servlet.http.HttpServletResponse.SC_SERVICE_UNAVAILABLE;

import static org.apache.aurora.scheduler.http.LeaderRedirect.LeaderStatus;
import static org.apache.aurora.scheduler.http.LeaderRedirect.LeaderStatus.LEADING;
import static org.apache.aurora.scheduler.http.LeaderRedirect.LeaderStatus.NOT_LEADING;
import static org.apache.aurora.scheduler.http.LeaderRedirect.LeaderStatus.NO_LEADER;

/**
 * Servlet that exposes the leader health. It is meant to be used by load balancers such as
 * ELB to always forward to the leading scheduler.
 */
@Path("/leaderhealth")
public class LeaderHealth {

  private static final Map<LeaderStatus, Pair<Integer, String>> RESPONSE_CODES = ImmutableMap.of(
      LEADING, Pair.of(SC_OK, "This is the leading scheduler."),
      NOT_LEADING, Pair.of(SC_SERVICE_UNAVAILABLE, "Another instance is the leading scheduler."),
      NO_LEADER, Pair.of(SC_BAD_GATEWAY, "There is no leading scheduler or the leader is unknown.")
  );

  private static final String HELP = generateHelpText();

  private final LeaderRedirect leaderRedirect;

  @Inject
  LeaderHealth(LeaderRedirect leaderRedirect) {
    this.leaderRedirect = Objects.requireNonNull(leaderRedirect);
  }

  @VisibleForTesting
  static String getHelpText(LeaderStatus status) {
    return "Current status: " + RESPONSE_CODES.get(status).getSecond()
        + "\n\n"
        + HELP;
  }

  /**
   * Gets the leadership status of the scheduler.
   *
   * @return HTTP response.
   */
  @GET
  public Response get() {
    LeaderStatus leaderStatus = leaderRedirect.getLeaderStatus();
    return Response.status(RESPONSE_CODES.get(leaderStatus).getFirst())
        .type(MediaType.TEXT_PLAIN_TYPE)
        .entity(getHelpText(leaderStatus))
        .build();
  }

  private static String generateHelpText() {
    Map<Integer, String> details = Maps.transformValues(
        Maps.uniqueIndex(RESPONSE_CODES.values(), Pair::getFirst),
        Pair::getSecond);

    return "This endpoint can indicate to a load balancer which among a group of schedulers "
        + "is leading.\n\nThe response codes are:\n"
        + Joiner.on("\n").withKeyValueSeparator(": ").join(details);
  }
}

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

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import javax.inject.Inject;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.aurora.gen.apiConstants.BYPASS_LEADER_REDIRECT_HEADER_NAME;
import static org.apache.aurora.scheduler.http.LeaderRedirect.LeaderStatus;

/**
 * An HTTP filter that will redirect the request to the leading scheduler.
 */
public class LeaderRedirectFilter extends AbstractFilter {
  private static final Logger LOG = LoggerFactory.getLogger(LeaderRedirectFilter.class);

  @VisibleForTesting
  static final String NO_LEADER_PAGE = "no-leader.html";

  private final LeaderRedirect redirector;

  @Inject
  LeaderRedirectFilter(LeaderRedirect redirector) {
    this.redirector = Objects.requireNonNull(redirector);
  }

  private void sendServiceUnavailable(HttpServletResponse response) throws IOException {
    response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
    Resources.asByteSource(Resources.getResource(LeaderRedirectFilter.class, NO_LEADER_PAGE))
        .copyTo(response.getOutputStream());
  }

  @Override
  public void doFilter(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws IOException, ServletException {

    if (request.getHeader(BYPASS_LEADER_REDIRECT_HEADER_NAME) != null) {
      LOG.info(BYPASS_LEADER_REDIRECT_HEADER_NAME + " header was present on the request, bypassing "
          + "leader redirection.");
      chain.doFilter(request, response);
      return;
    }

    LeaderStatus leaderStatus = redirector.getLeaderStatus();
    switch (leaderStatus) {
      case LEADING:
        chain.doFilter(request, response);
        return;
      case NO_LEADER:
        sendServiceUnavailable(response);
        return;
      case NOT_LEADING:
        Optional<String> leaderRedirect = redirector.getRedirectTarget(request);
        if (leaderRedirect.isPresent()) {
          response.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
          response.setHeader(HttpHeaders.LOCATION, leaderRedirect.get());
          return;
        }

        LOG.warn("Got no leader redirect contrary to leader status, treating as if no leader "
            + "was found.");
        sendServiceUnavailable(response);
        return;
      default:
        throw new IllegalArgumentException("Encountered unknown LeaderStatus: " + leaderStatus);
    }
  }
}

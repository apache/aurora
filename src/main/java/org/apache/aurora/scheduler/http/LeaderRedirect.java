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

import java.io.Closeable;
import java.io.IOException;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;

import org.apache.aurora.common.thrift.Endpoint;
import org.apache.aurora.common.thrift.ServiceInstance;
import org.apache.aurora.scheduler.app.ServiceGroupMonitor;
import org.apache.aurora.scheduler.app.ServiceGroupMonitor.MonitorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * Redirect logic for finding the leading scheduler in the event that this process is not the
 * leader.
 */
class LeaderRedirect implements Closeable {

  enum LeaderStatus {
    /**
     * This instance is currently the leading scheduler.
     */
    LEADING,

    /**
     * There is not currently an elected leading scheduler.
     */
    NO_LEADER,

    /**
     * This instance is not currently the leading scheduler.
     */
    NOT_LEADING,
  }

  private static final Logger LOG = LoggerFactory.getLogger(LeaderRedirect.class);

  private final HttpService httpService;
  private final ServiceGroupMonitor serviceGroupMonitor;

  @Inject
  LeaderRedirect(HttpService httpService, ServiceGroupMonitor serviceGroupMonitor) {
    this.httpService = requireNonNull(httpService);
    this.serviceGroupMonitor = requireNonNull(serviceGroupMonitor);
  }

  /**
   * Initiates the monitor that will watch the scheduler host set.
   *
   * @throws MonitorException If monitoring failed to initialize.
   */
  public void monitor() throws MonitorException {
    serviceGroupMonitor.start();
  }

  @Override
  public void close() throws IOException {
    serviceGroupMonitor.close();
  }

  private Optional<HostAndPort> getLeaderHttp() {
    Optional<ServiceInstance> leadingScheduler = getLeader();

    if (leadingScheduler.isPresent() && leadingScheduler.get().isSetServiceEndpoint()) {
      Endpoint leaderHttp = leadingScheduler.get().getServiceEndpoint();
      if (leaderHttp != null && leaderHttp.isSetHost() && leaderHttp.isSetPort()) {
        return Optional.of(HostAndPort.fromParts(leaderHttp.getHost(), leaderHttp.getPort()));
      }
    }

    LOG.warn("Leader service instance seems to be incomplete: " + leadingScheduler);
    return Optional.absent();
  }

  private Optional<HostAndPort> getLocalHttp() {
    HostAndPort localHttp = httpService.getAddress();
    return (localHttp == null) ? Optional.absent()
        : Optional.of(HostAndPort.fromParts(localHttp.getHost(), localHttp.getPort()));
  }

  /**
   * Gets the optional HTTP endpoint that should be redirected to in the event that this
   * scheduler is not the leader.
   *
   * @return Optional redirect target.
   */
  @VisibleForTesting
  Optional<HostAndPort> getRedirect() {
    Optional<HostAndPort> leaderHttp = getLeaderHttp();
    Optional<HostAndPort> localHttp = getLocalHttp();

    if (leaderHttp.isPresent()) {
      if (leaderHttp.equals(localHttp)) {
        return Optional.absent();
      } else {
        return leaderHttp;
      }
    } else {
      LOG.info("No leader found, not redirecting.");
      return Optional.absent();
    }
  }

  /**
   * Gets the current status of the elected leader.
   *
   * @return a {@code LeaderStatus} indicating whether there is an elected leader (and if so, if
   * this instance is the leader).
   */
  LeaderStatus getLeaderStatus() {
    Optional<ServiceInstance> leadingScheduler = getLeader();
    if (!leadingScheduler.isPresent()) {
      return LeaderStatus.NO_LEADER;
    }

    if (!leadingScheduler.get().isSetServiceEndpoint()) {
      LOG.warn("Leader service instance seems to be incomplete: " + leadingScheduler);
      return LeaderStatus.NO_LEADER;
    }

    Optional<HostAndPort> leaderHttp = getLeaderHttp();
    Optional<HostAndPort> localHttp = getLocalHttp();

    if (leaderHttp.isPresent() && leaderHttp.equals(localHttp)) {
      return LeaderStatus.LEADING;
    }

    return LeaderStatus.NOT_LEADING;
  }

  /**
   * Gets the optional redirect URI target in the event that this process is not the leading
   * scheduler.
   *
   * @param req HTTP request.
   * @return An optional redirect destination to route the request to the leading scheduler.
   */
  Optional<String> getRedirectTarget(HttpServletRequest req) {
    Optional<HostAndPort> redirectTarget = getRedirect();
    if (redirectTarget.isPresent()) {
      HostAndPort target = redirectTarget.get();
      StringBuilder redirect = new StringBuilder()
          .append(req.getScheme())
          .append("://")
          .append(target.getHost())
          .append(':')
          .append(target.getPort())
          .append(
              // If Jetty rewrote the path, we want to be sure to redirect to the original path
              // rather than the rewritten path to be sure it's a route the UI code recognizes.
              Optional.fromNullable(
                  req.getAttribute(JettyServerModule.ORIGINAL_PATH_ATTRIBUTE_NAME))
                  .or(req.getRequestURI()));

      String queryString = req.getQueryString();
      if (queryString != null) {
        redirect.append('?').append(queryString);
      }

      return Optional.of(redirect.toString());
    } else {
      return Optional.absent();
    }
  }

  private Optional<ServiceInstance> getLeader() {
    ImmutableSet<ServiceInstance> hostSet = serviceGroupMonitor.get();
    switch (hostSet.size()) {
      case 0:
        LOG.warn("No serviceGroupMonitor in host set, will not redirect despite not being leader.");
        return Optional.absent();
      case 1:
        LOG.debug("Found leader scheduler at {}", hostSet);
        return Optional.of(Iterables.getOnlyElement(hostSet));
      default:
        LOG.error("Multiple serviceGroupMonitor detected, will not redirect: {}", hostSet);
        return Optional.absent();
    }
  }
}

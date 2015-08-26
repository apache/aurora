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

import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Atomics;

import org.apache.aurora.common.net.pool.DynamicHostSet;
import org.apache.aurora.common.net.pool.DynamicHostSet.HostChangeMonitor;
import org.apache.aurora.common.net.pool.DynamicHostSet.MonitorException;
import org.apache.aurora.common.thrift.Endpoint;
import org.apache.aurora.common.thrift.ServiceInstance;

import static java.util.Objects.requireNonNull;

/**
 * Redirect logic for finding the leading scheduler in the event that this process is not the
 * leader.
 */
public class LeaderRedirect {

  // TODO(wfarner): Should we tie this directly to the producer of the node (HttpModule?  It seems
  // like the right thing to do, but would introduce an otherwise unnecessary dependency.
  @VisibleForTesting
  static final String HTTP_PORT_NAME = "http";

  private static final Logger LOG = Logger.getLogger(LeaderRedirect.class.getName());

  private final HttpService httpService;
  private final DynamicHostSet<ServiceInstance> schedulers;

  private final AtomicReference<ServiceInstance> leader = Atomics.newReference();

  @Inject
  LeaderRedirect(HttpService httpService, DynamicHostSet<ServiceInstance> schedulers) {
    this.httpService = requireNonNull(httpService);
    this.schedulers = requireNonNull(schedulers);
  }

  /**
   * Initiates the monitor that will watch the scheduler host set.
   *
   * @throws MonitorException If monitoring failed to initialize.
   */
  public void monitor() throws MonitorException {
    schedulers.watch(new SchedulerMonitor());
  }

  private Optional<HostAndPort> getLeaderHttp() {
    ServiceInstance leadingScheduler = leader.get();
    if (leadingScheduler == null) {
      return Optional.absent();
    }

    if (leadingScheduler.isSetAdditionalEndpoints()) {
      Endpoint leaderHttp = leadingScheduler.getAdditionalEndpoints().get(HTTP_PORT_NAME);
      if (leaderHttp != null && leaderHttp.isSetHost() && leaderHttp.isSetPort()) {
        return Optional.of(HostAndPort.fromParts(leaderHttp.getHost(), leaderHttp.getPort()));
      }
    }

    LOG.warning("Leader service instance seems to be incomplete: " + leadingScheduler);
    return Optional.absent();
  }

  private Optional<HostAndPort> getLocalHttp() {
    HostAndPort localHttp = httpService.getAddress();
    return (localHttp == null) ? Optional.absent()
        : Optional.of(HostAndPort.fromParts(localHttp.getHostText(), localHttp.getPort()));
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
   * Gets the optional redirect URI target in the event that this process is not the leading
   * scheduler.
   *
   * @param req HTTP request.
   * @return An optional redirect destination to route the request to the leading scheduler.
   */
  public Optional<String> getRedirectTarget(HttpServletRequest req) {
    Optional<HostAndPort> redirectTarget = getRedirect();
    if (redirectTarget.isPresent()) {
      HostAndPort target = redirectTarget.get();
      StringBuilder redirect = new StringBuilder()
          .append(req.getScheme())
          .append("://")
          .append(target.getHostText())
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

  /**
   * Monitor to track scheduler leader changes.
   */
  private class SchedulerMonitor implements HostChangeMonitor<ServiceInstance> {
    @Override
    public void onChange(ImmutableSet<ServiceInstance> hostSet) {
      switch (hostSet.size()) {
        case 0:
          LOG.warning("No schedulers in host set, will not redirect despite not being leader.");
          leader.set(null);
          break;

        case 1:
          LOG.info("Found leader scheduler at " + hostSet);
          leader.set(Iterables.getOnlyElement(hostSet));
          break;

        default:
          LOG.severe("Multiple schedulers detected, will not redirect: " + hostSet);
          leader.set(null);
          break;
      }
    }
  }
}

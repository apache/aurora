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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.http.HttpServletRequest;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.thrift.Endpoint;
import org.apache.aurora.common.thrift.ServiceInstance;
import org.apache.aurora.scheduler.app.ServiceGroupMonitor;
import org.apache.aurora.scheduler.app.ServiceGroupMonitor.MonitorException;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.http.LeaderRedirect.LeaderStatus;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;

public class LeaderRedirectTest extends EasyMockTest {

  private static final int HTTP_PORT = 500;

  private static final Function<HostAndPort, ServiceInstance> CREATE_INSTANCE =
      endpoint -> new ServiceInstance()
          .setServiceEndpoint(new Endpoint(endpoint.getHostText(), endpoint.getPort()));

  private AtomicReference<ImmutableSet<ServiceInstance>> schedulers;
  private ServiceGroupMonitor serviceGroupMonitor;
  private LeaderRedirect leaderRedirector;

  @Before
  public void setUp() throws MonitorException {
    schedulers = new AtomicReference<>(ImmutableSet.of());
    serviceGroupMonitor = createMock(ServiceGroupMonitor.class);

    HttpService http = createMock(HttpService.class);
    expect(http.getAddress()).andStubReturn(HostAndPort.fromParts("localhost", HTTP_PORT));

    leaderRedirector = new LeaderRedirect(http, serviceGroupMonitor);
  }

  private void replayAndMonitor(int expectedGetCalls) throws Exception {
    serviceGroupMonitor.start();
    expectLastCall();

    expect(serviceGroupMonitor.get()).andAnswer(() -> schedulers.get()).times(expectedGetCalls);

    control.replay();
    leaderRedirector.monitor();
  }

  @Test
  public void testLeader() throws Exception {
    replayAndMonitor(3);
    publishSchedulers(localPort(HTTP_PORT));

    assertEquals(Optional.absent(), leaderRedirector.getRedirect());

    // NB: LEADING takes 2 tests of the server group membership to calculate; thus we expect 3
    // server group get calls, 1 for the getRedirect() above and 2 here.
    assertEquals(LeaderStatus.LEADING, leaderRedirector.getLeaderStatus());
  }

  @Test
  public void testNotLeader() throws Exception {
    replayAndMonitor(3);

    HostAndPort remote = HostAndPort.fromParts("foobar", HTTP_PORT);
    publishSchedulers(remote);

    assertEquals(Optional.of(remote), leaderRedirector.getRedirect());

    // NB: NOT_LEADING takes 2 tests of the server group membership to calculate; thus we expect 3
    // server group get calls, 1 for the getRedirect() above and 2 here.
    assertEquals(LeaderStatus.NOT_LEADING, leaderRedirector.getLeaderStatus());
  }

  @Test
  public void testLeaderOnSameHost() throws Exception {
    replayAndMonitor(3);

    HostAndPort local = localPort(555);
    publishSchedulers(local);

    assertEquals(Optional.of(local), leaderRedirector.getRedirect());

    // NB: NOT_LEADING takes 2 tests of the server group membership to calculate; thus we expect 3
    // server group get calls, 1 for the getRedirect() above and 2 here.
    assertEquals(LeaderStatus.NOT_LEADING, leaderRedirector.getLeaderStatus());
  }

  @Test
  public void testNoLeaders() throws Exception {
    replayAndMonitor(2);

    assertEquals(Optional.absent(), leaderRedirector.getRedirect());
    assertEquals(LeaderStatus.NO_LEADER, leaderRedirector.getLeaderStatus());
  }

  @Test
  public void testMultipleLeaders() throws Exception {
    replayAndMonitor(2);

    publishSchedulers(HostAndPort.fromParts("foobar", 500), HostAndPort.fromParts("baz", 800));

    assertEquals(Optional.absent(), leaderRedirector.getRedirect());
    assertEquals(LeaderStatus.NO_LEADER, leaderRedirector.getLeaderStatus());
  }

  @Test
  public void testBadServiceInstance() throws Exception {
    replayAndMonitor(2);

    publishSchedulers(ImmutableSet.of(new ServiceInstance()));

    assertEquals(Optional.absent(), leaderRedirector.getRedirect());
    assertEquals(LeaderStatus.NO_LEADER, leaderRedirector.getLeaderStatus());
  }

  private HttpServletRequest mockRequest(String attributeValue, String queryString) {
    HttpServletRequest mockRequest = createMock(HttpServletRequest.class);
    expect(mockRequest.getScheme()).andReturn("http");
    expect(mockRequest.getAttribute(JettyServerModule.ORIGINAL_PATH_ATTRIBUTE_NAME))
        .andReturn(attributeValue);
    expect(mockRequest.getRequestURI()).andReturn("/some/path");
    expect(mockRequest.getQueryString()).andReturn(queryString);

    return mockRequest;
  }

  @Test
  public void testRedirectTargetNoAttribute() throws Exception {
    HttpServletRequest mockRequest = mockRequest(null, null);

    replayAndMonitor(1);

    HostAndPort remote = HostAndPort.fromParts("foobar", HTTP_PORT);
    publishSchedulers(remote);

    assertEquals(
        Optional.of("http://foobar:500/some/path"),
        leaderRedirector.getRedirectTarget(mockRequest));
  }

  @Test
  public void testRedirectTargetWithAttribute() throws Exception {
    HttpServletRequest mockRequest = mockRequest("/the/original/path", null);

    replayAndMonitor(1);

    HostAndPort remote = HostAndPort.fromParts("foobar", HTTP_PORT);
    publishSchedulers(remote);

    assertEquals(
        Optional.of("http://foobar:500/the/original/path"),
        leaderRedirector.getRedirectTarget(mockRequest));
  }

  @Test
  public void testRedirectTargetQueryString() throws Exception {
    HttpServletRequest mockRequest = mockRequest(null, "bar=baz");

    replayAndMonitor(1);

    HostAndPort remote = HostAndPort.fromParts("foobar", HTTP_PORT);
    publishSchedulers(remote);

    assertEquals(
        Optional.of("http://foobar:500/some/path?bar=baz"),
        leaderRedirector.getRedirectTarget(mockRequest));
  }

  private void publishSchedulers(HostAndPort... schedulerHttpEndpoints) {
    publishSchedulers(ImmutableSet.copyOf(Iterables.transform(Arrays.asList(schedulerHttpEndpoints),
        CREATE_INSTANCE)));
  }

  private void publishSchedulers(ImmutableSet<ServiceInstance> instances) {
    schedulers.set(instances);
  }

  private static HostAndPort localPort(int port) {
    return HostAndPort.fromParts("localhost", port);
  }
}

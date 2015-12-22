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

import javax.servlet.http.HttpServletRequest;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;

import org.apache.aurora.common.net.pool.DynamicHostSet;
import org.apache.aurora.common.net.pool.DynamicHostSet.HostChangeMonitor;
import org.apache.aurora.common.net.pool.DynamicHostSet.MonitorException;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.thrift.Endpoint;
import org.apache.aurora.common.thrift.ServiceInstance;
import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.http.LeaderRedirect.LeaderStatus;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class LeaderRedirectTest extends EasyMockTest {

  private static final int HTTP_PORT = 500;

  private static final Function<HostAndPort, ServiceInstance> CREATE_INSTANCE =
      endpoint -> new ServiceInstance()
          .setServiceEndpoint(new Endpoint(endpoint.getHostText(), endpoint.getPort()));

  private Capture<HostChangeMonitor<ServiceInstance>> monitorCapture;

  private LeaderRedirect leaderRedirector;

  @Before
  public void setUp() throws MonitorException {
    DynamicHostSet<ServiceInstance> schedulers =
        createMock(new Clazz<DynamicHostSet<ServiceInstance>>() { });

    HttpService http = createMock(HttpService.class);
    expect(http.getAddress()).andStubReturn(HostAndPort.fromParts("localhost", HTTP_PORT));

    leaderRedirector = new LeaderRedirect(http, schedulers);

    monitorCapture = new Capture<>();
    expect(schedulers.watch(capture(monitorCapture))).andReturn(null);
  }

  private void replayAndMonitor() throws Exception {
    control.replay();
    leaderRedirector.monitor();
  }

  @Test
  public void testLeader() throws Exception {
    replayAndMonitor();
    publishSchedulers(localPort(HTTP_PORT));

    assertEquals(Optional.absent(), leaderRedirector.getRedirect());
    assertEquals(LeaderStatus.LEADING, leaderRedirector.getLeaderStatus());
  }

  @Test
  public void testNotLeader() throws Exception {
    replayAndMonitor();

    HostAndPort remote = HostAndPort.fromParts("foobar", HTTP_PORT);
    publishSchedulers(remote);

    assertEquals(Optional.of(remote), leaderRedirector.getRedirect());
    assertEquals(LeaderStatus.NOT_LEADING, leaderRedirector.getLeaderStatus());
  }

  @Test
  public void testLeaderOnSameHost() throws Exception {
    replayAndMonitor();

    HostAndPort local = localPort(555);
    publishSchedulers(local);

    assertEquals(Optional.of(local), leaderRedirector.getRedirect());
    assertEquals(LeaderStatus.NOT_LEADING, leaderRedirector.getLeaderStatus());
  }

  @Test
  public void testNoLeaders() throws Exception {
    replayAndMonitor();

    assertEquals(Optional.absent(), leaderRedirector.getRedirect());
    assertEquals(LeaderStatus.NO_LEADER, leaderRedirector.getLeaderStatus());
  }

  @Test
  public void testMultipleLeaders() throws Exception {
    replayAndMonitor();

    publishSchedulers(HostAndPort.fromParts("foobar", 500), HostAndPort.fromParts("baz", 800));

    assertEquals(Optional.absent(), leaderRedirector.getRedirect());
    assertEquals(LeaderStatus.NO_LEADER, leaderRedirector.getLeaderStatus());
  }

  @Test
  public void testBadServiceInstance() throws Exception {
    replayAndMonitor();

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

    replayAndMonitor();

    HostAndPort remote = HostAndPort.fromParts("foobar", HTTP_PORT);
    publishSchedulers(remote);

    assertEquals(
        Optional.of("http://foobar:500/some/path"),
        leaderRedirector.getRedirectTarget(mockRequest));
  }

  @Test
  public void testRedirectTargetWithAttribute() throws Exception {
    HttpServletRequest mockRequest = mockRequest("/the/original/path", null);

    replayAndMonitor();

    HostAndPort remote = HostAndPort.fromParts("foobar", HTTP_PORT);
    publishSchedulers(remote);

    assertEquals(
        Optional.of("http://foobar:500/the/original/path"),
        leaderRedirector.getRedirectTarget(mockRequest));
  }

  @Test
  public void testRedirectTargetQueryString() throws Exception {
    HttpServletRequest mockRequest = mockRequest(null, "bar=baz");

    replayAndMonitor();

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
    monitorCapture.getValue().onChange(instances);
  }

  private static HostAndPort localPort(int port) {
    return HostAndPort.fromParts("localhost", port);
  }
}

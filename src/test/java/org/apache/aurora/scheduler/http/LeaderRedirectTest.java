/**
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

import java.util.Arrays;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;
import com.google.inject.util.Providers;
import com.twitter.common.application.ShutdownRegistry.ShutdownRegistryImpl;
import com.twitter.common.application.modules.LifecycleModule.ServiceRunner;
import com.twitter.common.application.modules.LocalServiceRegistry;
import com.twitter.common.application.modules.LocalServiceRegistry.LocalService;
import com.twitter.common.base.Commands;
import com.twitter.common.net.pool.DynamicHostSet;
import com.twitter.common.net.pool.DynamicHostSet.HostChangeMonitor;
import com.twitter.common.net.pool.DynamicHostSet.MonitorException;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.thrift.Endpoint;
import com.twitter.thrift.ServiceInstance;

import org.easymock.Capture;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.aurora.scheduler.http.LeaderRedirect.HTTP_PORT_NAME;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class LeaderRedirectTest extends EasyMockTest {

  private static final int HTTP_PORT = 500;

  private static final Function<HostAndPort, ServiceInstance> CREATE_INSTANCE =
      new Function<HostAndPort, ServiceInstance>() {
        @Override
        public ServiceInstance apply(HostAndPort endpoint) {
          return new ServiceInstance()
              .setAdditionalEndpoints(ImmutableMap.of(HTTP_PORT_NAME,
                  new Endpoint(endpoint.getHostText(), endpoint.getPort())));
        }
      };

  private Capture<HostChangeMonitor<ServiceInstance>> monitorCapture;

  private LeaderRedirect leaderRedirector;

  @Before
  public void setUp() throws MonitorException {
    DynamicHostSet<ServiceInstance> schedulers =
        createMock(new Clazz<DynamicHostSet<ServiceInstance>>() { });

    ServiceRunner fakeRunner = new ServiceRunner() {
      @Override
      public LocalService launch() {
        return LocalService.auxiliaryService(HTTP_PORT_NAME, HTTP_PORT, Commands.NOOP);
      }
    };

    Set<ServiceRunner> services = ImmutableSet.of(fakeRunner);
    LocalServiceRegistry serviceRegistry =
        new LocalServiceRegistry(Providers.of(services), new ShutdownRegistryImpl());
    leaderRedirector = new LeaderRedirect(serviceRegistry, schedulers);

    monitorCapture = new Capture<>();
    expect(schedulers.watch(capture(monitorCapture))).andReturn(null);
    control.replay();
    leaderRedirector.monitor();
  }

  @Ignore("http://jira.local.twitter.com/browse/MESOS-323")
  @Test
  public void testLeader() {
    publishSchedulers(localPort(HTTP_PORT));

    assertEquals(Optional.<HostAndPort>absent(), leaderRedirector.getRedirect());
  }

  @Test
  public void testNotLeader() {
    HostAndPort remote = HostAndPort.fromParts("foobar", HTTP_PORT);
    publishSchedulers(remote);

    assertEquals(Optional.of(remote), leaderRedirector.getRedirect());
  }

  @Test
  public void testLeaderOnSameHost() {
    HostAndPort local = localPort(555);
    publishSchedulers(local);

    assertEquals(Optional.of(local), leaderRedirector.getRedirect());
  }

  @Test
  public void testNoLeaders() {
    assertEquals(Optional.<HostAndPort>absent(), leaderRedirector.getRedirect());
  }

  @Test
  public void testMultipleLeaders() {
    publishSchedulers(HostAndPort.fromParts("foobar", 500), HostAndPort.fromParts("baz", 800));

    assertEquals(Optional.<HostAndPort>absent(), leaderRedirector.getRedirect());
  }

  @Test
  public void testBadServiceInstance() {
    ServiceInstance badLocal = new ServiceInstance()
        .setAdditionalEndpoints(ImmutableMap.of("foo", new Endpoint("localhost", 500)));

    publishSchedulers(ImmutableSet.of(badLocal));

    assertEquals(Optional.<HostAndPort>absent(), leaderRedirector.getRedirect());
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

package com.twitter.mesos.scheduler;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;

import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.application.LocalServiceRegistry;
import com.twitter.common.net.pool.DynamicHostSet;
import com.twitter.common.net.pool.DynamicHostSet.HostChangeMonitor;
import com.twitter.common.net.pool.DynamicHostSet.MonitorException;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.thrift.Endpoint;
import com.twitter.thrift.ServiceInstance;

import static com.twitter.mesos.scheduler.LeaderRedirect.HTTP_PORT_NAME;
import static org.easymock.EasyMock.capture;
import static org.junit.Assert.assertEquals;

/**
 * @author William Farner
 */
public class LeaderRedirectTest extends EasyMockTest {

  private LocalServiceRegistry serviceRegistry;
  private Capture<HostChangeMonitor<ServiceInstance>> monitorCapture;

  private LeaderRedirect leaderRedirector;

  @Before
  public void setUp() throws MonitorException {
    serviceRegistry = new LocalServiceRegistry(ImmutableSet.of(HTTP_PORT_NAME));
    DynamicHostSet<ServiceInstance> schedulers =
        createMock(new Clazz<DynamicHostSet<ServiceInstance>>() { });
    leaderRedirector = new LeaderRedirect(serviceRegistry, schedulers);

    monitorCapture = new Capture<HostChangeMonitor<ServiceInstance>>();
    schedulers.monitor(capture(monitorCapture));
    control.replay();
    leaderRedirector.monitor();
  }

  @Test
  public void testLeader() throws Exception {
    serviceRegistry.announce(HTTP_PORT_NAME, 500, false);
    HostAndPort local = localPort(500);
    publishSchedulers(local);

    assertEquals(Optional.<HostAndPort>absent(), leaderRedirector.getRedirect());
  }

  @Test
  public void testNotLeader() {
    serviceRegistry.announce(HTTP_PORT_NAME, 500, false);
    HostAndPort remote = HostAndPort.fromParts("foobar", 500);
    publishSchedulers(remote);

    assertEquals(Optional.of(remote), leaderRedirector.getRedirect());
  }

  @Test
  public void testLeaderOnSameHost() throws Exception {
    serviceRegistry.announce(HTTP_PORT_NAME, 500, false);
    HostAndPort local = localPort(555);
    publishSchedulers(local);

    assertEquals(Optional.of(local), leaderRedirector.getRedirect());
  }

  @Test
  public void testNoLeaders() throws Exception {
    serviceRegistry.announce(HTTP_PORT_NAME, 500, false);

    assertEquals(Optional.<HostAndPort>absent(), leaderRedirector.getRedirect());
  }

  @Test
  public void testMultipleLeaders() throws Exception {
    serviceRegistry.announce(HTTP_PORT_NAME, 500, false);
    publishSchedulers(HostAndPort.fromParts("foobar", 500), HostAndPort.fromParts("baz", 800));

    assertEquals(Optional.<HostAndPort>absent(), leaderRedirector.getRedirect());
  }

  @Test
  public void testBadServiceInstance() throws Exception {
    serviceRegistry.announce(HTTP_PORT_NAME, 500, false);

    ServiceInstance badLocal = new ServiceInstance()
        .setAdditionalEndpoints(ImmutableMap.of("foo",
            new Endpoint(InetAddress.getLocalHost().getHostAddress(), 500)));

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

  private static HostAndPort localPort(int port) throws UnknownHostException {
    return HostAndPort.fromParts(InetAddress.getLocalHost().getHostAddress(), port);
  }

  private static final Function<HostAndPort, ServiceInstance> CREATE_INSTANCE =
      new Function<HostAndPort, ServiceInstance>() {
        @Override public ServiceInstance apply(HostAndPort endpoint) {
          return new ServiceInstance()
              .setAdditionalEndpoints(ImmutableMap.of(HTTP_PORT_NAME,
                  new Endpoint(endpoint.getHostText(), endpoint.getPort())));
        }
      };
}

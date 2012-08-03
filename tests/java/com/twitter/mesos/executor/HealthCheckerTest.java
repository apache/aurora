package com.twitter.mesos.executor;

import org.junit.Before;
import org.junit.Test;

import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.executor.HealthChecker.HealthCheckException;
import com.twitter.mesos.executor.HttpSignaler.Method;
import com.twitter.mesos.executor.HttpSignaler.SignalException;

import static org.easymock.EasyMock.expect;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class HealthCheckerTest extends EasyMockTest {

  private static final String SIGNAL_URL = "http://localhost:8080/health";

  private HttpSignaler signaler;
  private HealthChecker healthChecker;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    signaler = createMock(HttpSignaler.class);
    healthChecker = new HealthChecker(signaler);
  }

  @Test
  public void testHealthy() throws SignalException, HealthCheckException {
    expect(signaler.signal(Method.GET, SIGNAL_URL)).andReturn("ok");
    expect(signaler.signal(Method.GET, SIGNAL_URL)).andReturn("ok   \t\r\n");
    expect(signaler.signal(Method.GET, SIGNAL_URL)).andReturn("OK");

    control.replay();

    assertThat(healthChecker.apply(8080), is(Boolean.TRUE));
    assertThat(healthChecker.apply(8080), is(Boolean.TRUE));
    assertThat(healthChecker.apply(8080), is(Boolean.TRUE));
  }

  @Test
  public void testUnhealthy() throws SignalException, HealthCheckException {
    expect(signaler.signal(Method.GET, SIGNAL_URL)).andReturn("");

    control.replay();

    assertThat(healthChecker.apply(8080), is(Boolean.FALSE));
  }

  @Test(expected = HealthCheckException.class)
  public void testSignalingFailed() throws SignalException, HealthCheckException {
    expect(signaler.signal(Method.GET, SIGNAL_URL)).andThrow(new SignalException("Signal failed."));

    control.replay();

    assertThat(healthChecker.apply(8080), is(Boolean.FALSE));
  }
}

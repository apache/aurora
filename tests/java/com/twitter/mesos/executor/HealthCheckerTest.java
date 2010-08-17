package com.twitter.mesos.executor;

import com.twitter.common.base.ExceptionalFunction;
import com.twitter.mesos.executor.HealthChecker.HealthCheckException;
import com.twitter.mesos.executor.HttpSignaler.SignalException;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.easymock.EasyMock.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

/**
 * @author wfarner
 */
public class HealthCheckerTest {

  private static final String SIGNAL_URL = "http://localhost:8080/healthz";

  private IMocksControl control;
  private ExceptionalFunction<String, List<String>, SignalException> signaler;
  private HealthChecker healthChecker;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    control = createControl();
    signaler = control.createMock(ExceptionalFunction.class);
    healthChecker = new HealthChecker(signaler);
  }

  @Test
  public void testHealthy() throws SignalException, HealthCheckException {
    expect(signaler.apply(SIGNAL_URL)).andReturn(Arrays.asList("ok"));

    control.replay();

    assertThat(healthChecker.apply(8080), is(Boolean.TRUE));
  }

  @Test
  public void testHealthyWithWhitespace() throws SignalException, HealthCheckException {
    expect(signaler.apply(SIGNAL_URL)).andReturn(Arrays.asList("\t\r\n  ok\n\t \r"));

    control.replay();

    assertThat(healthChecker.apply(8080), is(Boolean.TRUE));
  }

  @Test
  public void testUnhealthy() throws SignalException, HealthCheckException {
    expect(signaler.apply(SIGNAL_URL)).andReturn(Arrays.asList(""));

    control.replay();

    assertThat(healthChecker.apply(8080), is(Boolean.FALSE));
  }

  @Test(expected = HealthCheckException.class)
  public void testSignalingFailed() throws SignalException, HealthCheckException {
    expect(signaler.apply(SIGNAL_URL)).andThrow(new SignalException("Signal failed."));

    control.replay();

    assertThat(healthChecker.apply(8080), is(Boolean.FALSE));
  }
}

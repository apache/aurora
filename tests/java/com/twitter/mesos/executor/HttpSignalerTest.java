package com.twitter.mesos.executor;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.executor.HttpSignaler.HttpSignalerImpl;
import com.twitter.mesos.executor.HttpSignaler.Method;
import com.twitter.mesos.executor.HttpSignaler.SignalException;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;

public class HttpSignalerTest extends EasyMockTest {

  private static final String TEST_URL = "http://localhost:8080/quitquitquit";

  private static final Amount<Long, Time> SIGNAL_TIMEOUT = Amount.of(1L, Time.SECONDS);

  private ExecutorService executor;
  private Future<String> future;
  private HttpSignaler signaler;

  @Before
  public void setUp() {
    executor = control.createMock(ExecutorService.class);
    future = createMock(new Clazz<Future<String>>() { });
    signaler = new HttpSignalerImpl(executor, SIGNAL_TIMEOUT);
  }

  @Test(expected = SignalException.class)
  public void testTimeout() throws Exception {
    Capture<Callable<String>> capture = new Capture<Callable<String>>();
    expect(executor.submit(capture(capture))).andReturn(future);
    expect(future.get(SIGNAL_TIMEOUT.as(Time.MILLISECONDS), TimeUnit.MILLISECONDS))
        .andThrow(new TimeoutException());

    control.replay();

    signaler.signal(Method.GET, TEST_URL);
  }

  @Test(expected = SignalException.class)
  public void testExecutionException() throws Exception {
    Capture<Callable<String>> capture = new Capture<Callable<String>>();
    expect(executor.submit(capture(capture))).andReturn(future);
    expect(future.get(SIGNAL_TIMEOUT.as(Time.MILLISECONDS), TimeUnit.MILLISECONDS))
        .andThrow(new ExecutionException(new Exception()));

    control.replay();

    signaler.signal(Method.GET, TEST_URL);
  }

  @Test(expected = SignalException.class)
  public void testInterruptedException() throws Exception {
    Capture<Callable<String>> capture = new Capture<Callable<String>>();
    expect(executor.submit(capture(capture))).andReturn(future);
    expect(future.get(SIGNAL_TIMEOUT.as(Time.MILLISECONDS), TimeUnit.MILLISECONDS))
        .andThrow(new InterruptedException());

    control.replay();

    signaler.signal(Method.GET, TEST_URL);
  }
}

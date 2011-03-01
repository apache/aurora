package com.twitter.mesos.executor;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.mesos.executor.HttpSignaler.SignalException;
import org.easymock.Capture;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.easymock.EasyMock.*;

/**
 * @author William Farner
 */
public class HttpSignalerTest {

  private static final String TEST_URL = "http://localhost:8080/quitquitquit";

  private static final Amount<Long, Time> SIGNAL_TIMEOUT = Amount.of(1L, Time.SECONDS);

  private IMocksControl control;
  private ExecutorService executor;
  private Future<List<String>> future;
  private HttpSignaler signaler;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    control = createControl();
    executor = control.createMock(ExecutorService.class);
    future = control.createMock(Future.class);
    signaler = new HttpSignaler(executor, SIGNAL_TIMEOUT);
  }

  @Test(expected = SignalException.class)
  public void testTimeout() throws Exception {
    Capture<Callable<List<String>>> capture = new Capture<Callable<List<String>>>();
    expect(executor.submit(capture(capture))).andReturn(future);
    expect(future.get(SIGNAL_TIMEOUT.as(Time.MILLISECONDS), TimeUnit.MILLISECONDS))
        .andThrow(new TimeoutException());

    control.replay();

    signaler.apply(TEST_URL);
  }

  @Test(expected = SignalException.class)
  public void testExecutionException() throws Exception {
    Capture<Callable<List<String>>> capture = new Capture<Callable<List<String>>>();
    expect(executor.submit(capture(capture))).andReturn(future);
    expect(future.get(SIGNAL_TIMEOUT.as(Time.MILLISECONDS), TimeUnit.MILLISECONDS))
        .andThrow(new ExecutionException(new Exception()));

    control.replay();

    signaler.apply(TEST_URL);
  }

  @Test(expected = SignalException.class)
  public void testInterruptedException() throws Exception {
    Capture<Callable<List<String>>> capture = new Capture<Callable<List<String>>>();
    expect(executor.submit(capture(capture))).andReturn(future);
    expect(future.get(SIGNAL_TIMEOUT.as(Time.MILLISECONDS), TimeUnit.MILLISECONDS))
        .andThrow(new InterruptedException());

    control.replay();

    signaler.apply(TEST_URL);
  }
}

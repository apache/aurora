package com.twitter.mesos.executor;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Resources;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.mesos.executor.HttpSignaler.SignalException;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

/**
 * Handles HTTP signaling to a process.
 *
 * @author wfarner
 */
public class HttpSignaler implements ExceptionalFunction<String, List<String>, SignalException> {
  private static final Logger LOG = Logger.getLogger(HttpSignaler.class.getName());

  private final ExecutorService executor;
  private final Amount<Long, Time> signalTimeout;

  public HttpSignaler(ExecutorService executor, Amount<Long, Time> signalTimeout) {
    this.executor = Preconditions.checkNotNull(executor);
    this.signalTimeout = Preconditions.checkNotNull(signalTimeout);
  }

  @Override
  public List<String> apply(String url) throws SignalException {
    MorePreconditions.checkNotBlank(url);

    final URL signalUrl;
    try {
      signalUrl = new URL(url);
    } catch (MalformedURLException e) {
      throw new SignalException("Malformed URL " + url, e);
    }

    LOG.info("Signaling URL: " + signalUrl);
    Future<List<String>> task = executor.submit(new Callable<List<String>>() {
      @Override public List<String> call() throws Exception {
        return Resources.readLines(signalUrl, Charsets.UTF_8);
      }
    });

    try {
      return task.get(signalTimeout.as(Time.MILLISECONDS), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new SignalException("Interrupted while requesting signal URL " + url, e);
    } catch (ExecutionException e) {
      throw new SignalException("Failed to signal url " + url + ", " + e.getMessage());
    } catch (TimeoutException e) {
      throw new SignalException("Signal request timed out: " + url, e);
    }
  }

  public static class SignalException extends Exception {
    public SignalException(String msg) {
      super(msg);
    }
    public SignalException(String msg, Throwable t) {
      super(msg, t);
    }
  }
}

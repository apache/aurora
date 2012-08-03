package com.twitter.mesos.executor;

import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.common.io.CharStreams;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

/**
 * Handles HTTP signaling to a process.
 */
public interface HttpSignaler {

  enum Method {
    GET("GET"),
    POST("POST");

    private final String value;

    private Method(String value) {
      this.value = value;
    }
  }

  /**
   * Sends an HTTP request to the provided URL.
   *
   * @param method HTTP method to send in the request.
   * @param url Full URL to call.
   * @return The raw HTTP response body.
   * @throws SignalException If the URL could not be signaled, or timed out.
   */
  String signal(Method method, String url) throws SignalException;

  public static class HttpSignalerImpl implements HttpSignaler {
    private static final Logger LOG = Logger.getLogger(HttpSignaler.class.getName());

    private final ExecutorService executor;
    private final Amount<Long, Time> signalTimeout;

    public HttpSignalerImpl(ExecutorService executor, Amount<Long, Time> signalTimeout) {
      this.executor = Preconditions.checkNotNull(executor);
      this.signalTimeout = Preconditions.checkNotNull(signalTimeout);
    }

    @Override public String signal(final Method method, String url) throws SignalException {
      Preconditions.checkNotNull(method);
      Preconditions.checkNotNull(url);

      final URL signalUrl;
      try {
        signalUrl = new URL(url);
      } catch (MalformedURLException e) {
        throw new SignalException("Malformed URL " + url, e);
      }

      LOG.info("Signaling URL: " + signalUrl);
      Future<String> task = executor.submit(new Callable<String>() {
        @Override public String call() throws Exception {
          HttpURLConnection conn = (HttpURLConnection) signalUrl.openConnection();
          try {
            conn.setRequestMethod(method.value);
            return CharStreams.toString(new InputStreamReader(conn.getInputStream()));
          } finally {
            conn.disconnect();
          }
        }
      });

      try {
        long startNanos = System.nanoTime();
        String result = task.get(signalTimeout.as(Time.MILLISECONDS), TimeUnit.MILLISECONDS);
        LOG.fine(signalUrl + " responded in " + (System.nanoTime() - startNanos) + " ns.");
        return result;
      } catch (InterruptedException e) {
        throw new SignalException("Interrupted while requesting signal URL " + url, e);
      } catch (ExecutionException e) {
        throw new SignalException("Failed to signal url " + url + ", " + e.getMessage());
      } catch (TimeoutException e) {
        throw new SignalException("Signal request timed out: " + url, e);
      }
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

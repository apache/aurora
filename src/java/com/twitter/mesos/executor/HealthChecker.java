package com.twitter.mesos.executor;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.mesos.executor.HealthChecker.HealthCheckException;
import com.twitter.mesos.executor.HttpSignaler.SignalException;

import java.util.List;

/**
 * Function that checks the health of a process via HTTP signaling.
 *
 * @author wfarner
 */
public class HealthChecker implements ExceptionalFunction<Integer, Boolean, HealthCheckException> {

  private static final String HEALTH_CHECK_ENDPOINT = "healthz";
  private static final String URL_FORMAT = "http://localhost:%d/" + HEALTH_CHECK_ENDPOINT;

  private static final String HEALTH_CHECK_OK_VALUE = "ok";

  private final ExceptionalFunction<String, List<String>, SignalException> httpSignaler;

  @Inject
  public HealthChecker(ExceptionalFunction<String, List<String>, SignalException> httpSignaler) {
    this.httpSignaler = Preconditions.checkNotNull(httpSignaler);
  }

  @Override
  public Boolean apply(Integer healthCheckPort) throws HealthCheckException {
    try {
      List<String> lines = httpSignaler.apply(String.format(URL_FORMAT, healthCheckPort));
      return HEALTH_CHECK_OK_VALUE.equalsIgnoreCase(Joiner.on("").join(lines).trim());
    } catch (HttpSignaler.SignalException e) {
      throw new HealthCheckException("Failed to check health.", e);
    }
  }

  public static class HealthCheckException extends Exception {
    public HealthCheckException(String msg) {
      super(msg);
    }

    public HealthCheckException(String msg, Throwable t) {
      super(msg, t);
    }
  }
}

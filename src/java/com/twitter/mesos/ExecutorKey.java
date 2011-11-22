package com.twitter.mesos;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.mesos.Protos.ExecutorID;

import com.twitter.common.base.MorePreconditions;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Unique identifier for an executor.
 *
 * @author William Farner
 */
public class ExecutorKey {
  public static final String THERMOS_EXECUTOR_ID_PREFIX = "thermos-";

  public final ExecutorID executor;
  public final String hostname;

  /**
   * Creates a new executor key.
   *
   * @param executor ID of the executor.
   * @param hostname Hostname where the executor is running.
   */
  public ExecutorKey(ExecutorID executor, String hostname) {
    this.executor = checkNotNull(executor);
    this.hostname = MorePreconditions.checkNotBlank(hostname);
  }

  /**
   * Returns the http port for the executor.
   *
   * <p>NOTE: this is used by {@code executors.st}
   *
   * @return http port
   */
  public int getExecutorHttpPort() {
    return executor.getValue().startsWith(THERMOS_EXECUTOR_ID_PREFIX) ? 1338 : 1337;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ExecutorKey)) {
      return false;
    }

    ExecutorKey other = (ExecutorKey) o;
    return new EqualsBuilder()
        .append(this.executor, other.executor)
        .append(this.hostname, other.hostname)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(executor)
        .append(hostname)
        .toHashCode();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("executor", executor)
        .append("hostname", hostname)
        .toString();
  }
}

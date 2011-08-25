package com.twitter.mesos;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.SlaveID;

import com.twitter.common.base.MorePreconditions;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Unique identifier for an executor.
 *
 * @author William Farner
 */
public class ExecutorKey {

  public final SlaveID slave;
  public final ExecutorID executor;
  public final String hostname;

  /**
   * Creates a new executor key.
   *
   * @param slave ID of the slave running the executor.
   * @param executor ID of the executor.
   * @param hostname Hostname where the executor is running.
   */
  public ExecutorKey(SlaveID slave, ExecutorID executor, String hostname) {
    this.slave = checkNotNull(slave);
    this.executor = checkNotNull(executor);
    this.hostname = MorePreconditions.checkNotBlank(hostname);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ExecutorKey)) {
      return false;
    }

    ExecutorKey other = (ExecutorKey) o;
    return new EqualsBuilder()
        .append(this.slave, other.slave)
        .append(this.executor, other.executor)
        .append(this.hostname, other.hostname)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(slave)
        .append(executor)
        .append(hostname)
        .toHashCode();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("slave", slave)
        .append("executor", executor)
        .append("hostname", hostname)
        .toString();

  }
}

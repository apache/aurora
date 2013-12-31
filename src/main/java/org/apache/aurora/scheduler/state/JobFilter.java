package com.twitter.aurora.scheduler.state;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import com.twitter.aurora.scheduler.storage.entities.IJobConfiguration;
import com.twitter.aurora.scheduler.storage.entities.ITaskConfig;

/**
 * An action that either accepts a configuration or rejects it with a reason.
 */
public interface JobFilter {
  /**
   * Accept the JobConfiguration or reject it with a reason.
   *
   * @param jobConfiguration The job configuration to filter.
   * @return A result and the reason the result was reached.
   */
  JobFilterResult filter(IJobConfiguration jobConfiguration);

  /**
   * Accept the TaskConfig with the specified number of instances
   * or reject it with a reason.
   *
   * @param template The task configuration to filter.
   * @param instanceCount Number of instances to apply taskConfig to.
   * @return A result and the reason the result was reached.
   */
  JobFilterResult filter(ITaskConfig template, int instanceCount);

  /**
   * An indication of whether a job passed a filter or not.
   */
  public static final class JobFilterResult {
    private final boolean pass;
    private final String reason;

    private JobFilterResult(boolean pass, String reason) {
      this.pass = pass;
      this.reason = Preconditions.checkNotNull(reason);
    }

    /**
     * Create a new result indicating the job has passed the filter.
     *
     * @return A result indicating the job has passed with a default reason.
     * @see #fail(String)
     */
    public static JobFilterResult pass() {
      return new JobFilterResult(true, "Accepted by filter.");
    }

    /**
     * Create a new result indicating the job has passed the filter.
     *
     * @param reason A reason that the job has passed.
     * @return A result indicating the job has passed because of the given reason.
     * @throws NullPointerException if reason is {@code null}.
     * @see #fail(String)
     */
    public static JobFilterResult pass(String reason) {
      return new JobFilterResult(true, reason);
    }

    /**
     * Create a new result indicating the job has failed the filter.
     *
     * @param reason A reason that the job has failed.
     * @return A result indicating the job has failed because of the given reason.
     * @throws NullPointerException if {@code reason} is {@code null}.
     * @see #pass()
     * @see #pass(String)
     */
    public static JobFilterResult fail(String reason) {
      return new JobFilterResult(false, reason);
    }

    /**
     * Indicates whether the result indicates a pass.
     *
     * @return {@code true} if the result indicates a pass.
     */
    public boolean isPass() {
      return pass;
    }

    /**
     * Indicates the reason for the result.
     *
     * @return The reason for the result.
     */
    public String getReason() {
      return reason;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof JobFilterResult)) {
        return false;
      }
      JobFilterResult that = (JobFilterResult) o;
      return Objects.equal(pass, that.pass)
          && Objects.equal(reason, that.reason);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(pass, reason);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("pass", pass)
          .add("reason", reason)
          .toString();
    }
  }
}

package com.twitter.aurora.scheduler.filter;

import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;

import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.scheduler.configuration.Resources;

/**
 * Determines whether a proposed scheduling assignment should be allowed.
 */
public interface SchedulingFilter {

  /**
   * Reason for a proposed scheduling assignment to be filtered out.
   * A veto also contains a score, which is an opaque indicator as to how strong a veto is.  This
   * is only intended to be used for relative ranking of vetoes for determining which veto against
   * a scheduling assignment is 'weakest'.
   */
  public static class Veto {
    public static final int MAX_SCORE = 1000;

    private final String reason;
    private final int score;
    private final boolean valueMismatch;

    private Veto(String reason, int score, boolean valueMismatch) {
      this.reason = reason;
      this.score = Math.min(MAX_SCORE, score);
      this.valueMismatch = valueMismatch;
    }

    @VisibleForTesting
    public Veto(String reason, int score) {
      this(reason, score, false);
    }

    /**
     * Creates a special veto that represents a mismatch between the server and task's configuration
     * for an attribute.
     *
     * @param reason Information about the value mismatch.
     * @return A constraint mismatch veto.
     */
    public static Veto constraintMismatch(String reason) {
      return new Veto(reason, MAX_SCORE, true);
    }

    public String getReason() {
      return reason;
    }

    public int getScore() {
      return score;
    }

    public boolean isConstraintMismatch() {
      return valueMismatch;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Veto)) {
        return false;
      }

      Veto other = (Veto) o;
      return Objects.equal(reason, other.reason)
          && (score == other.score)
          && (valueMismatch == other.valueMismatch);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(reason, score, valueMismatch);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("reason", reason)
          .add("score", score)
          .add("valueMismatch", valueMismatch)
          .toString();
    }
  }

  /**
   * Applies a task against the filter with the given resources, and on the host.
   *
   * @param offer Resources offered.
   * @param slaveHost Host that the resources are associated with.
   * @param task Task.
   * @param taskId Canonical ID of the task.
   * @return A set of vetoes indicating reasons the task cannot be scheduled.  If the task may be
   *    scheduled, the set will be empty.
   */
  Set<Veto> filter(Resources offer, String slaveHost, TaskConfig task, String taskId);
}

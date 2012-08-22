package com.twitter.mesos.scheduler;

import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;

import com.twitter.mesos.gen.TwitterTaskInfo;

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

    @VisibleForTesting
    public Veto(String reason, int score) {
      this.reason = reason;
      this.score = Math.min(MAX_SCORE, score);
    }

    public String getReason() {
      return reason;
    }

    public int getScore() {
      return score;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Veto)) {
        return false;
      }

      Veto other = (Veto) o;
      return Objects.equal(reason, other.reason)
          && (score == other.score);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(reason, score);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("reason", reason)
          .add("score", score)
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
  Set<Veto> filter(Resources offer, String slaveHost, TwitterTaskInfo task, String taskId);
}

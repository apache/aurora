package com.twitter.mesos.scheduler;

import java.util.Set;

import com.google.common.base.Objects;

import com.twitter.mesos.gen.TwitterTaskInfo;

/**
 * Determines whether a proposed scheduling assignment should be allowed.
 */
public interface SchedulingFilter {

  /**
   * Reason for a proposed scheduling assignment to be filtered out.
   */
  public static class Veto {
    private final String reason;

    Veto(String reason) {
      this.reason = reason;
    }

    public String getReason() {
      return reason;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Veto)) {
        return false;
      }

      Veto other = (Veto) o;
      return reason.equals(other.getReason());
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(reason);
    }

    @Override
    public String toString() {
      return "Veto<" + reason + ">";
    }
  }

  /**
   * Applies a task against the filter with the given resources, and on the host.
   *
   * @param resourceOffer Resources offered.
   * @param slaveHost Host that the resources are associated with.
   * @param task Task.
   * @return A set of vetoes indicating reasons the task cannot be scheduled.  If the task may be
   *    scheduled, the set will be empty.
   */
  Set<Veto> filter(Resources resourceOffer, String slaveHost, TwitterTaskInfo task);
}

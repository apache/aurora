package com.twitter.mesos.scheduler;

import java.util.Set;

import com.google.common.base.Optional;

import com.twitter.mesos.gen.TwitterTaskInfo;

/**
 * Determines whether a proposed scheduling assignment should be allowed.
 *
 * @author William Farner
 */
public interface SchedulingFilter {

  /**
   * Reason for a proposed scheduling assignment to be filtered out.
   *
   * @author William Farner
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
    public String toString() {
      return "Veto<" + reason + ">";
    }
  }

  Set<Veto> filter(Resources resourceOffer, Optional<String> slaveHost, TwitterTaskInfo task);
}
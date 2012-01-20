package com.twitter.mesos.scheduler;

import javax.annotation.Nullable;

import com.google.common.base.Predicate;

import com.twitter.mesos.gen.TwitterTaskInfo;

/**
 * Determines whether a proposed scheduling assignment should be allowed.
 *
 * @author William Farner
 */
public interface SchedulingFilter {

  /**
   * Creates a filter that checks static state to ensure that task configurations meet a
   * resource offer for a given slave.  The returned filter will not check the configuration against
   * any dynamic state.
   *
   * @param resourceOffer The resource offer to check against tasks.
   * @param slaveHost The slave host that the resource offer is associated with, or {@code null} if
   * the machine restrictions should be ignored.
   * @return A new predicate that can be used to find tasks satisfied by the offer.
   */
  Predicate<TwitterTaskInfo> staticFilter(Resources resourceOffer, @Nullable String slaveHost);

  /**
   * Creates a filter that uses the task constraints described in the config to make a scheduling
   * decision based on the resources from other slave hosts.
   *
   * @param slaveHost The slave host that the resource offer is associated with.
   * @return A new predicate that can be used to find tasks satisfied by the offer.
   */
  Predicate<TwitterTaskInfo> dynamicFilter(final String slaveHost);
}

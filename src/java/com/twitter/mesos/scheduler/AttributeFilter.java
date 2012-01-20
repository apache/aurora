package com.twitter.mesos.scheduler;

import java.util.Collection;

import javax.annotation.Nullable;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.Attribute;
import com.twitter.mesos.gen.LimitConstraint;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.ValueConstraint;
import com.twitter.mesos.scheduler.SchedulingFilterImpl.AttributeLoader;

/**
 * Utility class that matches attributes to constraints.
 *
 * @author William Farner
 */
class AttributeFilter {

  private AttributeFilter() {
    // Utility class.
  }

  /**
   * Tests whether an attribute matches a constraint.
   *
   * @param attribute  An optional attribute.
   * @param constraint Constraint to match.
   * @return {@code true} if the attribute satisfies the constraint, {@code false} otherwise.
   */
  static boolean matches(Optional<Attribute> attribute, ValueConstraint constraint) {
    // There is no attribute matching the constraint.
    if (!attribute.isPresent()) {
      return constraint.isNegated();
    }

    boolean match =
        !Sets.intersection(attribute.get().getValues(), constraint.getValues()).isEmpty();
    return constraint.isNegated() ^ match;
  }

  /**
   * Tests whether an attribute matches a limit constraint.
   *
   * @param attribute Attribute to match.
   * @param jobKey Key of the job with the limited constraint.
   * @param limit Limit value.
   * @param activeTasks All active tasks in the system.
   * @param attributeFetcher Interface for fetching attributes for hosts in the system.
   * @return {@code true} if the limit constraint is satisfied, {@code false} otherwise.
   */
  static boolean matches(final Attribute attribute,
      final String jobKey,
      int limit,
      Collection<ScheduledTask> activeTasks,
      final AttributeLoader attributeFetcher) {

    Predicate<ScheduledTask> sameJob =
        Predicates.compose(Predicates.equalTo(jobKey), Tasks.SCHEDULED_TO_JOB_KEY);

    Predicate<ScheduledTask> hasAttribute = new Predicate<ScheduledTask>() {
      @Override public boolean apply(ScheduledTask task) {
        Iterable<Attribute> hostAttributes =
            attributeFetcher.apply(task.getAssignedTask().getSlaveHost());
        return Iterables.any(hostAttributes, Predicates.equalTo(attribute));
      }
    };

    return limit > Iterables.size(
        Iterables.filter(activeTasks, Predicates.and(sameJob, hasAttribute)));
  }
}

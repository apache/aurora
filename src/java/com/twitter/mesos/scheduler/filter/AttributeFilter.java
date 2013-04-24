package com.twitter.mesos.scheduler.filter;

import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.Attribute;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.ValueConstraint;
import com.twitter.mesos.scheduler.filter.SchedulingFilterImpl.AttributeLoader;

/**
 * Utility class that matches attributes to constraints.
 */
final class AttributeFilter {

  private static final Function<Attribute, Set<String>> GET_VALUES =
      new Function<Attribute, Set<String>>() {
        @Override public Set<String> apply(Attribute attribute) {
          return attribute.getValues();
        }
      };

  private AttributeFilter() {
    // Utility class.
  }

  /**
   * Tests whether a constraint is satisfied by attributes.
   *
   * @param attributes Host attributes.
   * @param constraint Constraint to match.
   * @return {@code true} if the attribute satisfies the constraint, {@code false} otherwise.
   */
  static boolean matches(Set<Attribute> attributes, ValueConstraint constraint) {
    Set<String> allAttributes =
        ImmutableSet.copyOf(Iterables.concat(Iterables.transform(attributes, GET_VALUES)));
    boolean match = Iterables.any(constraint.getValues(), Predicates.in(allAttributes));
    return constraint.isNegated() ^ match;
  }

  /**
   * Tests whether an attribute matches a limit constraint.
   *
   * @param attributes Attributes to match against.
   * @param jobKey Key of the job with the limited constraint.
   * @param limit Limit value.
   * @param activeTasks All active tasks in the system.
   * @param attributeFetcher Interface for fetching attributes for hosts in the system.
   * @return {@code true} if the limit constraint is satisfied, {@code false} otherwise.
   */
  static boolean matches(final Set<Attribute> attributes,
      final String jobKey,
      int limit,
      Iterable<ScheduledTask> activeTasks,
      final AttributeLoader attributeFetcher) {

    Predicate<ScheduledTask> sameJob =
        Predicates.compose(Predicates.equalTo(jobKey), Tasks.SCHEDULED_TO_JOB_KEY);

    Predicate<ScheduledTask> hasAttribute = new Predicate<ScheduledTask>() {
      @Override public boolean apply(ScheduledTask task) {
        Iterable<Attribute> hostAttributes =
            attributeFetcher.apply(task.getAssignedTask().getSlaveHost());
        return Iterables.any(hostAttributes, Predicates.in(attributes));
      }
    };

    return limit > Iterables.size(
        Iterables.filter(activeTasks, Predicates.and(sameJob, hasAttribute)));
  }
}

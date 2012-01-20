package com.twitter.mesos.scheduler;

import java.util.Collection;
import java.util.logging.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;

import com.twitter.mesos.gen.Attribute;
import com.twitter.mesos.gen.Constraint;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskConstraint;
import com.twitter.mesos.scheduler.SchedulingFilterImpl.AttributeLoader;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;

/**
 * Filter that determines whether a task's constraints are satisfied.
 *
 * @author William Farner
 */
class ConstraintFilter implements Predicate<Constraint> {

  private static final Logger LOG = Logger.getLogger(ConstraintFilter.class.getName());

  private final String jobKey;
  private final Supplier<Collection<ScheduledTask>> activeTasksSupplier;
  private final AttributeLoader attributeLoader;
  private final Iterable<Attribute> hostAttributes;

  /**
   * Creates a new constraint filer for a given job.
   *
   * @param jobKey Key for the job.
   * @param activeTasksSupplier Supplier to fetch active tasks (if necessary).
   * @param attributeLoader Interface to fetch host attributes (if necessary).
   * @param hostAttributes The attributes of the host to test against.
   */
  ConstraintFilter(
      String jobKey,
      Supplier<Collection<ScheduledTask>> activeTasksSupplier,
      AttributeLoader attributeLoader,
      Iterable<Attribute> hostAttributes) {

    this.jobKey = checkNotBlank(jobKey);
    this.activeTasksSupplier = checkNotNull(activeTasksSupplier);
    this.attributeLoader = checkNotNull(attributeLoader);
    this.hostAttributes = checkNotNull(hostAttributes);
  }

  @Override
  public boolean apply(Constraint constraint) {
    Predicate<Attribute> matchName = new NameFilter(constraint.getName());
    Attribute attribute =
        Iterables.getOnlyElement(Iterables.filter(hostAttributes, matchName), null);

    TaskConstraint taskConstraint = constraint.getConstraint();
    switch (taskConstraint.getSetField()) {
      case VALUE:
        return AttributeFilter.matches(Optional.fromNullable(attribute), taskConstraint.getValue());

      case LIMIT:
        if (attribute == null) {
          // The host does not specify an attribute matching the constraint.
          return false;
        }

        return AttributeFilter.matches(
            attribute,
            jobKey,
            taskConstraint.getLimit().getLimit(),
            activeTasksSupplier.get(),
            attributeLoader);

      default:
        LOG.warning("Failed to recognize the constraint type: " + taskConstraint.getSetField());
        throw new SchedulerException("Failed to recognize the constraint type: "
            + taskConstraint.getSetField());
    }
  }

  // Finds all the attributes given by the name.
  private static class NameFilter implements Predicate<Attribute> {
    private final String attributeName;

    NameFilter(String attributeName) {
      this.attributeName = attributeName;
    }

    @Override public boolean apply(Attribute attribute) {
      return attributeName.equals(attribute.getName());
    }
  }
}

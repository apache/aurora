/**
 * Copyright 2013 Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.filter;

import java.util.Set;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.aurora.gen.Attribute;
import org.apache.aurora.scheduler.base.SchedulerException;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.filter.SchedulingFilterImpl.AttributeLoader;
import org.apache.aurora.scheduler.storage.entities.IConstraint;
import org.apache.aurora.scheduler.storage.entities.ITaskConstraint;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Filter that determines whether a task's constraints are satisfied.
 */
class ConstraintFilter implements Function<IConstraint, Optional<Veto>> {

  private static final Logger LOG = Logger.getLogger(ConstraintFilter.class.getName());

  private final CachedJobState cachedjobState;
  private final AttributeLoader attributeLoader;
  private final Iterable<Attribute> hostAttributes;

  /**
   * Creates a new constraint filer for a given job.
   *
   * @param cachedjobState Cached information about the job containing the task being matched.
   * @param attributeLoader Interface to fetch host attributes (if necessary).
   * @param hostAttributes The attributes of the host to test against.
   */
  ConstraintFilter(
      CachedJobState cachedjobState,
      AttributeLoader attributeLoader,
      Iterable<Attribute> hostAttributes) {

    this.cachedjobState = checkNotNull(cachedjobState);
    this.attributeLoader = checkNotNull(attributeLoader);
    this.hostAttributes = checkNotNull(hostAttributes);
  }

  @VisibleForTesting
  static Veto limitVeto(String limit) {
    return new Veto("Limit not satisfied: " + limit, Veto.MAX_SCORE);
  }

  @VisibleForTesting
  static Veto mismatchVeto(String constraint) {
    return Veto.constraintMismatch("Constraint not satisfied: " + constraint);
  }

  @VisibleForTesting
  static Veto maintenanceVeto(String reason) {
    return new Veto("Host " + reason + " for maintenance", Veto.MAX_SCORE);
  }

  @Override
  public Optional<Veto> apply(IConstraint constraint) {
    Set<Attribute> attributes =
        ImmutableSet.copyOf(Iterables.filter(hostAttributes, new NameFilter(constraint.getName())));

    ITaskConstraint taskConstraint = constraint.getConstraint();
    switch (taskConstraint.getSetField()) {
      case VALUE:
        boolean matches =
            AttributeFilter.matches(attributes, taskConstraint.getValue());
        return matches
            ? Optional.<Veto>absent()
            : Optional.of(mismatchVeto(constraint.getName()));

      case LIMIT:
        if (attributes.isEmpty()) {
          return Optional.of(mismatchVeto(constraint.getName()));
        }

        boolean satisfied = AttributeFilter.matches(
            attributes,
            taskConstraint.getLimit().getLimit(),
            cachedjobState.getActiveTasks(),
            attributeLoader);
        return satisfied
            ? Optional.<Veto>absent()
            : Optional.of(limitVeto(constraint.getName()));

      default:
        LOG.warning("Unrecognized constraint type: " + taskConstraint.getSetField());
        throw new SchedulerException("Failed to recognize the constraint type: "
            + taskConstraint.getSetField());
    }
  }

  /**
   * A filter to find attributes matching a name.
   */
  static class NameFilter implements Predicate<Attribute> {
    private final String attributeName;

    NameFilter(String attributeName) {
      this.attributeName = attributeName;
    }

    @Override
    public boolean apply(Attribute attribute) {
      return attributeName.equals(attribute.getName());
    }
  }
}

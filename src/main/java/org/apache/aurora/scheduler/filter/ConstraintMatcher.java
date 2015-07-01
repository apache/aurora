/**
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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.aurora.gen.Attribute;
import org.apache.aurora.scheduler.base.SchedulerException;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.storage.entities.IAttribute;
import org.apache.aurora.scheduler.storage.entities.IConstraint;
import org.apache.aurora.scheduler.storage.entities.ITaskConstraint;

/**
 * Filter that determines whether a task's constraints are satisfied.
 */
final class ConstraintMatcher {
  private ConstraintMatcher() {
    // Utility class.
  }

  private static final Function<IAttribute, Set<String>> GET_VALUES =
      new Function<IAttribute, Set<String>>() {
        @Override
        public Set<String> apply(IAttribute attribute) {
          return attribute.getValues();
        }
      };

  /**
   * Gets the veto (if any) for a scheduling constraint based on the {@link AttributeAggregate} this
   * filter was created with.
   *
   * @param constraint Scheduling filter to check.
   * @return A veto if the constraint is not satisfied based on the existing state of the job.
   */
  static Optional<Veto> getVeto(
      AttributeAggregate cachedjobState,
      Iterable<IAttribute> hostAttributes,
      IConstraint constraint) {

    Iterable<IAttribute> sameNameAttributes =
        Iterables.filter(hostAttributes, new NameFilter(constraint.getName()));
    Optional<IAttribute> attribute;
    if (Iterables.isEmpty(sameNameAttributes)) {
      attribute = Optional.absent();
    } else {
      Set<String> attributeValues = ImmutableSet.copyOf(
          Iterables.concat(Iterables.transform(sameNameAttributes, GET_VALUES)));
      attribute =
          Optional.of(IAttribute.build(new Attribute(constraint.getName(), attributeValues)));
    }

    ITaskConstraint taskConstraint = constraint.getConstraint();
    switch (taskConstraint.getSetField()) {
      case VALUE:
        boolean matches = AttributeFilter.matches(
            attribute.transform(GET_VALUES).or(ImmutableSet.of()),
            taskConstraint.getValue());
        return matches
            ? Optional.absent()
            : Optional.of(Veto.constraintMismatch(constraint.getName()));

      case LIMIT:
        if (!attribute.isPresent()) {
          return Optional.of(Veto.constraintMismatch(constraint.getName()));
        }

        boolean satisfied = AttributeFilter.matches(
            attribute.get(),
            taskConstraint.getLimit().getLimit(),
            cachedjobState);
        return satisfied
            ? Optional.absent()
            : Optional.of(Veto.unsatisfiedLimit(constraint.getName()));

      default:
        throw new SchedulerException("Failed to recognize the constraint type: "
            + taskConstraint.getSetField());
    }
  }

  /**
   * A filter to find attributes matching a name.
   */
  static class NameFilter implements Predicate<IAttribute> {
    private final String attributeName;

    NameFilter(String attributeName) {
      this.attributeName = attributeName;
    }

    @Override
    public boolean apply(IAttribute attribute) {
      return attributeName.equals(attribute.getName());
    }
  }
}

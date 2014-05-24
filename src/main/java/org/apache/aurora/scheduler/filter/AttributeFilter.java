/**
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

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

import org.apache.aurora.gen.Attribute;
import org.apache.aurora.scheduler.storage.entities.IValueConstraint;

/**
 * Utility class that matches attributes to constraints.
 */
final class AttributeFilter {

  private AttributeFilter() {
    // Utility class.
  }

  /**
   * Tests whether a constraint is satisfied by attributes.
   *
   * @param values Host attribute values.
   * @param constraint Constraint to match.
   * @return {@code true} if the attribute satisfies the constraint, {@code false} otherwise.
   */
  static boolean matches(Set<String> values, IValueConstraint constraint) {
    boolean match = Iterables.any(constraint.getValues(), Predicates.in(values));
    return constraint.isNegated() ^ match;
  }

  /**
   * Tests whether an attribute matches a limit constraint.
   *
   * @param attribute Attribute to match against.
   * @param limit Limit value.
   * @param attributeAggregate Cached state of the job being filtered.
   * @return {@code true} if the limit constraint is satisfied, {@code false} otherwise.
   */
  static boolean matches(final Attribute attribute,
      int limit,
      AttributeAggregate attributeAggregate) {

    for (String value : attribute.getValues()) {
      if (limit <= attributeAggregate.getNumTasksWithAttribute(attribute.getName(), value)) {
        return false;
      }
    }

    return true;
  }
}

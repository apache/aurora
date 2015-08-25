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
package com.twitter.common.testing.easymock;

import java.util.Collection;
import java.util.List;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;

import org.easymock.IArgumentMatcher;

import static org.easymock.EasyMock.reportMatcher;

/**
 * This EasyMock argument matcher tests Iterables for equality irrespective of order.
 *
 * @param <T> type argument for the Iterables being matched.
 */
public class IterableEquals<T> implements IArgumentMatcher {
  private final Multiset<T> elements = HashMultiset.create();

  /**
   * Constructs an IterableEquals object that tests for equality against the specified expected
   * Iterable.
   *
   * @param expected an Iterable containing the elements that are expected, in any order.
   */
  public IterableEquals(Iterable<T> expected) {
    Iterables.addAll(elements, expected);
  }

  @Override
  public boolean matches(Object observed) {
    if (observed instanceof Iterable<?>) {
      Multiset<Object> observedElements = HashMultiset.create((Iterable<?>) observed);
      return elements.equals(observedElements);
    }
    return false;
  }

  @Override
  public void appendTo(StringBuffer buffer) {
    buffer.append("eqIterable(").append(elements).append(")");
  }

  /**
   * When used in EasyMock expectations, this matches an Iterable having the same elements in any
   * order.
   *
   * @return null, to avoid a compile time error.
   */
  public static <T> Iterable<T> eqIterable(Iterable<T> in) {
    reportMatcher(new IterableEquals(in));
    return null;
  }

  /**
   * When used in EasyMock expectations, this matches a List having the same elements in any order.
   *
   * @return null, to avoid a compile time error.
   */
  public static <T> List<T> eqList(Iterable<T> in) {
    reportMatcher(new IterableEquals(in));
    return null;
  }

  /**
   * When used in EasyMock expectations, this matches a Collection having the same elements in any
   * order.
   *
   * @return null, to avoid a compile time error.
   */
  public static <T> Collection<T> eqCollection(Iterable<T> in) {
    reportMatcher(new IterableEquals(in));
    return null;
  }
}

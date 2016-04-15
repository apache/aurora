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
package org.apache.aurora.common.base;

import java.util.List;
import java.util.function.Consumer;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Utilities for dealing with Consumers.
 *
 * @author John Sirois
 */
public final class Consumers {

  private static final Consumer<?> NOOP = item -> {
    // noop
  };

  private Consumers() {
    // utility
  }

  /**
   * Combines multiple consumers into a single consumer, whose calls are replicated sequentially
   * in the order that they were provided.
   * If an exception is encountered from a consumer it propagates to the top-level consumer and the
   * remaining consumer are not executed.
   *
   * @param consumers Consumers to combine.
   * @param <T> Type accepted by the consumers.
   * @return A single consumer that will fan out all calls to {@link Consumer#accept(Object)} to
   *    the wrapped consumers.
   */
  public static <T> Consumer<T> combine(List<Consumer<T>> consumers) {
    checkNotNull(consumers);
    checkArgument(Iterables.all(consumers, Predicates.notNull()));

    return consumers.stream().reduce(noop(), Consumer::andThen);
  }

  /**
   * Applies a filter to a consumer, such that the consumer will only be called when the filter is
   * satisfied (returns {@code true}}.
   *
   * @param filter Filter to determine when {@code consumer} is called.
   * @param consumer Consumer to filter.
   * @param <T> Type handled by the filter and the consumer.
   * @return A filtered consumer.
   */
  public static <T> Consumer<T> filter(final Predicate<T> filter, final Consumer<T> consumer) {
    checkNotNull(filter);
    checkNotNull(consumer);

    return item -> {
      if (filter.apply(item)) {
        consumer.accept(item);
      }
    };
  }

  /**
   * Returns a consumer that will do nothing.
   *
   * @param <T> The consumer argument type.
   * @return A consumer that does nothing.
   */
  @SuppressWarnings("unchecked")
  public static <T> Consumer<T> noop() {
    return (Consumer<T>) NOOP;
  }
}

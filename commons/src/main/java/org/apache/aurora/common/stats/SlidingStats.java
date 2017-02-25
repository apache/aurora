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
package org.apache.aurora.common.stats;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.aurora.common.base.MorePreconditions;
import org.apache.aurora.common.util.Clock;

import static java.util.Objects.requireNonNull;

/**
 * Tracks event statistics over a sliding window of time. An event is something that has a
 * frequency and associated total.
 *
 * @author William Farner
 */
public class SlidingStats {

  /**
   * An abstraction for an action to be timed by SlidingStats.
   *
   * @param <V> The result of the successfully completed action.
   * @param <E> The exception type that the action might throw.
   */
  @FunctionalInterface
  public interface Timeable<V, E extends Exception> {

    /**
     * A convenient typedef for action that throws no checked exceptions - it runs quietly.
     *
     * @param <V> The result of the successfully completed action.
     */
    @FunctionalInterface
    interface Quiet<V> extends Timeable<V, RuntimeException> {
      // empty
    }

    /**
     * Encapsulates an action with no result.
     *
     * @param <E> The exception type that the action might throw.
     */
    @FunctionalInterface
    interface NoResult<E extends Exception> extends Timeable<Void, E> {
      @Override
      default Void invoke() throws E {
        execute();
        return null;
      }

      /**
       * Similar to {@link Timeable#invoke()} except no result is returned.
       *
       * @throws E If action fails.
       */
      void execute() throws E;

      /**
       * A convenient typedef for action with no result that throws no checked exceptions - it runs
       * quietly.
       */
      @FunctionalInterface
      interface Quiet extends NoResult<RuntimeException> {
        // empty
      }
    }

    /**
     * Abstracts an action that has a result, but may also throw a specific exception.
     *
     * @return The result of the successfully completed action.
     * @throws E If action fails.
     */
    V invoke() throws E;
  }

  private static final int DEFAULT_WINDOW_SIZE = 1;

  private final AtomicLong total;
  private final AtomicLong events;
  private final Stat<Double> perEventLatency;
  private final Clock clock;

  /**
   * Creates a new sliding statistic with the given name
   *
   * @param name Name for this stat collection.
   * @param totalUnitDisplay String to display for the total counter unit.
   */
  public SlidingStats(String name, String totalUnitDisplay) {
    this(name, totalUnitDisplay, DEFAULT_WINDOW_SIZE, Clock.SYSTEM_CLOCK);
  }

  /**
   * Creates a new sliding statistic with the given name
   *
   * @param name Name for this stat collection.
   * @param totalUnitDisplay String to display for the total counter unit.
   * @param windowSize The window size for the per second Rate and Ratio stats.
   * @param clock The clock abstraction to use for timing in {@link #time(Timeable)} calls.
   */
  public SlidingStats(String name, String totalUnitDisplay, int windowSize, Clock clock) {
    MorePreconditions.checkNotBlank(name);

    String totalDisplay = name + "_" + totalUnitDisplay + "_total";
    String eventDisplay = name + "_events";
    total = Stats.exportLong(totalDisplay);
    events = Stats.exportLong(eventDisplay);
    perEventLatency = Stats.export(Ratio.of(name + "_" + totalUnitDisplay + "_per_event",
        Rate.of(totalDisplay + "_per_sec", total).withWindowSize(windowSize).build(),
        Rate.of(eventDisplay + "_per_sec", events).withWindowSize(windowSize).build()));
    this.clock = requireNonNull(clock);
  }

  public AtomicLong getTotalCounter() {
    return total;
  }

  public AtomicLong getEventCounter() {
    return events;
  }

  public Stat<Double> getPerEventLatency() {
    return perEventLatency;
  }

  /**
   * Accumulates counter by an offset.  This is is useful for tracking things like
   * latency of operations.
   *
   * TODO(William Farner): Implement a wrapper to SlidingStats that expects to accumulate time, and can
   *    convert between time units.
   *
   * @param value The value to accumulate.
   */
  public void accumulate(long value) {
    total.addAndGet(value);
    events.incrementAndGet();
  }

  /**
   * Accumulates counter by the nanoseconds it takes to execute the supplied action.
   *
   * @param action An action that produces result of type V and may throw exception E.
   * @param <V> The return type of action.
   * @param <E> The exception type that might be thrown by action.
   * @return The value returned by action.
   * @throws E A subclass of {@link Exception} that might be thrown by action.
   */
  public <V, E extends Exception> V time(Timeable<V, E> action) throws E {
    long start = clock.nowNanos();
    try {
      return action.invoke();
    } finally {
      accumulate(clock.nowNanos() - start);
    }
  }

  @Override
  public String toString() {
    return total + " " + events;
  }
}

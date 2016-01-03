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
package org.apache.aurora.common.util;

import com.google.common.base.Preconditions;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;

/**
 * A BackoffStrategy that implements truncated binary exponential backoff.
 */
public class TruncatedBinaryBackoff implements BackoffStrategy {
  private final long initialBackoffMs;
  private final long maxBackoffIntervalMs;
  private final boolean stopAtMax;
  private final Random random;

  /**
   * Creates a new TruncatedBinaryBackoff that will start by backing off for {@code initialBackoff}
   * and then backoff of twice as long each time its called until reaching the {@code maxBackoff} at
   * which point shouldContinue() will return false and any future backoffs will always wait for
   * that amount of time.
   *
   * @param initialBackoff the initial amount of time to backoff
   * @param maxBackoff the maximum amount of time to backoff
   * @param stopAtMax whether shouldContinue() returns false when the max is reached
   * @param random An instance of the random util which can be used for applying jitter to backoff.
   */
  public TruncatedBinaryBackoff(
      Amount<Long, Time> initialBackoff,
      Amount<Long, Time> maxBackoff,
      boolean stopAtMax,
      Random random) {

    Preconditions.checkNotNull(initialBackoff);
    Preconditions.checkNotNull(maxBackoff);
    Preconditions.checkNotNull(random);
    Preconditions.checkArgument(initialBackoff.getValue() > 0);
    Preconditions.checkArgument(maxBackoff.compareTo(initialBackoff) >= 0);
    initialBackoffMs = initialBackoff.as(Time.MILLISECONDS);
    maxBackoffIntervalMs = maxBackoff.as(Time.MILLISECONDS);
    this.stopAtMax = stopAtMax;
    this.random = random;
  }

  /**
   * Same as the main constructor
   * {@link TruncatedBinaryBackoff#TruncatedBinaryBackoff(Amount, Amount, boolean, Random)},
   * but this will use a default Random implementation returned by
   * {@link Random.Util#newDefaultRandom()}.
   *
   * @param initialBackoff the initial amount of time to backoff
   * @param maxBackoff the maximum amount of time to backoff
   * @param stopAtMax whether shouldContinue() returns false when the max is reached
   */
  public TruncatedBinaryBackoff(
      Amount<Long, Time> initialBackoff,
      Amount<Long, Time> maxBackoff,
      boolean stopAtMax) {

    this(initialBackoff, maxBackoff, stopAtMax, Random.Util.newDefaultRandom());
  }

  /**
   * Same as the constructor
   * {@link TruncatedBinaryBackoff#TruncatedBinaryBackoff(Amount, Amount, boolean)},
   * but this will always return true from shouldContinue().
   *
   * @param initialBackoff the initial amount of time to backoff
   * @param maxBackoff the maximum amount of time to backoff
   */
  public TruncatedBinaryBackoff(Amount<Long, Time> initialBackoff, Amount<Long, Time> maxBackoff) {
    this(initialBackoff, maxBackoff, false);
  }

  @Override
  public long calculateBackoffMs(long lastBackoffMs) {
    Preconditions.checkArgument(lastBackoffMs >= 0);
    long halfBackoff = (lastBackoffMs == 0) ? initialBackoffMs : lastBackoffMs;

    return Math.min(
        maxBackoffIntervalMs,
        halfBackoff + Math.round(random.nextDouble() * halfBackoff));
  }

  @Override
  public boolean shouldContinue(long lastBackoffMs) {
    Preconditions.checkArgument(lastBackoffMs >= 0);
    boolean stop = stopAtMax && (lastBackoffMs >= maxBackoffIntervalMs);

    return !stop;
  }
}

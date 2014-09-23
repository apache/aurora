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
package org.apache.aurora.scheduler.async;

import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.Random;

import org.apache.aurora.scheduler.async.OfferQueue.OfferReturnDelay;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Returns offers after a random duration within a fixed window.
 */
@VisibleForTesting
class RandomJitterReturnDelay implements OfferReturnDelay {
  private final int minHoldTimeMs;
  private final int maxJitterWindowMs;
  private final Random random;

  RandomJitterReturnDelay(int minHoldTimeMs, int maxJitterWindowMs, Random random) {
    checkArgument(minHoldTimeMs >= 0);
    checkArgument(maxJitterWindowMs >= 0);

    this.minHoldTimeMs = minHoldTimeMs;
    this.maxJitterWindowMs = maxJitterWindowMs;
    this.random = Objects.requireNonNull(random);
  }

  @Override
  public Amount<Integer, Time> get() {
    return Amount.of(minHoldTimeMs + random.nextInt(maxJitterWindowMs), Time.MILLISECONDS);
  }
}

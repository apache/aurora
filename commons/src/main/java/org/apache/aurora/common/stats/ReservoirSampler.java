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

import java.util.Vector;

import com.google.common.base.Preconditions;

import org.apache.aurora.common.util.Random;

/**
 * An in memory implementation of Reservoir Sampling for sampling from
 * a population.
 * <p>Several optimizations can be done.
 * Especially, one can avoid rolling the dice as many times as the
 * size of the population with an involved trick.
 * See "Random Sampling with a Reservoir", Vitter, 1985</p>
 * <p>TODO (delip): Fix this when the problem arises</p>
 *
 * @param <T> Type of the sample
 * @author Delip Rao
 */
public class ReservoirSampler<T> {
  private final Vector<T> reservoir = new Vector<T>();
  private final int numSamples;

  private final Random random;
  private int numItemsSeen = 0;

  /**
   * Create a new sampler with a certain reservoir size using
   * a supplied random number generator.
   *
   * @param numSamples Maximum number of samples to
   *                   retain in the reservoir. Must be non-negative.
   * @param random Instance of the random number generator
   *               to use for sampling
   */
  public ReservoirSampler(int numSamples, Random random) {
    Preconditions.checkArgument(numSamples > 0,
        "numSamples should be positive");
    Preconditions.checkNotNull(random);
    this.numSamples = numSamples;
    this.random = random;
  }

  /**
   * Create a new sampler with a certain reservoir size using
   * the default random number generator.
   *
   * @param numSamples Maximum number of samples to
   *        retain in the reservoir. Must be non-negative.
   */
  public ReservoirSampler(int numSamples) {
    this(numSamples, Random.Util.newDefaultRandom());
  }

  /**
   * Sample an item and store in the reservoir if needed.
   *
   * @param item The item to sample - may not be null.
   */
  public void sample(T item) {
    Preconditions.checkNotNull(item);
    if (reservoir.size() < numSamples) {
      // reservoir not yet full, just append
      reservoir.add(item);
    } else {
      // find a sample to replace
      int rIndex = random.nextInt(numItemsSeen + 1);
      if (rIndex < numSamples) {
        reservoir.set(rIndex, item);
      }
    }
    numItemsSeen++;
  }

  /**
   * Get samples collected in the reservoir.
   *
   * @return A sequence of the samples. No guarantee is provided on the order of the samples.
   */
  public Iterable<T> getSamples() {
    return reservoir;
  }
}
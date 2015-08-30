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

/**
 * An interface to define the common functionality that is required for generating random values.
 *
 * @author William Farner
 */
public interface Random {

  /**
   * @see java.util.Random#nextDouble()
   */
  double nextDouble();

  /**
   * @see java.util.Random#nextInt(int)
   */
  int nextInt(int n);

  /**
   * A Random that wraps a java.util.Random.
   */
  class SystemRandom implements Random {
    private final java.util.Random rand;

    public SystemRandom(java.util.Random rand) {
      this.rand = Preconditions.checkNotNull(rand);
    }

    @Override
    public double nextDouble() {
      return rand.nextDouble();
    }

    @Override
    public int nextInt(int n) {
      return rand.nextInt(n);
    }
  }

  // Utility class.
  class Util {
    private Util() {}

    /**
     * Creates a new Random based off the default system Random.
     * @return A new default Random.
     */
    public static Random newDefaultRandom() {
      return new SystemRandom(new java.util.Random());
    }
  }
}

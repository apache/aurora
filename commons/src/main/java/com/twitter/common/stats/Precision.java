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
package com.twitter.common.stats;

import com.google.common.base.Preconditions;

/**
 * Precision expresses the maximum epsilon tolerated for a typical size of input
 * e.g.: Precision(0.01, 1000) express that we tolerate a error of 1% for 1000 entries
 *       it means that max difference between the real quantile and the estimate one is
 *       error = 0.01*1000 = 10
 *       For an entry like (1 to 1000), q(0.5) will be [490 <= x <= 510] (real q(0.5) = 500)
 */
public class Precision {
  private final double epsilon;
  private final int n;

  /**
   * Create a Precision instance representing a precision per number of entries
   *
   * @param epsilon is the maximum error tolerated
   * @param n size of the data set
   */
  public Precision(double epsilon, int n) {
    Preconditions.checkArgument(0.0 < epsilon, "Epsilon must be positive!");
    Preconditions.checkArgument(1 < n, "N (expected number of elements) must be greater than 1!");

    this.epsilon = epsilon;
    this.n = n;
  }

  public double getEpsilon() {
    return epsilon;
  }

  public int getN() {
    return n;
  }
}

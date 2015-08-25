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

/**
 * Helper class containing only static methods
 */
public final class Histograms {

  private Histograms() {
    /* Disable */
  }

  /**
   * Helper method that return an array of quantiles
   * @param h the histogram to query
   * @param quantiles an array of double representing the quantiles
   * @return the array of computed quantiles
   */
  public static long[] extractQuantiles(Histogram h, double[] quantiles) {
    long[] results = new long[quantiles.length];
    for (int i = 0; i < results.length; i++) {
      double q = quantiles[i];
      results[i] = h.getQuantile(q);
    }
    return results;
  }
}

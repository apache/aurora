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
package org.apache.aurora.scheduler.sla;

import java.util.List;

import com.google.common.math.Quantiles;

/**
 * Utility methods for the SLA calculations.
 */
final class SlaUtil {

  private SlaUtil() {
    // Utility class.
  }

  /**
   * Reports the percentile value from the continuous distribution described by a given list of
   * samples.
   *
   * Example: [30, 60, 70, 90], the 50 percentile is 65.0 (i.e. larger values cover 50% of the PDF
   * (Probability Density Function)).
   * Example: [30, 60, 70, 90], the 75 percentile is 52.5 (i.e. larger values cover 75% of the PDF).
   * Example: [30, 60, 70, 90], the 90 percentile is 39.0 (i.e. larger values cover 85% of the PDF).
   * Example: [30, 60, 70, 90], the 99 percentile is 30.9 (i.e. larger values cover 95% of the PDF).
   *
   * @param list List to calculate percentile for.
   * @param percentile Percentile value to apply.
   * @return Element at the given percentile.
   */
  static Number percentile(List<Long> list, double percentile) {
    if (list.isEmpty()) {
      return 0.0;
    }

    // index should be a full integer. use quantile scale to allow reporting of percentile values
    // such as p99.9.
    double percentileCopy = percentile;
    int quantileScale = 100;
    while ((percentileCopy - Math.floor(percentileCopy)) > 0) {
      quantileScale *= 10;
      percentileCopy *= 10;
    }

    return Quantiles.scale(quantileScale).index((int) Math.floor(quantileScale - percentileCopy))
        .compute(list);
  }
}

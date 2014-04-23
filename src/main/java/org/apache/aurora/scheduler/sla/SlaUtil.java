/**
 * Copyright 2014 Apache Software Foundation
 *
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

import com.google.common.collect.Ordering;

/**
 * Utility methods for the SLA calculations.
 */
final class SlaUtil {

  private SlaUtil() {
    // Utility class.
  }

  /**
   * Reports the percentile value from the given list ordered in a non-descending order.
   * Example: [30, 60, 70, 90], the 75 percentile is 30 (i.e. 75% of elements are greater).
   *
   * @param list List to calculate percentile for.
   * @param percentile Percentile value to apply.
   * @return Element at the given percentile.
   */
  static Long percentile(List<Long> list, double percentile) {
    if (list.isEmpty()) {
      return 0L;
    }

    List<Long> sorted = Ordering.natural().immutableSortedCopy(list);
    int total = sorted.size();
    int percentileElements = (int) Math.floor(percentile / 100 * total);
    int index = total - percentileElements - 1;
    return index >= 0 && index < total ? sorted.get(index) : 0;
  }
}

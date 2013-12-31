/**
 * Copyright 2013 Apache Software Foundation
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
package org.apache.aurora.scheduler.quota;

import com.google.common.annotations.VisibleForTesting;

import org.apache.aurora.scheduler.storage.entities.IQuota;

/**
 * Calculates and formats detailed quota comparison result.
 */
class QuotaComparisonResult {

  private enum Resource {
    CPU("core(s)"),
    RAM("MB"),
    DISK("MB");

    private final String unit;
    private Resource(String unit) {
      this.unit = unit;
    }
  }

  enum Result {
    SUFFICIENT_QUOTA,
    INSUFFICIENT_QUOTA
  }

  private final String details;
  private final Result result;

  @VisibleForTesting
  QuotaComparisonResult(Result result, String details) {
    this.result = result;
    this.details = details;
  }

  Result result() {
    return result;
  }

  String details() {
    return details;
  }

  static QuotaComparisonResult greaterOrEqual(IQuota a, IQuota b) {
    StringBuilder details = new StringBuilder();
    boolean result = compare(a.getNumCpus(), b.getNumCpus(), Resource.CPU, details)
        & compare(a.getRamMb(), b.getRamMb(), Resource.RAM, details)
        & compare(a.getDiskMb(), b.getDiskMb(), Resource.DISK, details);

    return new QuotaComparisonResult(
        result ? Result.SUFFICIENT_QUOTA : Result.INSUFFICIENT_QUOTA,
        details.toString());
  }

  private static boolean compare(
      double a,
      double b,
      Resource resource,
      StringBuilder details) {

    boolean result = a >= b;
    if (!result) {
      details
          .append(details.length() > 0 ? "; " : "")
          .append(resource)
          .append(" quota exceeded by ")
          .append(String.format("%.2f", b - a))
          .append(" ")
          .append(resource.unit);
    }

    return result;
  }
}

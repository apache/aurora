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
package org.apache.aurora.scheduler.quota;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;

import static java.util.Objects.requireNonNull;

/**
 * Calculates and formats detailed quota comparison result.
 */
public class QuotaCheckResult {

  /**
   * Quota check result.
   */
  public static enum Result {
    /**
     * There is sufficient quota for the requested operation.
     */
    SUFFICIENT_QUOTA,

    /**
     * There is not enough allocated quota for the requested operation.
     */
    INSUFFICIENT_QUOTA
  }

  private static enum Resource {
    CPU("core(s)"),
    RAM("MB"),
    DISK("MB");

    private final String unit;
    private Resource(String unit) {
      this.unit = unit;
    }

    String getUnit() {
      return unit;
    }
  }

  private final Optional<String> details;
  private final Result result;

  @VisibleForTesting
  public QuotaCheckResult(Result result) {
    this(result, Optional.<String>absent());
  }

  private QuotaCheckResult(Result result, Optional<String> details) {
    this.result = requireNonNull(result);
    this.details = requireNonNull(details);
  }

  /**
   * Gets quota check result.
   *
   * @return Quota check result.
   */
  public Result getResult() {
    return result;
  }

  /**
   * Gets detailed quota violation description in case quota check fails.
   *
   * @return Quota check details.
   */
  public Optional<String> getDetails() {
    return details;
  }

  static QuotaCheckResult greaterOrEqual(IResourceAggregate a, IResourceAggregate b) {
    StringBuilder details = new StringBuilder();
    boolean result = compare(a.getNumCpus(), b.getNumCpus(), Resource.CPU, details)
        & compare(a.getRamMb(), b.getRamMb(), Resource.RAM, details)
        & compare(a.getDiskMb(), b.getDiskMb(), Resource.DISK, details);

    return new QuotaCheckResult(
        result ? Result.SUFFICIENT_QUOTA : Result.INSUFFICIENT_QUOTA,
        Optional.of(details.toString()));
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
          .append(resource.getUnit());
    }

    return result;
  }
}

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

import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.resources.ResourceType;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.resources.ResourceBag.IS_NEGATIVE;

/**
 * Calculates and formats detailed quota comparison result.
 */
public class QuotaCheckResult {

  /**
   * Quota check result.
   */
  public enum Result {
    /**
     * There is sufficient quota for the requested operation.
     */
    SUFFICIENT_QUOTA,

    /**
     * There is not enough allocated quota for the requested operation.
     */
    INSUFFICIENT_QUOTA
  }

  private final Optional<String> details;
  private final Result result;

  @VisibleForTesting
  public QuotaCheckResult(Result result) {
    this(result, Optional.absent());
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

  static QuotaCheckResult greaterOrEqual(ResourceBag a, ResourceBag b) {
    StringBuilder details = new StringBuilder();
    ResourceBag difference = a.subtract(b);
    difference.getResourceVectors().entrySet().stream()
        .filter(IS_NEGATIVE)
        .forEach(entry -> addMessage(entry.getKey(), Math.abs(entry.getValue()), details));

    return new QuotaCheckResult(
        details.length() > 0 ? Result.INSUFFICIENT_QUOTA : Result.SUFFICIENT_QUOTA,
        Optional.of(details.toString()));
  }

  private static void addMessage(ResourceType resourceType, Double overage, StringBuilder details) {
    details
        .append(details.length() > 0 ? "; " : "")
        .append(resourceType.getAuroraName())
        .append(" quota exceeded by ")
        .append(String.format("%.2f", overage))
        .append(" ")
        .append(resourceType.getAuroraUnit());
  }
}

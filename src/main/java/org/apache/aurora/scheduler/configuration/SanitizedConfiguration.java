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
package org.apache.aurora.scheduler.configuration;

import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;

import org.apache.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;

/**
 * Wrapper for a configuration that has been fully-sanitized and populated with defaults.
 */
public final class SanitizedConfiguration {

  private final IJobConfiguration sanitized;
  private final Set<Integer> instanceIds;

  /**
   * Constructs a SanitizedConfiguration object and populates the set of instance IDs for
   * the provided {@link org.apache.aurora.scheduler.storage.entities.ITaskConfig}.
   *
   * @param sanitized A sanitized configuration.
   */
  @VisibleForTesting
  public SanitizedConfiguration(IJobConfiguration sanitized) {
    this.sanitized = sanitized;
    this.instanceIds = ContiguousSet.create(
        Range.closedOpen(0, sanitized.getInstanceCount()),
        DiscreteDomain.integers());
  }

  /**
   * Wraps an unsanitized job configuration.
   *
   * @param unsanitized Unsanitized configuration to sanitize/populate and wrap.
   * @return A wrapper containing the sanitized configuration.
   * @throws TaskDescriptionException If the configuration is invalid.
   */
  public static SanitizedConfiguration fromUnsanitized(
      ConfigurationManager configurationManager,
      IJobConfiguration unsanitized) throws TaskDescriptionException {

    return new SanitizedConfiguration(configurationManager.validateAndPopulate(unsanitized));
  }

  public IJobConfiguration getJobConfig() {
    return sanitized;
  }

  public Set<Integer> getInstanceIds() {
    return instanceIds;
  }

  /**
   * Determines whether this job is configured as a cron job.
   *
   * @return {@code true} if this is a cron job, otherwise {@code false}.
   */
  public boolean isCron() {
    return getJobConfig().isSetCronSchedule();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof SanitizedConfiguration)) {
      return false;
    }

    SanitizedConfiguration other = (SanitizedConfiguration) o;

    return Objects.equal(sanitized, other.sanitized);
  }

  @Override
  public int hashCode() {
    return sanitized.hashCode();
  }

  @Override
  public String toString() {
    return sanitized.toString();
  }
}

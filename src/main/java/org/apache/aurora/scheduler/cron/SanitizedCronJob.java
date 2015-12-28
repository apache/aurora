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
package org.apache.aurora.scheduler.cron;

import java.util.Objects;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Strings;

import org.apache.aurora.gen.CronCollisionPolicy;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.configuration.SanitizedConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;

import static java.util.Objects.requireNonNull;

/**
 * Used by functions that expect field validation before being called.
 */
public final class SanitizedCronJob {
  @VisibleForTesting
  public static final String NO_CRON_SCHEDULE = "Job has an empty cron schedule.";

  private final SanitizedConfiguration config;
  private final CrontabEntry crontabEntry;

  private SanitizedCronJob(ConfigurationManager configurationManager, IJobConfiguration unsanitized)
      throws CronException, ConfigurationManager.TaskDescriptionException {

    this(SanitizedConfiguration.fromUnsanitized(configurationManager, unsanitized));
  }

  private SanitizedCronJob(SanitizedConfiguration config) throws CronException {
    final IJobConfiguration job = config.getJobConfig();
    if (!hasCronSchedule(job)) {
      throw new CronException(NO_CRON_SCHEDULE);
    }

    Optional<CrontabEntry> entry = CrontabEntry.tryParse(job.getCronSchedule());
    if (!entry.isPresent()) {
      throw new CronException("Invalid cron schedule: " + job.getCronSchedule());
    }

    this.config = config;
    this.crontabEntry = entry.get();
  }

  /**
   * Get the default cron collision policy.
   *
   * @param policy A (possibly null) policy.
   * @return The given policy or a default if the policy was null.
   */
  public static CronCollisionPolicy orDefault(@Nullable CronCollisionPolicy policy) {
    return Optional.fromNullable(policy).or(CronCollisionPolicy.KILL_EXISTING);
  }

  /**
   * Create a SanitizedCronJob from a SanitizedConfiguration. SanitizedCronJob performs additional
   * validation to ensure that the provided job contains all properties needed to run it on a
   * cron schedule.
   *
   * @param config Config to validate.
   * @return Config wrapped in defaults.
   * @throws CronException If a cron-specific validation error occured.
   */
  public static SanitizedCronJob from(SanitizedConfiguration config)
      throws CronException {

    return new SanitizedCronJob(config);
  }

  /**
   * Create a cron job from an unsanitized input job. Suitable for RPC input validation.
   *
   * @param unsanitized Unsanitized input job.
   * @return A sanitized job if all validation succeeds.
   * @throws CronException If validation fails with a cron-specific error.
   * @throws ConfigurationManager.TaskDescriptionException If validation fails with a non
   * cron-specific error.
   */
  public static SanitizedCronJob fromUnsanitized(
      ConfigurationManager configurationManager,
      IJobConfiguration unsanitized)
      throws CronException, ConfigurationManager.TaskDescriptionException {

    return new SanitizedCronJob(configurationManager, unsanitized);
  }

  /**
   * Get this job's cron collision policy.
   *
   * @return This job's cron collision policy.
   */
  public CronCollisionPolicy getCronCollisionPolicy() {
    return orDefault(config.getJobConfig().getCronCollisionPolicy());
  }

  private static boolean hasCronSchedule(IJobConfiguration job) {
    requireNonNull(job);
    return !Strings.isNullOrEmpty(job.getCronSchedule());
  }

  /**
   * Returns the cron schedule associated with this job.
   *
   * @return The cron schedule associated with this job.
   */
  public CrontabEntry getCrontabEntry() {
    return crontabEntry;
  }

  /**
   * Returns the sanitized job configuration associated with the cron job.
   *
   * @return This cron job's sanitized job configuration.
   */
  public SanitizedConfiguration getSanitizedConfig() {
    return config;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof SanitizedCronJob)) {
      return false;
    }

    SanitizedCronJob other = (SanitizedCronJob) o;

    return Objects.equals(config, other.config) && Objects.equals(crontabEntry, other.crontabEntry);
  }

  @Override
  public int hashCode() {
    return Objects.hash(config, crontabEntry);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("config", config)
        .add("crontabEntry", crontabEntry)
        .toString();
  }
}

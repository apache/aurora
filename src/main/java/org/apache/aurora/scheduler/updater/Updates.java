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
package org.apache.aurora.scheduler.updater;

import java.util.Set;

import com.google.common.collect.Sets;

import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.apiConstants;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;

import static java.util.Objects.requireNonNull;

/**
 * Utility functions for job updates.
 */
public final class Updates {
  private Updates() {
    // Utility class.
  }

  /**
   * Different states that an active job update may be in.
   */
  public static final Set<JobUpdateStatus> ACTIVE_JOB_UPDATE_STATES =
      Sets.immutableEnumSet(apiConstants.ACTIVE_JOB_UPDATE_STATES);

  public static IJobUpdateKey getKey(IJobUpdateSummary summary) {
    return IJobUpdateKey.build(
        new JobUpdateKey(
            JobKeys.assertValid(summary.getJobKey()).newBuilder(),
            requireNonNull(summary.getUpdateId())));
  }
}

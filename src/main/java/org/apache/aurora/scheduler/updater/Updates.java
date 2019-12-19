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

import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.apiConstants;
import org.apache.aurora.scheduler.storage.entities.IInstanceTaskConfig;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IRange;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static org.apache.aurora.scheduler.base.Numbers.toRange;

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

  /**
   * Creates a range set representing all instance IDs represented by a set of instance
   * configurations included in a job update.
   *
   * @param configs Job update components.
   * @return A range set representing the instance IDs mentioned in instance groupings.
   */
  public static ImmutableRangeSet<Integer> getInstanceIds(Set<IInstanceTaskConfig> configs) {
    ImmutableRangeSet.Builder<Integer> builder = ImmutableRangeSet.builder();
    for (IInstanceTaskConfig config : configs) {
      for (IRange range : config.getInstances()) {
        builder.add(Range.closed(range.getFirst(), range.getLast()));
      }
    }

    return builder.build();
  }

  /**
   * Get the config from a set of {@link IInstanceTaskConfig} for a given instance ID if it exists.
   */
  public static Optional<ITaskConfig> getConfig(
      int id,
      Set<IInstanceTaskConfig> instanceGroups) {

    for (IInstanceTaskConfig group : instanceGroups) {
      for (IRange range : group.getInstances()) {
        if (toRange(range).contains(id)) {
          return Optional.of(group.getTask());
        }
      }
    }

    return Optional.empty();
  }

  /**
   * Get the latest {@link JobUpdateStatus} for an update.
   */
  static JobUpdateStatus getJobUpdateStatus(IJobUpdateDetails jobUpdateDetails) {
    return Iterables.getLast(jobUpdateDetails.getUpdateEvents()).getStatus();
  }
}

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

import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;

import static org.apache.aurora.gen.JobUpdateStatus.ABORTED;
import static org.apache.aurora.gen.JobUpdateStatus.ERROR;
import static org.apache.aurora.gen.JobUpdateStatus.FAILED;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLED_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLED_FORWARD;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_FORWARD;
import static org.apache.aurora.gen.JobUpdateStatus.ROLL_BACK_PAUSED;
import static org.apache.aurora.gen.JobUpdateStatus.ROLL_FORWARD_PAUSED;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.MonitorAction.ROLL_BACK;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.MonitorAction.ROLL_FORWARD;

/**
 * A state machine that determines which high-level actions should be performed in response to
 * a requested change in the status of a job update.
 */
final class JobUpdateStateMachine {

  private JobUpdateStateMachine() {
    // Utility class.
  }

  private static final Multimap<JobUpdateStatus, JobUpdateStatus> ALLOWED_TRANSITIONS =
      ImmutableMultimap.<JobUpdateStatus, JobUpdateStatus>builder()
          .putAll(ROLLING_FORWARD,
              ROLLING_BACK,
              ROLL_FORWARD_PAUSED,
              ROLLED_FORWARD,
              ABORTED,
              ERROR)
          .putAll(ROLLING_BACK, ROLL_BACK_PAUSED, ROLLED_BACK, ABORTED, ERROR, FAILED)
          .putAll(ROLL_FORWARD_PAUSED, ROLLING_FORWARD, ABORTED, ERROR)
          .putAll(ROLL_BACK_PAUSED, ROLLING_BACK, ABORTED, ERROR)
          .build();

  private static final Map<JobUpdateStatus, MonitorAction> ACTIONS =
      ImmutableMap.<JobUpdateStatus, MonitorAction>builder()
          .put(ROLLING_FORWARD, ROLL_FORWARD)
          .put(ROLLING_BACK, ROLL_BACK)
          .build();

  private static final BiMap<JobUpdateStatus, JobUpdateStatus> ACTIVE_TO_PAUSED_STATES =
      ImmutableBiMap.of(
          ROLLING_FORWARD, ROLL_FORWARD_PAUSED,
          ROLLING_BACK, ROLL_BACK_PAUSED);

  static final IJobUpdateQuery ACTIVE_QUERY = IJobUpdateQuery.build(
      new JobUpdateQuery()
          .setUpdateStatuses(ImmutableSet.copyOf(ACTIVE_TO_PAUSED_STATES.keySet())));

  private static final Map<JobUpdateStatus, JobUpdateStatus> PAUSE_BEHAVIOR =
      ImmutableMap.<JobUpdateStatus, JobUpdateStatus>builder()
          .putAll(ACTIVE_TO_PAUSED_STATES)
          .put(ROLL_FORWARD_PAUSED, ROLL_FORWARD_PAUSED)
          .put(ROLL_BACK_PAUSED, ROLL_BACK_PAUSED)
          .build();

  static final Function<JobUpdateStatus, JobUpdateStatus> GET_PAUSE_STATE =
      new Function<JobUpdateStatus, JobUpdateStatus>() {
        @Override
        public JobUpdateStatus apply(JobUpdateStatus status) {
          return PAUSE_BEHAVIOR.get(status);
        }
      };

  private static final Map<JobUpdateStatus, JobUpdateStatus> RESUME_BEHAVIOR =
      ImmutableMap.<JobUpdateStatus, JobUpdateStatus>builder()
          .putAll(ACTIVE_TO_PAUSED_STATES.inverse())
          .put(ROLLING_FORWARD, ROLLING_FORWARD)
          .put(ROLLING_BACK, ROLLING_BACK)
          .build();

  static final Function<JobUpdateStatus, JobUpdateStatus> GET_RESUME_STATE =
      new Function<JobUpdateStatus, JobUpdateStatus>() {
        @Override
        public JobUpdateStatus apply(JobUpdateStatus status) {
          return RESUME_BEHAVIOR.get(status);
        }
      };

  /**
   * Determines the action to take in response to a status change on a job update.
   *
   * @param from Starting state.
   * @param to Desired target state.
   * @throws IllegalStateException if the requested transition is not allowed.
   */
  static void assertTransitionAllowed(JobUpdateStatus from, JobUpdateStatus to) {
    if (!ALLOWED_TRANSITIONS.containsEntry(from, to)) {
      throw new IllegalStateException("Cannot transition update from " + from + " to " + to);
    }
  }

  static MonitorAction getActionForStatus(JobUpdateStatus status) {
    return Optional.fromNullable(ACTIONS.get(status)).or(MonitorAction.STOP_WATCHING);
  }

  static boolean isActive(JobUpdateStatus status) {
    return ACTIVE_TO_PAUSED_STATES.keySet().contains(status);
  }

  /**
   * An action to take in response to a state transition.
   */
  enum MonitorAction {
    /**
     * The update has moved to an inactive state and its tasks should no longer be monitored.
     */
    STOP_WATCHING,

    /**
     * Initiate an update of the job.
     */
    ROLL_FORWARD,

    /**
     * Initiate a revert of the job update to the previous configuration.
     */
    ROLL_BACK
  }
}

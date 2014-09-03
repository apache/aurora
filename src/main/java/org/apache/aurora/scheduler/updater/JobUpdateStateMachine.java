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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import org.apache.aurora.gen.JobUpdateStatus;

import static org.apache.aurora.gen.JobUpdateStatus.ABORTED;
import static org.apache.aurora.gen.JobUpdateStatus.ERROR;
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
          .putAll(ROLLING_BACK, ROLL_BACK_PAUSED, ROLLED_BACK, ABORTED, ERROR)
          .putAll(ROLL_FORWARD_PAUSED, ROLLING_FORWARD, ABORTED, ERROR)
          .putAll(ROLL_BACK_PAUSED, ROLLING_BACK, ABORTED, ERROR)
          .build();

  private static final Map<JobUpdateStatus, MonitorAction> ACTIONS =
      ImmutableMap.<JobUpdateStatus, MonitorAction>builder()
          .put(ROLLING_FORWARD, ROLL_FORWARD)
          .put(ROLLING_BACK, ROLL_BACK)
          .build();

  /**
   * Determines the action to take in response to a status change on a job update.
   *
   * @param from Starting state.
   * @param to Desired target state.
   * @return The action to perform when moving from {@code from} to {@code to}.
   * @throws IllegalStateException if the requested transition is not allowed.
   */
  static MonitorAction transition(JobUpdateStatus from, JobUpdateStatus to) {
    if (!ALLOWED_TRANSITIONS.containsEntry(from, to)) {
      throw new IllegalStateException("Cannot transition update from " + from + " to " + to);
    }

    return Optional.fromNullable(ACTIONS.get(to)).or(MonitorAction.STOP_WATCHING);
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

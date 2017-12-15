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
import java.util.Optional;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

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
import static org.apache.aurora.gen.JobUpdateStatus.ROLL_BACK_AWAITING_PULSE;
import static org.apache.aurora.gen.JobUpdateStatus.ROLL_BACK_PAUSED;
import static org.apache.aurora.gen.JobUpdateStatus.ROLL_FORWARD_AWAITING_PULSE;
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
              ROLL_FORWARD_AWAITING_PULSE,
              ROLLED_FORWARD,
              ABORTED,
              FAILED,
              ERROR)
          .putAll(ROLLING_BACK,
              ROLL_BACK_PAUSED,
              ROLL_BACK_AWAITING_PULSE,
              ROLLED_BACK,
              ABORTED,
              ERROR,
              FAILED)
          .putAll(ROLL_FORWARD_PAUSED,
              ROLLING_BACK,
              ROLLING_FORWARD,
              ROLL_FORWARD_AWAITING_PULSE,
              ABORTED,
              ERROR)
          .putAll(ROLL_BACK_PAUSED, ROLLING_BACK, ROLL_BACK_AWAITING_PULSE, ABORTED, ERROR)
          .putAll(ROLL_FORWARD_AWAITING_PULSE,
              ROLLING_BACK,
              ROLLING_FORWARD,
              ROLL_FORWARD_PAUSED,
              ABORTED,
              ERROR)
          .putAll(ROLL_BACK_AWAITING_PULSE, ROLLING_BACK, ROLL_BACK_PAUSED, ABORTED, ERROR)
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

  private static final BiMap<JobUpdateStatus, JobUpdateStatus> ACTIVE_TO_BLOCKED_STATES =
      ImmutableBiMap.of(
          ROLLING_FORWARD, ROLL_FORWARD_AWAITING_PULSE,
          ROLLING_BACK, ROLL_BACK_AWAITING_PULSE);

  private static final BiMap<JobUpdateStatus, JobUpdateStatus> BLOCKED_TO_PAUSED_STATES =
      ImmutableBiMap.of(
          ROLL_FORWARD_AWAITING_PULSE, ROLL_FORWARD_PAUSED,
          ROLL_BACK_AWAITING_PULSE, ROLL_BACK_PAUSED);

  static final IJobUpdateQuery ACTIVE_QUERY = IJobUpdateQuery.build(
      new JobUpdateQuery().setUpdateStatuses(Updates.ACTIVE_JOB_UPDATE_STATES));

  static final Set<JobUpdateStatus> AUTO_RESUME_STATES =
      Sets.immutableEnumSet(ACTIVE_TO_PAUSED_STATES.keySet());

  private static final Map<JobUpdateStatus, JobUpdateStatus> PAUSE_BEHAVIOR =
      ImmutableMap.<JobUpdateStatus, JobUpdateStatus>builder()
          .putAll(ACTIVE_TO_PAUSED_STATES)
          .putAll(BLOCKED_TO_PAUSED_STATES)
          .put(ROLL_FORWARD_PAUSED, ROLL_FORWARD_PAUSED)
          .put(ROLL_BACK_PAUSED, ROLL_BACK_PAUSED)
          .build();

  private static final Map<JobUpdateStatus, JobUpdateStatus> BLOCK_BEHAVIOR =
      ImmutableMap.<JobUpdateStatus, JobUpdateStatus>builder()
          .putAll(ACTIVE_TO_BLOCKED_STATES)
          .put(ROLL_FORWARD_AWAITING_PULSE, ROLL_FORWARD_AWAITING_PULSE)
          .put(ROLL_BACK_AWAITING_PULSE, ROLL_BACK_AWAITING_PULSE)
          .build();

  private static final Map<JobUpdateStatus, JobUpdateStatus> UNBLOCK_BEHAVIOR =
      ImmutableMap.<JobUpdateStatus, JobUpdateStatus>builder()
          .putAll(ACTIVE_TO_BLOCKED_STATES.inverse())
          .put(ROLLING_FORWARD, ROLLING_FORWARD)
          .put(ROLLING_BACK, ROLLING_BACK)
          .put(ROLL_FORWARD_PAUSED, ROLL_FORWARD_PAUSED)
          .put(ROLL_BACK_PAUSED, ROLL_BACK_PAUSED)
          .build();

  static final Function<JobUpdateStatus, JobUpdateStatus> GET_PAUSE_STATE =
      PAUSE_BEHAVIOR::get;

  private static final Map<JobUpdateStatus, JobUpdateStatus> RESUME_ACTIVE_BEHAVIOR =
      ImmutableMap.<JobUpdateStatus, JobUpdateStatus>builder()
          .putAll(ACTIVE_TO_PAUSED_STATES.inverse())
          .put(ROLLING_FORWARD, ROLLING_FORWARD)
          .put(ROLLING_BACK, ROLLING_BACK)
          .build();

  private static final Map<JobUpdateStatus, JobUpdateStatus> RESUME_BLOCKED_BEHAVIOR =
      ImmutableMap.<JobUpdateStatus, JobUpdateStatus>builder()
          .putAll(BLOCKED_TO_PAUSED_STATES.inverse())
          .put(ROLL_FORWARD_AWAITING_PULSE, ROLL_FORWARD_AWAITING_PULSE)
          .put(ROLL_BACK_AWAITING_PULSE, ROLL_BACK_AWAITING_PULSE)
          .build();

  static final Function<JobUpdateStatus, JobUpdateStatus> GET_ACTIVE_RESUME_STATE =
      RESUME_ACTIVE_BEHAVIOR::get;

  static final Function<JobUpdateStatus, JobUpdateStatus> GET_BLOCKED_RESUME_STATE =
      RESUME_BLOCKED_BEHAVIOR::get;

  static final Function<JobUpdateStatus, JobUpdateStatus> GET_UNBLOCKED_STATE =
      UNBLOCK_BEHAVIOR::get;

  static JobUpdateStatus getBlockedState(JobUpdateStatus status) {
    return BLOCK_BEHAVIOR.get(status);
  }

  /**
   * Determines the action to take in response to a status change on a job update.
   *
   * @param from Starting state.
   * @param to Desired target state.
   * @throws UpdateStateException if the requested transition is not allowed.
   */
  static void assertTransitionAllowed(JobUpdateStatus from, JobUpdateStatus to)
      throws UpdateStateException {

    if (!ALLOWED_TRANSITIONS.containsEntry(from, to)) {
      throw new UpdateStateException("Cannot transition update from " + from + " to " + to);
    }
  }

  static MonitorAction getActionForStatus(JobUpdateStatus status) {
    return Optional.ofNullable(ACTIONS.get(status)).orElse(MonitorAction.STOP_WATCHING);
  }

  static boolean isActive(JobUpdateStatus status) {
    return ACTIVE_TO_PAUSED_STATES.keySet().contains(status);
  }

  static boolean isAwaitingPulse(JobUpdateStatus status) {
    return BLOCKED_TO_PAUSED_STATES.keySet().contains(status);
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

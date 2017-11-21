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
package org.apache.aurora.scheduler.state;

import java.util.Objects;

import com.google.common.base.Optional;

import org.apache.aurora.gen.ScheduleStatus;

/**
 * Descriptions of the different types of external work commands that task state machines may
 * trigger.
 */
class SideEffect {
  private final Action action;
  private final Optional<ScheduleStatus> nextState;

  SideEffect(Action action, Optional<ScheduleStatus> nextState) {
    this.action = action;
    this.nextState = nextState;
  }

  public Action getAction() {
    return action;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof SideEffect)) {
      return false;
    }

    SideEffect other = (SideEffect) o;
    return Objects.equals(action, other.action)
        && Objects.equals(nextState, other.nextState);
  }

  @Override
  public int hashCode() {
    return Objects.hash(action, nextState);
  }

  @Override
  public String toString() {
    if (nextState.isPresent()) {
      return action.toString() + " " + nextState.get();
    } else {
      return action.toString();
    }
  }

  enum Action {
    /**
     * Send an instruction for the runner of this task to kill the task.
     */
    KILL,

    /**
     * Transition a task to LOST.
     */
    TRANSITION_TO_LOST,

    /**
     * Create a new state machine with a copy of this task.
     */
    RESCHEDULE,

    /**
     * Update the task's state (schedule status) in the persistent store to match the state machine.
     */
    SAVE_STATE,

    /**
     * Delete this task from the persistent store.
     */
    DELETE,

    /**
     * Increment the failure count for this task.
     */
    INCREMENT_FAILURES
  }
}

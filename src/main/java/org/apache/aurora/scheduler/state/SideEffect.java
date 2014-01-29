/**
 * Copyright 2014 Apache Software Foundation
 *
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

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

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
    if (action == Action.STATE_CHANGE) {
      Preconditions.checkArgument(
          nextState.isPresent(),
          "A next state must be provided for a state change action.");
    }
    this.nextState = nextState;
  }

  public Action getAction() {
    return action;
  }

  public Optional<ScheduleStatus> getNextState() {
    return nextState;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof SideEffect)) {
      return false;
    }

    SideEffect other = (SideEffect) o;
    return Objects.equal(action, other.action)
        && Objects.equal(nextState, other.nextState);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(action, nextState);
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
    INCREMENT_FAILURES,

    /**
     * Perform an additional state change on the task.
     */
    STATE_CHANGE
  }
}

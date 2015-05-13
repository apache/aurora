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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import static org.apache.aurora.scheduler.state.StateChangeResult.ILLEGAL_WITH_SIDE_EFFECTS;
import static org.apache.aurora.scheduler.state.StateChangeResult.SUCCESS;

/**
 * The actions that should be performed in response to a state transition attempt.
 *
 * {@see TaskStateMachine}
 */
public class TransitionResult {
  private final StateChangeResult result;
  private final ImmutableSet<SideEffect> sideEffects;

  /**
   * Creates a transition result with the given side effects.
   *
   * @param result Transition attempt result.
   * @param sideEffects Actions that must be performed in response to the state transition.
   */
  public TransitionResult(StateChangeResult result, ImmutableSet<SideEffect> sideEffects) {
    this.result = result;
    this.sideEffects = Objects.requireNonNull(sideEffects);
    if (!this.sideEffects.isEmpty()) {
      Preconditions.checkArgument(
          result == SUCCESS || result == ILLEGAL_WITH_SIDE_EFFECTS,
          "Invalid transition result for a non-empty set of side effects");
    }
  }

  public StateChangeResult getResult() {
    return result;
  }

  public ImmutableSet<SideEffect> getSideEffects() {
    return sideEffects;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TransitionResult)) {
      return false;
    }

    TransitionResult other = (TransitionResult) o;
    return Objects.equals(result, other.result)
        && Objects.equals(sideEffects, other.sideEffects);
  }

  @Override
  public int hashCode() {
    return Objects.hash(result, sideEffects);
  }

  @Override
  public String toString() {
    return com.google.common.base.Objects.toStringHelper(this)
        .add("result", result)
        .add("sideEffects", sideEffects)
        .toString();
  }
}

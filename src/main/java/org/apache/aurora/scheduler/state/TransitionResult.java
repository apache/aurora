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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

/**
 * The actions that should be performed in response to a state transition attempt.
 *
 * {@see TaskStateMachine}
 */
public class TransitionResult {
  private final boolean success;
  private final ImmutableSet<SideEffect> sideEffects;

  /**
   * Creates a transition result with the given side effects.
   *
   * @param success Whether the transition attempt relevant to this result was successful.
   * @param sideEffects Actions that must be performed in response to the state transition.
   */
  public TransitionResult(boolean success, ImmutableSet<SideEffect> sideEffects) {
    this.success = success;
    this.sideEffects = Preconditions.checkNotNull(sideEffects);
  }

  public boolean isSuccess() {
    return success;
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
    return success == other.success
        && Objects.equal(sideEffects, other.sideEffects);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(success, sideEffects);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("success", success)
        .add("sideEffects", sideEffects)
        .toString();
  }
}

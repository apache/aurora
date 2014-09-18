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

import java.util.Objects;
import java.util.Set;

import com.google.common.base.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Side-effect resulting from evaluating an instance during a job update.  Side-effects include
 * actions that should be performed as well as status changes of the instance monitor.
 */
public class SideEffect {
  private final Optional<InstanceAction> action;
  private final Set<InstanceUpdateStatus> statusChanges;

  /**
   * Creates a new side-effect.
   *
   * @param action Action to be taken on the instance, if necessary.
   * @param statusChanges Any status changes to the instance monitor.
   */
  public SideEffect(Optional<InstanceAction> action, Set<InstanceUpdateStatus> statusChanges) {
    this.action = requireNonNull(action);
    this.statusChanges = requireNonNull(statusChanges);
  }

  /**
   * Gets the action that should be performed with this side-effect, if any.
   *
   * @return The action associated with this side-effect.
   */
  public Optional<InstanceAction> getAction() {
    return action;
  }

  /**
   * Gets the status changes that the instance underwent while being evaluated.
   *
   * @return Instance updater status changes.
   */
  public Set<InstanceUpdateStatus> getStatusChanges() {
    return statusChanges;
  }

  /**
   * Tests whether any of multiple side-effects contain {@link #getAction() actions} to be
   * performed.
   *
   * @param sideEffects Side-effects to inspect for actions.
   * @return {@code true} if at least one of the side-effects contains an action to perform,
   *         otherwise {@code false}.
   */
  public static boolean hasActions(Iterable<SideEffect> sideEffects) {
    for (SideEffect sideEffect : sideEffects) {
      if (sideEffect.getAction().isPresent()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof SideEffect)) {
      return false;
    }

    SideEffect other = (SideEffect) o;
    return Objects.equals(other.getAction(), getAction())
        && Objects.equals(other.getStatusChanges(), getStatusChanges());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getAction(), getStatusChanges());
  }

  @Override
  public String toString() {
    return com.google.common.base.Objects.toStringHelper(this)
        .add("action", getAction())
        .add("statusChanges", getStatusChanges())
        .toString();
  }

  /**
   * The status of an instance being updated as part of a one-way job update.
   */
  public enum InstanceUpdateStatus {
    /**
     * The instance has not yet been modified by the update.
     */
    IDLE,

    /**
     * The instance is being modified to reach the target state of the one-way update.
     */
    WORKING,

    /**
     * The instance updated successfully and is no longer being monitored.
     */
    SUCCEEDED,

    /**
     * The instance failed to update and is no longer being monitored.
     */
    FAILED
  }
}

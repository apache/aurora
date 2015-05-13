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

/**
 * The result of a task state change request.
 */
public enum StateChangeResult {
  /**
   * Task is already in target state.
   */
  NOOP,

  /**
   * Task state has been changed successfully.
   */
  SUCCESS,

  /**
   * Change is illegal given the current task state.
   */
  ILLEGAL,

  /**
   * Same as {@link #ILLEGAL} but processing state change request generated some task side effects
   * (e.g.: a driver.kill() request has been placed).
   */
  ILLEGAL_WITH_SIDE_EFFECTS,

  /**
   * Change is not allowed due to the task not in CAS state.
   */
  INVALID_CAS_STATE
}

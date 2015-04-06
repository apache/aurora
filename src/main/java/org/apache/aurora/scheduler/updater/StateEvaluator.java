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

import com.google.common.base.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Determines actions that must be taken to change the configuration of a running task.
 * <p>
 * A state evaluator is expected to be used multiple times over the course of changing an active
 * task's configuration.  This should be invoked every time the state of an instance changes to
 * determine what action to take next.  It's expected that it will eventually converge by
 * {@link Result#SUCCEEDED succeeding}, or failing with {@link Result#FAILED_TERMINATED}.
 *
 * @param <T> Instance state type.
 */
interface StateEvaluator<T> {

  /**
   * Evaluates the state differences between the desired state and the provided {@code actualState}.
   * <p>
   * This function should be idempotent, with the exception of an internal failure counter that
   * increments when an updating task exits, or an active but not
   * {@link org.apache.aurora.gen.ScheduleStatus#RUNNING RUNNING} task takes too long to start.
   * <p>
   * It is the responsibility of the caller to ensure that the {@code actualState} is the latest
   * value.  Note: the caller should avoid calling this when a terminal task is moving to another
   * terminal state.  It should also suppress deletion events for tasks that have been replaced by
   * an active task.
   *
   * @param actualState The actual observed state of the task.
   * @return the evaluation result, including the state of the instance update, and a necessary
   *         action to perform.
   */
  Result evaluate(T actualState);

  Optional<Failure> NO_FAILURE = Optional.absent();

  enum Result {
    EVALUATE_ON_STATE_CHANGE(Optional.of(InstanceAction.AWAIT_STATE_CHANGE), NO_FAILURE),
    REPLACE_TASK_AND_EVALUATE_ON_STATE_CHANGE(Optional.of(InstanceAction.ADD_TASK), NO_FAILURE),
    KILL_TASK_AND_EVALUATE_ON_STATE_CHANGE(Optional.of(InstanceAction.KILL_TASK), NO_FAILURE),
    EVALUATE_AFTER_MIN_RUNNING_MS(Optional.of(InstanceAction.WATCH_TASK), NO_FAILURE),
    SUCCEEDED(Optional.<InstanceAction>absent(), NO_FAILURE),
    FAILED_TERMINATED(Optional.<InstanceAction>absent(), Optional.of(Failure.EXITED));

    private final Optional<InstanceAction> action;
    private final Optional<Failure> failure;

    Result(Optional<InstanceAction> action, Optional<Failure> failure) {
      this.action = requireNonNull(action);
      this.failure = requireNonNull(failure);
    }

    public Optional<InstanceAction> getAction() {
      return action;
    }

    public Optional<Failure> getFailure() {
      return failure;
    }
  }

  enum Failure {
    EXITED("exited.");

    private final String reason;

    Failure(String reason) {
      this.reason = reason;
    }

    public String getReason() {
      return reason;
    }
  }
}

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
import java.util.Objects;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.twitter.common.util.StateMachine;

import org.apache.aurora.scheduler.updater.strategy.UpdateStrategy;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkArgument;

import static org.apache.aurora.scheduler.updater.StateEvaluator.Result;

/**
 * Controller for a one-way job update (i.e. no rollbacks).  The controller will coordinate updates
 * of all instances within the job, and roll up the results of the individual updates into the
 * result of the job update.
 *
 * @param <K> Type used to uniquely identify instances.
 * @param <T> Instance data type.
 */
class OneWayJobUpdater<K, T> {
  private final UpdateStrategy<K> strategy;
  private final int maxFailedInstances;
  private final Map<K, InstanceUpdate<T>> instances;
  private final StateMachine<OneWayStatus> stateMachine =
      StateMachine.<OneWayStatus>builder("job_update")
          .initialState(OneWayStatus.IDLE)
          .addState(OneWayStatus.IDLE, OneWayStatus.WORKING)
          .addState(OneWayStatus.WORKING, OneWayStatus.SUCCEEDED, OneWayStatus.FAILED)
          .addState(OneWayStatus.SUCCEEDED)
          .addState(OneWayStatus.FAILED)
          .throwOnBadTransition(true)
          .build();

  /**
   * Creates a new one-way updater.
   *
   * @param strategy The strategy to decide which instances to update after a state change.
   * @param maxFailedInstances Maximum tolerated failures before the update is considered failed.
   * @param instanceEvaluators Evaluate the state of individual instances, and decide what actions
   *                           must be taken to update them.
   */
  OneWayJobUpdater(
      UpdateStrategy<K> strategy,
      int maxFailedInstances,
      Map<K, StateEvaluator<T>> instanceEvaluators) {

    this.strategy = requireNonNull(strategy);
    this.maxFailedInstances = maxFailedInstances;
    checkArgument(!instanceEvaluators.isEmpty());

    this.instances = ImmutableMap.copyOf(Maps.transformValues(
        instanceEvaluators,
        new Function<StateEvaluator<T>, InstanceUpdate<T>>() {
          @Override
          public InstanceUpdate<T> apply(StateEvaluator<T> evaluator) {
            return new InstanceUpdate<>(evaluator);
          }
        }));
  }

  private static final Function<InstanceUpdate<?>, InstanceUpdateStatus> GET_STATE =
      new Function<InstanceUpdate<?>, InstanceUpdateStatus>() {
        @Override
        public InstanceUpdateStatus apply(InstanceUpdate<?> manager) {
          return manager.getState();
        }
      };

  private static <K, T> Map<K, InstanceUpdate<T>> filterByStatus(
      Map<K, InstanceUpdate<T>> instances,
      InstanceUpdateStatus status) {

    return ImmutableMap.copyOf(
        Maps.filterValues(instances, Predicates.compose(Predicates.equalTo(status), GET_STATE)));
  }

  private static Optional<InstanceAction> resultToAction(Result result) {
    switch (result) {
      case EVALUATE_ON_STATE_CHANGE:
        return Optional.of(InstanceAction.EVALUATE_ON_STATE_CHANGE);
      case REPLACE_TASK_AND_EVALUATE_ON_STATE_CHANGE:
        return Optional.of(InstanceAction.REPLACE_TASK_AND_EVALUATE_ON_STATE_CHANGE);
      case KILL_TASK_AND_EVALUATE_ON_STATE_CHANGE:
        return Optional.of(InstanceAction.KILL_TASK_AND_EVALUATE_ON_STATE_CHANGE);
      case EVALUATE_AFTER_MIN_RUNNING_MS:
        return Optional.of(InstanceAction.EVALUATE_AFTER_MIN_RUNNING_MS);
      default:
        break;
    }

    return Optional.absent();
  }

  @VisibleForTesting
  Set<K> getInstances() {
    return ImmutableSet.copyOf(instances.keySet());
  }

  /**
   * Performs an evaluation of the job.  An evaluation would normally be triggered to initiate the
   * update, as a result of a state change relevant to the update, or due to a
   * {@link InstanceAction#EVALUATE_AFTER_MIN_RUNNING_MS requested} instance re-evaluation.
   *
   * @param instancesNeedingUpdate Instances triggering the event, if any.
   * @param stateProvider Provider to fetch state of instances, and pass to
   *                      {@link StateEvaluator#evaluate(Object)}.
   * @return The outcome of the evaluation, including the state of the job update and actions the
   *         caller should perform on individual instances.
   * @throws IllegalStateException if the job updater is not currently
   *         {@link OneWayStatus#WORKING working} state, as indicated by a previous evaluation.
   */
  synchronized EvaluationResult<K> evaluate(
      Set<K> instancesNeedingUpdate,
      InstanceStateProvider<K, T> stateProvider) {

    if (stateMachine.getState() == OneWayStatus.IDLE) {
      stateMachine.transition(OneWayStatus.WORKING);
    }
    Preconditions.checkState(
        stateMachine.getState() == OneWayStatus.WORKING,
        "Attempted to evaluate an inactive job updater.");

    // Call order is important here: update on-demand instances, evaluate new instances, compute
    // job update state.
    Map<K, InstanceAction> actions = ImmutableMap.<K, InstanceAction>builder()
        // Re-evaluate instances that are in need of update.
        .putAll(evaluateInstances(instancesNeedingUpdate, stateProvider))
        // If ready to begin updating more instances, evaluate those as well.
        .putAll(startNextInstanceGroup(stateProvider))
        .build();

    return new EvaluationResult<K>(computeJobUpdateStatus(), actions);
  }

  private Map<K, InstanceAction> evaluateInstances(
      Set<K> instanceIds,
      InstanceStateProvider<K, T> stateProvider) {

    ImmutableMap.Builder<K, InstanceAction> actions = ImmutableMap.builder();
    for (K instanceId : instanceIds) {
      InstanceUpdate<T> update = instances.get(instanceId);
      // Suppress state changes for updates that are not in-progress.
      if (update.getState() == InstanceUpdateStatus.WORKING) {
        Optional<InstanceAction> action =
            resultToAction(update.evaluate(stateProvider.getState(instanceId)));
        if (action.isPresent()) {
          actions.put(instanceId, action.get());
        }
      }
    }

    return actions.build();
  }

  private Map<K, InstanceAction> startNextInstanceGroup(InstanceStateProvider<K, T> stateProvider) {
    ImmutableMap.Builder<K, InstanceAction> actions = ImmutableMap.builder();

    Map<K, InstanceUpdate<T>> idle = filterByStatus(instances, InstanceUpdateStatus.IDLE);
    if (!idle.isEmpty()) {
      Map<K, InstanceUpdate<T>> working =
          filterByStatus(instances, InstanceUpdateStatus.WORKING);
      for (K instance : strategy.getNextGroup(idle.keySet(), working.keySet())) {
        Result result = instances.get(instance).evaluate(stateProvider.getState(instance));
        Optional<InstanceAction> action = resultToAction(result);
        if (action.isPresent()) {
          actions.put(instance, action.get());
        }
      }
    }

    return actions.build();
  }

  private OneWayStatus computeJobUpdateStatus() {
    Map<K, InstanceUpdate<T>> idle = filterByStatus(instances, InstanceUpdateStatus.IDLE);
    Map<K, InstanceUpdate<T>> working =
        filterByStatus(instances, InstanceUpdateStatus.WORKING);
    Map<K, InstanceUpdate<T>> failed = filterByStatus(instances, InstanceUpdateStatus.FAILED);
    // TODO(wfarner): This needs to be updated to support rollback.
    if (failed.size() > maxFailedInstances) {
      stateMachine.transition(OneWayStatus.FAILED);
    } else if (working.isEmpty() && idle.isEmpty()) {
      stateMachine.transition(OneWayStatus.SUCCEEDED);
    }

    return stateMachine.getState();
  }

  /**
   * Container and state for the update of an individual instance.
   */
  private static class InstanceUpdate<T> {
    private final StateEvaluator<T> evaluator;
    private final StateMachine<InstanceUpdateStatus> stateMachine =
        StateMachine.<InstanceUpdateStatus>builder("instance_update")
            .initialState(InstanceUpdateStatus.IDLE)
            .addState(InstanceUpdateStatus.IDLE, InstanceUpdateStatus.WORKING)
            .addState(
                InstanceUpdateStatus.WORKING,
                InstanceUpdateStatus.SUCCEEDED,
                InstanceUpdateStatus.FAILED)
            .addState(InstanceUpdateStatus.SUCCEEDED)
            .addState(InstanceUpdateStatus.FAILED)
            .throwOnBadTransition(true)
            .build();

    InstanceUpdate(StateEvaluator<T> evaluator) {
      this.evaluator = requireNonNull(evaluator);
    }

    InstanceUpdateStatus getState() {
      return stateMachine.getState();
    }

    Result evaluate(T actualState) {
      if (stateMachine.getState() == InstanceUpdateStatus.IDLE) {
        stateMachine.transition(InstanceUpdateStatus.WORKING);
      }

      Result result = evaluator.evaluate(actualState);
      if (result == Result.SUCCEEDED) {
        stateMachine.transition(InstanceUpdateStatus.SUCCEEDED);
      } else if (result == Result.FAILED) {
        stateMachine.transition(InstanceUpdateStatus.FAILED);
      }
      return result;
    }
  }

  private enum InstanceUpdateStatus {
    IDLE,
    WORKING,
    SUCCEEDED,
    FAILED
  }

  /**
   * Status of the job update.
   */
  enum OneWayStatus {
    IDLE,
    WORKING,
    SUCCEEDED,
    FAILED
  }

  /**
   * Action that should be performed by the caller to converge towards the desired update state.
   */
  enum InstanceAction {
    EVALUATE_ON_STATE_CHANGE,
    REPLACE_TASK_AND_EVALUATE_ON_STATE_CHANGE,
    KILL_TASK_AND_EVALUATE_ON_STATE_CHANGE,
    EVALUATE_AFTER_MIN_RUNNING_MS
  }

  /**
   * Result of an evaluation round.
   */
  static class EvaluationResult<K> {
    private final OneWayStatus jobStatus;
    private final Map<K, InstanceAction> instanceActions;

    EvaluationResult(OneWayStatus jobStatus, Map<K, InstanceAction> instanceActions) {
      this.jobStatus = requireNonNull(jobStatus);
      this.instanceActions = requireNonNull(instanceActions);
    }

    public OneWayStatus getJobStatus() {
      return jobStatus;
    }

    public Map<K, InstanceAction> getInstanceActions() {
      return instanceActions;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof EvaluationResult)) {
        return false;
      }
      @SuppressWarnings("unchecked")
      EvaluationResult<K> other = (EvaluationResult<K>) obj;
      return other.getJobStatus().equals(this.getJobStatus())
          && other.getInstanceActions().equals(this.getInstanceActions());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getJobStatus(), getInstanceActions());
    }

    @Override
    public String toString() {
      return com.google.common.base.Objects.toStringHelper(this)
          .add("jobStatus", getJobStatus())
          .add("instanceActions", getInstanceActions())
          .toString();
    }
  }
}

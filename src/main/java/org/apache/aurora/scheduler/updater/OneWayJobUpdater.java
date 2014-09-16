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
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Maps.EntryTransformer;
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
  private static final Logger LOG = Logger.getLogger(OneWayJobUpdater.class.getName());

  private final UpdateStrategy<K> strategy;
  private final int maxFailedInstances;
  private final Map<K, InstanceUpdate<T>> instances;
  private final StateMachine<OneWayStatus> stateMachine =
      StateMachine.<OneWayStatus>builder("job_update")
          .initialState(OneWayStatus.IDLE)
          .addState(OneWayStatus.IDLE, OneWayStatus.WORKING)
          .addState(OneWayStatus.WORKING, OneWayStatus.SUCCEEDED, OneWayStatus.FAILED)
          .addState(OneWayStatus.SUCCEEDED, OneWayStatus.SUCCEEDED)
          .addState(OneWayStatus.FAILED, OneWayStatus.FAILED)
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

    this.instances = ImmutableMap.copyOf(Maps.transformEntries(
        instanceEvaluators,
        new EntryTransformer<K, StateEvaluator<T>, InstanceUpdate<T>>() {
          @Override
          public InstanceUpdate<T> transformEntry(K key, StateEvaluator<T> value) {
            return new InstanceUpdate<>("Instance " + key, value);
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

  private static <K, T> Set<K> filterByStatus(
      Map<K, InstanceUpdate<T>> instances,
      InstanceUpdateStatus status) {

    return ImmutableSet.copyOf(
        Maps.filterValues(instances, Predicates.compose(Predicates.equalTo(status), GET_STATE))
            .keySet());
  }

  private static Optional<InstanceAction> resultToAction(Result result) {
    switch (result) {
      case EVALUATE_ON_STATE_CHANGE:
        return Optional.of(InstanceAction.AWAIT_STATE_CHANGE);
      case REPLACE_TASK_AND_EVALUATE_ON_STATE_CHANGE:
        return Optional.of(InstanceAction.ADD_TASK);
      case KILL_TASK_AND_EVALUATE_ON_STATE_CHANGE:
        return Optional.of(InstanceAction.KILL_TASK);
      case EVALUATE_AFTER_MIN_RUNNING_MS:
        return Optional.of(InstanceAction.WATCH_TASK);
      default:
        return Optional.absent();
    }
  }

  @VisibleForTesting
  Set<K> getInstances() {
    return ImmutableSet.copyOf(instances.keySet());
  }

  /**
   * Checks whether an instance is in scope for this update.
   *
   * @param instanceId Instance id to check.
   * @return {@code true} if the instance is part of the update, as originally specified by
   *         {@code instanceEvaluators}.
   */
  boolean containsInstance(K instanceId) {
    return instances.containsKey(instanceId);
  }

  /**
   * Performs an evaluation of the job.  An evaluation would normally be triggered to initiate the
   * update, as a result of a state change relevant to the update, or due to a
   * {@link InstanceAction#WATCH_TASK requested} instance re-evaluation.
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
      Map<K, T> instancesNeedingUpdate,
      InstanceStateProvider<K, T> stateProvider) {

    if (stateMachine.getState() == OneWayStatus.IDLE) {
      stateMachine.transition(OneWayStatus.WORKING);
    }
    Preconditions.checkState(
        stateMachine.getState() == OneWayStatus.WORKING,
        "Attempted to evaluate an inactive job updater.");

    // Call order is important here: update on-demand instances, evaluate new instances, compute
    // job update state.
    ImmutableMap.Builder<K, InstanceAction> actions = ImmutableMap.<K, InstanceAction>builder()
        // Re-evaluate instances that are in need of update.
        .putAll(evaluateInstances(instancesNeedingUpdate));

    if (computeJobUpdateStatus() == OneWayStatus.WORKING) {
      // If ready to begin updating more instances, evaluate those as well.
      actions.putAll(startNextInstanceGroup(stateProvider));
    }

    return new EvaluationResult<K>(computeJobUpdateStatus(), actions.build());
  }

  private Map<K, InstanceAction> evaluateInstances(Map<K, T> updatedInstances) {
    ImmutableMap.Builder<K, InstanceAction> actions = ImmutableMap.builder();
    for (Map.Entry<K, T> entry : updatedInstances.entrySet()) {
      K instanceId = entry.getKey();
      InstanceUpdate<T> update = instances.get(instanceId);
      // Suppress state changes for updates that are not in-progress.
      if (update.getState() == InstanceUpdateStatus.WORKING) {
        Optional<InstanceAction> action = resultToAction(update.evaluate(entry.getValue()));
        if (action.isPresent()) {
          actions.put(instanceId, action.get());
        }
      } else {
        LOG.info("Ignoring state change for instance outside working set: " + instanceId);
      }
    }

    return actions.build();
  }

  private Map<K, InstanceAction> startNextInstanceGroup(InstanceStateProvider<K, T> stateProvider) {
    Set<K> idle = filterByStatus(instances, InstanceUpdateStatus.IDLE);
    if (idle.isEmpty()) {
      return ImmutableMap.of();
    } else {
      ImmutableMap.Builder<K, InstanceAction> builder = ImmutableMap.builder();
      Set<K> working = filterByStatus(instances, InstanceUpdateStatus.WORKING);
      Set<K> nextGroup = strategy.getNextGroup(idle, working);
      if (!nextGroup.isEmpty()) {
        for (K instance : nextGroup) {
          Result result = instances.get(instance).evaluate(stateProvider.getState(instance));
          Optional<InstanceAction> action = resultToAction(result);
          if (action.isPresent()) {
            builder.put(instance, action.get());
          }
        }
        LOG.info("Updated working set for update to "
            + filterByStatus(instances, InstanceUpdateStatus.WORKING));
      }

      Map<K, InstanceAction> actions = builder.build();
      if (!idle.isEmpty() && working.isEmpty() && actions.isEmpty()) {
        // There's no in-flight instances, and no actions - so there's nothing left to initiate more
        // work on this job. Try to find more work, or converge.
        return startNextInstanceGroup(stateProvider);
      } else {
        return actions;
      }
    }
  }

  private OneWayStatus computeJobUpdateStatus() {
    Set<K> idle = filterByStatus(instances, InstanceUpdateStatus.IDLE);
    Set<K> working = filterByStatus(instances, InstanceUpdateStatus.WORKING);
    Set<K> failed = filterByStatus(instances, InstanceUpdateStatus.FAILED);
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
    private final StateMachine<InstanceUpdateStatus> stateMachine;

    InstanceUpdate(String name, StateEvaluator<T> evaluator) {
      this.evaluator = requireNonNull(evaluator);
      stateMachine = StateMachine.<InstanceUpdateStatus>builder(name)
          .initialState(InstanceUpdateStatus.IDLE)
          .addState(InstanceUpdateStatus.IDLE, InstanceUpdateStatus.WORKING)
          .addState(
              InstanceUpdateStatus.WORKING,
              InstanceUpdateStatus.SUCCEEDED,
              InstanceUpdateStatus.FAILED)
          .addState(InstanceUpdateStatus.SUCCEEDED)
          .addState(InstanceUpdateStatus.FAILED)
          .throwOnBadTransition(true)
          .logTransitions()
          .build();
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

  @VisibleForTesting
  enum InstanceUpdateStatus {
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
   * Result of an evaluation round.
   */
  static class EvaluationResult<K> {
    private final OneWayStatus status;
    private final Map<K, InstanceAction> instanceActions;

    EvaluationResult(OneWayStatus jobStatus, Map<K, InstanceAction> instanceActions) {
      this.status = requireNonNull(jobStatus);
      this.instanceActions = requireNonNull(instanceActions);
    }

    public OneWayStatus getStatus() {
      return status;
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
      return other.getStatus().equals(this.getStatus())
          && other.getInstanceActions().equals(this.getInstanceActions());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getStatus(), getInstanceActions());
    }

    @Override
    public String toString() {
      return com.google.common.base.Objects.toStringHelper(this)
          .add("status", getStatus())
          .add("instanceActions", getInstanceActions())
          .toString();
    }
  }
}

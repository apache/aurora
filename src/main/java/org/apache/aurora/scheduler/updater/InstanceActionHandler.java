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

import java.util.Optional;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.storage.entities.IInstanceTaskConfig;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateInstructions;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IRange;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ISlaPolicy;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_FORWARD;
import static org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import static org.apache.aurora.scheduler.updater.Updates.getConfig;

interface InstanceActionHandler {

  Optional<Amount<Long, Time>> getReevaluationDelay(
      IInstanceKey instance,
      IJobUpdateInstructions instructions,
      MutableStoreProvider storeProvider,
      StateManager stateManager,
      UpdateAgentReserver reserver,
      JobUpdateStatus status,
      IJobUpdateKey key,
      SlaKillController slaKillController) throws UpdateStateException;

  Logger LOG = LoggerFactory.getLogger(InstanceActionHandler.class);

  static Optional<IScheduledTask> getExistingTask(
      MutableStoreProvider storeProvider,
      IInstanceKey instance) {

    return Optional.ofNullable(Iterables.getOnlyElement(
        storeProvider.getTaskStore().fetchTasks(Query.instanceScoped(instance).active()), null));
  }

  class AddTask implements InstanceActionHandler {
    private static ITaskConfig getTargetConfig(
        IJobUpdateInstructions instructions,
        boolean rollingForward,
        int instanceId) {

      if (rollingForward) {
        // Desired state is assumed to be non-null when AddTask is used.
        return instructions.getDesiredState().getTask();
      } else {
        for (IInstanceTaskConfig config : instructions.getInitialState()) {
          for (IRange range : config.getInstances()) {
            if (Range.closed(range.getFirst(), range.getLast()).contains(instanceId)) {
              return config.getTask();
            }
          }
        }

        throw new IllegalStateException("Failed to find instance " + instanceId);
      }
    }

    @Override
    public Optional<Amount<Long, Time>> getReevaluationDelay(
        IInstanceKey instance,
        IJobUpdateInstructions instructions,
        MutableStoreProvider storeProvider,
        StateManager stateManager,
        UpdateAgentReserver reserver,
        JobUpdateStatus status,
        IJobUpdateKey key,
        SlaKillController slaKillController) {

      Optional<IScheduledTask> task = getExistingTask(storeProvider, instance);
      if (task.isPresent()) {
        // Due to async event processing it's possible to have a race between task event
        // and instance addition. This is a perfectly valid case.
        LOG.info("Instance " + instance + " already exists while " + status);
      } else {
        LOG.info("Adding instance " + instance + " while " + status);
        ITaskConfig replacement = getTargetConfig(
            instructions,
            status == ROLLING_FORWARD,
            instance.getInstanceId());
        stateManager.insertPendingTasks(
            storeProvider,
            replacement,
            ImmutableSet.of(instance.getInstanceId()));
      }
      // A task state transition will trigger re-evaluation in this case, rather than a timer.
      return Optional.empty();
    }
  }

  class KillTask implements InstanceActionHandler {
    private final boolean reserveForReplacement;

    KillTask(boolean reserveForReplacement) {
      this.reserveForReplacement = reserveForReplacement;
    }

    @Override
    public Optional<Amount<Long, Time>> getReevaluationDelay(
        IInstanceKey instance,
        IJobUpdateInstructions instructions,
        MutableStoreProvider storeProvider,
        StateManager stateManager,
        UpdateAgentReserver reserver,
        JobUpdateStatus status,
        IJobUpdateKey key,
        SlaKillController slaKillController) throws UpdateStateException {

      Optional<IScheduledTask> task = getExistingTask(storeProvider, instance);
      if (task.isPresent()) {
        Optional<ISlaPolicy> slaPolicy = getSlaPolicy(instance, status, instructions);
        if (instructions.getSettings().isSlaAware() && slaPolicy.isPresent()) {
          slaKillController.slaKill(
              storeProvider,
              instance,
              task.get(),
              key,
              slaPolicy.get(),
              status,
              (MutableStoreProvider slaStoreProvider) -> killAndMaybeReserve(
                  instance,
                  slaStoreProvider,
                  stateManager,
                  reserver,
                  status,
                  key,
                  task.get()));
        } else {
          killAndMaybeReserve(
              instance,
              storeProvider,
              stateManager,
              reserver,
              status,
              key,
              task.get()
          );
        }
      } else {
        // Due to async event processing it's possible to have a race between task event
        // and it's deletion from the store. This is a perfectly valid case.
        LOG.info("No active instance " + instance + " to kill while " + status);
      }
      // A task state transition will trigger re-evaluation in this case, rather than a timer.
      return Optional.empty();
    }

    private void killAndMaybeReserve(
        IInstanceKey instance,
        MutableStoreProvider storeProvider,
        StateManager stateManager,
        UpdateAgentReserver reserver,
        JobUpdateStatus status,
        IJobUpdateKey key,
        IScheduledTask task) {

      LOG.info("Killing " + instance + " while " + status);
      stateManager.changeState(
          storeProvider,
          Tasks.id(task),
          Optional.empty(),
          ScheduleStatus.KILLING,
          Optional.of("Killed for job update " + key.getId()));
      if (reserveForReplacement && task.getAssignedTask().isSetSlaveId()) {
        reserver.reserve(task.getAssignedTask().getSlaveId(), instance);
      }
    }

    /**
     * Get the SLA policy that should be used to kill a task for an update. If the update is
     * {@link JobUpdateStatus#ROLLING_FORWARD}, then we use the config we are updating to. If the
     * update is {@link JobUpdateStatus#ROLLING_BACK}, then we use the config of the initial state.
     */
    private Optional<ISlaPolicy> getSlaPolicy(
        IInstanceKey instance,
        JobUpdateStatus status,
        IJobUpdateInstructions instructions) throws UpdateStateException {

      if (status == ROLLING_FORWARD) {
        // It is possible that an update only removes instances. In this case, there is no desired
        // state. Otherwise, get the task associated (this should never be null) and return an
        // optional of the SlaPolicy of the task (or empty if null).
        return Optional
            .ofNullable(instructions.getDesiredState())
            .map(desiredState -> desiredState.getTask().getSlaPolicy());
      } else if (status == ROLLING_BACK) {
        return getConfig(instance.getInstanceId(), instructions.getInitialState())
            .map(ITaskConfig::getSlaPolicy);
      } else {
        // This should not happen as there checks before this method is called, but we throw an
        // exception just in case.
        LOG.error("Attempted to perform an SLA-aware kill on instance {} while update is not "
            + "in an active state (it is in state {})", instance, status);
        throw new UpdateStateException("Attempted to perform an instance update action while not "
            + "in an active state.");
      }
    }
  }

  class WatchRunningTask implements InstanceActionHandler {
    @Override
    public Optional<Amount<Long, Time>> getReevaluationDelay(
        IInstanceKey instance,
        IJobUpdateInstructions instructions,
        MutableStoreProvider storeProvider,
        StateManager stateManager,
        UpdateAgentReserver reserver,
        JobUpdateStatus status,
        IJobUpdateKey key,
        SlaKillController slaKillController) {

      return Optional.of(Amount.of(
          (long) instructions.getSettings().getMinWaitInInstanceRunningMs(),
          Time.MILLISECONDS));
    }
  }
}

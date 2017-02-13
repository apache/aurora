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
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_FORWARD;
import static org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;

interface InstanceActionHandler {

  Optional<Amount<Long, Time>> getReevaluationDelay(
      IInstanceKey instance,
      IJobUpdateInstructions instructions,
      MutableStoreProvider storeProvider,
      StateManager stateManager,
      JobUpdateStatus status,
      IJobUpdateKey key);

  Logger LOG = LoggerFactory.getLogger(InstanceActionHandler.class);

  static Optional<IScheduledTask> getExistingTask(
      MutableStoreProvider storeProvider,
      IInstanceKey instance) {

    return Optional.fromNullable(Iterables.getOnlyElement(
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
        JobUpdateStatus status,
        IJobUpdateKey key) {

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
      return Optional.absent();
    }
  }

  class KillTask implements InstanceActionHandler {
    @Override
    public Optional<Amount<Long, Time>> getReevaluationDelay(
        IInstanceKey instance,
        IJobUpdateInstructions instructions,
        MutableStoreProvider storeProvider,
        StateManager stateManager,
        JobUpdateStatus status,
        IJobUpdateKey key) {

      Optional<IScheduledTask> task = getExistingTask(storeProvider, instance);
      if (task.isPresent()) {
        LOG.info("Killing " + instance + " while " + status);
        stateManager.changeState(
            storeProvider,
            Tasks.id(task.get()),
            Optional.absent(),
            ScheduleStatus.KILLING,
            Optional.of("Killed for job update " + key.getId()));
      } else {
        // Due to async event processing it's possible to have a race between task event
        // and it's deletion from the store. This is a perfectly valid case.
        LOG.info("No active instance " + instance + " to kill while " + status);
      }
      // A task state transition will trigger re-evaluation in this case, rather than a timer.
      return Optional.absent();
    }
  }

  class WatchRunningTask implements InstanceActionHandler {
    @Override
    public Optional<Amount<Long, Time>> getReevaluationDelay(
        IInstanceKey instance,
        IJobUpdateInstructions instructions,
        MutableStoreProvider storeProvider,
        StateManager stateManager,
        JobUpdateStatus status,
        IJobUpdateKey key) {

      return Optional.of(Amount.of(
          (long) instructions.getSettings().getMinWaitInInstanceRunningMs(),
          Time.MILLISECONDS));
    }
  }
}

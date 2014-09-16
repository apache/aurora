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

import java.util.logging.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.storage.entities.IInstanceTaskConfig;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateConfiguration;
import org.apache.aurora.scheduler.storage.entities.IRange;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_FORWARD;

interface InstanceActionHandler {

  Amount<Long, Time> getReevaluationDelay(
      IInstanceKey instance,
      IJobUpdateConfiguration updateConfig,
      TaskStore taskStore,
      StateManager stateManager,
      JobUpdateStatus status);

  Logger LOG = Logger.getLogger(InstanceActionHandler.class.getName());

  class AddTask implements InstanceActionHandler {
    private static ITaskConfig getTargetConfig(
        IJobUpdateConfiguration configuration,
        boolean rollingForward,
        int instanceId) {

      if (rollingForward) {
        return configuration.getNewTaskConfig();
      } else {
        for (IInstanceTaskConfig config : configuration.getOldTaskConfigs()) {
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
    public Amount<Long, Time> getReevaluationDelay(
        IInstanceKey instance,
        IJobUpdateConfiguration updateConfig,
        TaskStore taskStore,
        StateManager stateManager,
        JobUpdateStatus status) {

      // TODO(wfarner): This skips quota validation.  Either check quota here, or augment
      // quota checking to take updates into consideration (AURORA-686).
      LOG.info("Adding instance " + instance + " while " + status);
      ITaskConfig replacement = getTargetConfig(
          updateConfig,
          status == ROLLING_FORWARD,
          instance.getInstanceId());
      stateManager.insertPendingTasks(replacement, ImmutableSet.of(instance.getInstanceId()));
      return  Amount.of(
          (long) updateConfig.getSettings().getMaxWaitToInstanceRunningMs(),
          Time.MILLISECONDS);
    }
  }

  class KillTask implements InstanceActionHandler {
    @Override
    public Amount<Long, Time> getReevaluationDelay(
        IInstanceKey instance,
        IJobUpdateConfiguration updateConfig,
        TaskStore taskStore,
        StateManager stateManager,
        JobUpdateStatus status) {

      String taskId = Tasks.id(Iterables.getOnlyElement(
          taskStore.fetchTasks(Query.instanceScoped(instance).active())));
      LOG.info("Killing " + instance + " while " + status);
      stateManager.changeState(
          taskId,
          Optional.<ScheduleStatus>absent(),
          ScheduleStatus.KILLING,
          Optional.of("Killed for job update."));
      return Amount.of(
          (long) updateConfig.getSettings().getMaxWaitToInstanceRunningMs(),
          Time.MILLISECONDS);
    }
  }

  class WatchRunningTask implements InstanceActionHandler {
    @Override
    public Amount<Long, Time> getReevaluationDelay(
        IInstanceKey instance,
        IJobUpdateConfiguration updateConfig,
        TaskStore taskStore,
        StateManager stateManager,
        JobUpdateStatus status) {

      return Amount.of(
          (long) updateConfig.getSettings().getMinWaitInInstanceRunningMs(),
          Time.MILLISECONDS);
    }
  }
}

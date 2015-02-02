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
package org.apache.aurora.scheduler.async.preemptor;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.eventbus.Subscribe;

import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;

/**
 * A cached view of cluster state, kept up to date by pubsub notifications.
 */
public class CachedClusterState implements ClusterState, PubsubEvent.EventSubscriber {

  private final Multimap<String, PreemptionVictim> victims =
      Multimaps.synchronizedMultimap(HashMultimap.<String, PreemptionVictim>create());

  @Override
  public Multimap<String, PreemptionVictim> getSlavesToActiveTasks() {
    return Multimaps.unmodifiableMultimap(victims);
  }

  @Subscribe
  public void taskChangedState(TaskStateChange stateChange) {
    synchronized (victims) {
      String slaveId = stateChange.getTask().getAssignedTask().getSlaveId();
      PreemptionVictim victim = PreemptionVictim.fromTask(stateChange.getTask().getAssignedTask());
      if (Tasks.SLAVE_ASSIGNED_STATES.contains(stateChange.getNewState())) {
        victims.put(slaveId, victim);
      } else {
        victims.remove(slaveId, victim);
      }
    }
  }
}

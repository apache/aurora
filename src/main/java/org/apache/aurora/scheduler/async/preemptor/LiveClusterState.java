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

import java.util.EnumSet;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.gen.ScheduleStatus.PREEMPTING;
import static org.apache.aurora.scheduler.base.Tasks.SCHEDULED_TO_ASSIGNED;

class LiveClusterState implements ClusterState {

  @VisibleForTesting
  static final Query.Builder CANDIDATE_QUERY = Query.statusScoped(
      EnumSet.copyOf(Sets.difference(Tasks.SLAVE_ASSIGNED_STATES, EnumSet.of(PREEMPTING))));

  private final Storage storage;

  @Inject
  LiveClusterState(Storage storage) {
    this.storage = requireNonNull(storage);
  }

  @Override
  public Multimap<String, PreemptionVictim> getSlavesToActiveTasks() {
    // Only non-pending active tasks may be preempted.
    Iterable<IAssignedTask> activeTasks = Iterables.transform(
        Storage.Util.fetchTasks(storage, CANDIDATE_QUERY),
        SCHEDULED_TO_ASSIGNED);

    // Group the tasks by slave id so they can be paired with offers from the same slave.
    // Choosing to do this iteratively instead of using Multimaps.index/transform to avoid
    // generating a very large intermediate map.
    ImmutableMultimap.Builder<String, PreemptionVictim> tasksBySlave = ImmutableMultimap.builder();
    for (IAssignedTask task : activeTasks) {
      tasksBySlave.put(task.getSlaveId(), PreemptionVictim.fromTask(task));
    }
    return tasksBySlave.build();
  }
}

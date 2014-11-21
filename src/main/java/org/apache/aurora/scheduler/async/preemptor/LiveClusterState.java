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
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
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
  static final Function<IAssignedTask, String> TASK_TO_SLAVE_ID =
      new Function<IAssignedTask, String>() {
        @Override
        public String apply(IAssignedTask input) {
          return input.getSlaveId();
        }
      };

  @VisibleForTesting
  static final Query.Builder CANDIDATE_QUERY = Query.statusScoped(
      EnumSet.copyOf(Sets.difference(Tasks.SLAVE_ASSIGNED_STATES, EnumSet.of(PREEMPTING))));

  private final Storage storage;

  @Inject
  LiveClusterState(Storage storage) {
    this.storage = requireNonNull(storage);
  }

  @Override
  public Multimap<String, IAssignedTask> getSlavesToActiveTasks() {
    // Only non-pending active tasks may be preempted.
    Iterable<IAssignedTask> activeTasks = Iterables.transform(
        Storage.Util.consistentFetchTasks(storage, CANDIDATE_QUERY),
        SCHEDULED_TO_ASSIGNED);

    // Group the tasks by slave id so they can be paired with offers from the same slave.
    return Multimaps.index(activeTasks, TASK_TO_SLAVE_ID);
  }
}

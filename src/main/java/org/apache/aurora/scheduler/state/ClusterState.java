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

import com.google.common.collect.Multimap;

import org.apache.aurora.scheduler.preemptor.PreemptionVictim;

/**
 * The current state of scheduled tasks within the cluster.
 */
public interface ClusterState {

  /**
   * Gets a snapshot of the active tasks in the cluster, indexed by the slave IDs they are
   * assigned to.
   *
   * @return Active tasks and their associated slave IDs.
   */
  Multimap<String, PreemptionVictim> getSlavesToActiveTasks();
}

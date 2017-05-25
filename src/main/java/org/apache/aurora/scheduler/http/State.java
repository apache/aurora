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
package org.apache.aurora.scheduler.http;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;

import org.apache.aurora.scheduler.preemptor.ClusterState;
import org.apache.aurora.scheduler.preemptor.ClusterStateImpl;
import org.apache.aurora.scheduler.preemptor.PreemptionVictim;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import static org.apache.aurora.scheduler.http.api.GsonMessageBodyHandler.GSON;

/**
 * Servlet that exposes cluster state as JSON.
 */
@Path("/state")
public class State {
  private final ClusterState clusterState;

  @VisibleForTesting
  static String taskKey(ITaskConfig config) {
    return String.format("%s/%s/%s-%d",
        config.getJob().getRole(),
        config.getJob().getEnvironment(),
        config.getJob().getName(),
        config.hashCode());
  }

  /**
   * Class that normalizes the cluster state by removing instance-specific information and moving
   * tasks into a lookup table. This reduces the total size of the payload from O(RUNNING_TASKS) to
   * O(DISTINCT_RUNNING_TASK_CONFIGS).
   */
  private static class NormalizedClusterState {
    private final Map<String, ITaskConfig> taskConfigs;
    private final Map<String, List<String>> agents;

    NormalizedClusterState(
        Map<String, ITaskConfig> taskConfigMap,
        Map<String, List<String>> agentTasksMap) {

      this.taskConfigs = requireNonNull(taskConfigMap);
      this.agents = requireNonNull(agentTasksMap);
    }

    static NormalizedClusterState fromClusterState(Multimap<String, PreemptionVictim> state) {
      Map<String, ITaskConfig> tasks = new HashMap<>();
      ImmutableMap.Builder<String, List<String>> agents = new ImmutableMap.Builder<>();
      for (Entry<String, Collection<PreemptionVictim>> entry: state.asMap().entrySet()) {
        for (PreemptionVictim victim: entry.getValue()) {
          tasks.putIfAbsent(taskKey(victim.getConfig()), victim.getConfig());
        }
        agents.put(
            entry.getKey(),
            entry.getValue().stream().map(e -> taskKey(e.getConfig())).collect(toList()));
      }
      return new NormalizedClusterState(tasks, agents.build());
    }
  }

  @Inject
  State(ClusterStateImpl clusterState) {
    this.clusterState = requireNonNull(clusterState);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getState() {
    return Response.ok(GSON.toJson(NormalizedClusterState.fromClusterState(
        clusterState.getSlavesToActiveTasks()))).build();
  }
}

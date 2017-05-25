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

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.preemptor.ClusterStateImpl;
import org.apache.aurora.scheduler.preemptor.PreemptionVictim;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.http.State.taskKey;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class StateTest extends EasyMockTest {

  private ClusterStateImpl clusterState;
  private State state;

  @Before
  public void setUp() {
    clusterState = createMock(ClusterStateImpl.class);
    state = new State(clusterState);
  }

  private Multimap<String, PreemptionVictim> createState(IAssignedTask... tasks) {
    ImmutableMultimap.Builder<String, PreemptionVictim> victims = ImmutableSetMultimap.builder();
    for (IAssignedTask task : tasks) {
      victims.put(task.getSlaveId(), PreemptionVictim.fromTask(task));
    }
    return victims.build();
  }

  private IAssignedTask makeTask(String taskId, String agentId, TaskConfig taskConfig) {
    return IAssignedTask.build(new AssignedTask()
        .setSlaveId(agentId)
        .setSlaveHost(agentId + "-host")
        .setTaskId(taskId)
        .setTask(taskConfig));
  }

  private static List<String> getAgentTasks(JsonNode tasks) {
    return ImmutableList.copyOf(Iterators.transform(tasks.elements(), JsonNode::asText));
  }

  @Test
  public void testJson() throws Exception {
    TaskConfig task1 = new TaskConfig().setJob(new JobKey("role", "env", "job"));
    TaskConfig task2 = new TaskConfig().setJob(new JobKey("role", "env", "job"));
    TaskConfig task3 = new TaskConfig().setJob(new JobKey("role", "env", "another-job"));
    String task1Key = taskKey(ITaskConfig.build(task1));
    String task2Key = taskKey(ITaskConfig.build(task2));
    String task3Key = taskKey(ITaskConfig.build(task3));

    // Tests:
    // Same task config on multiple hosts.
    // Different tasks configs for the same job.
    // Multiple job keys.
    Multimap<String, PreemptionVictim> expected = createState(
        makeTask("task1", "agent1", task1),
        makeTask("task2", "agent1", task1),
        makeTask("task3", "agent2", task2),
        makeTask("task4", "agent3", task3),
        makeTask("task5", "agent2", task1));

    expect(clusterState.getSlavesToActiveTasks()).andReturn(expected);
    control.replay();

    JsonNode result = new ObjectMapper().readTree((String) state.getState().getEntity());
    ObjectNode tasks = (ObjectNode) result.get("taskConfigs");

    assertEquals(ImmutableSet.of(task1Key, task2Key, task3Key),
        ImmutableSet.copyOf(tasks.fieldNames()));

    JsonNode agents = result.get("agents");
    assertEquals(ImmutableList.of(task1Key, task1Key), getAgentTasks(agents.get("agent1")));
    assertEquals(ImmutableList.of(task2Key, task1Key), getAgentTasks(agents.get("agent2")));
    assertEquals(ImmutableList.of(task3Key), getAgentTasks(agents.get("agent3")));
  }
}

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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.preemptor.ClusterStateImpl;
import org.apache.aurora.scheduler.preemptor.PreemptionVictim;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.junit.Before;
import org.junit.Test;

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

  private IAssignedTask makeTask(String taskId, String agentId) {
    return IAssignedTask.build(new AssignedTask()
        .setSlaveId(agentId)
        .setSlaveHost(agentId + "-host")
        .setTaskId(taskId)
        .setTask(new TaskConfig().setJob(new JobKey("role", "env", "job"))));
  }

  @Test
  public void testJson() throws Exception {
    Multimap<String, PreemptionVictim> expected = createState(
        makeTask("task1", "agent1"),
        makeTask("task2", "agent1"),
        makeTask("task3", "agent2"),
        makeTask("task4", "agent3"));
    expect(clusterState.getSlavesToActiveTasks()).andReturn(expected);
    control.replay();

    JsonNode result = new ObjectMapper().readTree((String) state.getState().getEntity());
    assertEquals(ImmutableList.of("agent1", "agent2", "agent3"),
        ImmutableList.copyOf(result.fieldNames()));
    assertEquals(2, ((ArrayNode) result.get("agent1")).size());
    assertEquals(1, ((ArrayNode) result.get("agent2")).size());
    assertEquals(1, ((ArrayNode) result.get("agent3")).size());
  }
}

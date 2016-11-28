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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.testing.FakeTicker;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.metadata.NearestFit;
import org.apache.aurora.scheduler.scheduling.TaskGroup;
import org.apache.aurora.scheduler.scheduling.TaskGroups;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

public class PendingTasksTest extends EasyMockTest {

  private TaskGroups pendingTaskGroups;
  private NearestFit nearestFit;

  @Before
  public void setUp() {
    pendingTaskGroups = createMock(TaskGroups.class);
    nearestFit = new NearestFit(new FakeTicker());
  }

  /**
   * Create a {@link JsonNode} object to mimic the response.
   *
   * @param penaltyMs
   * @param taskIds
   * @param name
   * @param reasons
   * @return Json node for pending tasks whose values are initialized to the provided values.
   * @throws IOException
   */
  private JsonNode getMimicResponseJson(
      long penaltyMs, String[] taskIds, String name, List<String> reasons) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode mutablePendingTaskJson = mapper.createObjectNode();
    // Adding the key=value pairs to mutablePendingTaskJson.
    mutablePendingTaskJson.put("penaltyMs", penaltyMs);
    mutablePendingTaskJson.putArray("taskIds");
    for (String taskId : taskIds) {
      ((ArrayNode) mutablePendingTaskJson.get("taskIds")).add(taskId);
    }
    mutablePendingTaskJson.put("name", name);
    mutablePendingTaskJson.put("reason", reasons.toString());
    return mutablePendingTaskJson;
  }

  @Test
  public void testNoOffers() throws IOException {
    // Making a task that is not in PENDING state.
    IJobKey jobKey = IJobKey.build(new JobKey("role", "test", "nonPendingJob"));
    IScheduledTask task = TestUtils.makeTask(jobKey, "task0", 0,
        ScheduleStatus.ASSIGNED, 10, 10, 10);

    PubsubEvent.TaskStateChange taskStateChange = PubsubEvent.TaskStateChange.transition(
        task, ScheduleStatus.INIT);

    pendingTaskGroups.taskChangedState(taskStateChange);
    expectLastCall();

    // Recording the return value of pendingTaskGroups.getGroups().
    List<TaskGroup> taskGroupList = new ArrayList<>();
    expect(pendingTaskGroups.getGroups()).andReturn(taskGroupList).anyTimes();

    replay(pendingTaskGroups);

    // Testing.
    pendingTaskGroups.taskChangedState(taskStateChange);
    PendingTasks pendingTasks = new PendingTasks(pendingTaskGroups, nearestFit);
    JsonNode mimicResponseNoPendingTaskJson = new ObjectMapper().createArrayNode();
    JsonNode actualResponseJson = new ObjectMapper().valueToTree(
        pendingTasks.getOffers().getEntity());
    assertEquals(mimicResponseNoPendingTaskJson, actualResponseJson);
  }

  @Test
  public void testOffers() throws IOException {
    // Making pending tasks.
    IJobKey jobKey0 = IJobKey.build(new JobKey("role", "test", "jobA"));
    IJobKey jobKey1 = IJobKey.build(new JobKey("role", "test", "jobB"));
    IScheduledTask task0 = TestUtils.makeTask(jobKey0, "task0", 0,
        ScheduleStatus.PENDING, 1000, 1000000, 10);
    IScheduledTask task1 = TestUtils.makeTask(jobKey1, "task1", 0,
        ScheduleStatus.PENDING, 1000, 10, 1000000);

    PubsubEvent.TaskStateChange taskStateChange0 = PubsubEvent.TaskStateChange.transition(
        task0, ScheduleStatus.INIT);
    PubsubEvent.TaskStateChange taskStateChange1 = PubsubEvent.TaskStateChange.transition(
        task1, ScheduleStatus.INIT);

    pendingTaskGroups.taskChangedState(taskStateChange0);
    pendingTaskGroups.taskChangedState(taskStateChange1);
    expectLastCall();

    // Recording the return value of pendingTaskGroups.getGroups().
    TaskGroupKey taskGroupKey0 = TaskGroupKey.from(task0.getAssignedTask().getTask());
    TaskGroupKey taskGroupKey1 = TaskGroupKey.from(task1.getAssignedTask().getTask());
    TaskGroup taskGroup0 = new TaskGroup(taskGroupKey0, "task0");
    TaskGroup taskGroup1 = new TaskGroup(taskGroupKey1, "task1");
    List<TaskGroup> taskGroupList = new ArrayList<>();
    taskGroupList.add(taskGroup0);
    taskGroupList.add(taskGroup1);
    expect(pendingTaskGroups.getGroups()).andReturn(taskGroupList).anyTimes();

    // Creating vetoes for CPU and RAM, corresponding to task0.
    ImmutableSet<Veto> vetoes = ImmutableSet.<Veto>builder()
        .add(Veto.insufficientResources("CPU", 1))
        .add(Veto.insufficientResources("RAM", 1)).build();
    nearestFit.vetoed(new PubsubEvent.Vetoed(taskGroupKey0, vetoes));
    // Creating vetoes for CPU and DISK, corresponding to task1.
    ImmutableSet<Veto> vetoes1 = ImmutableSet.<Veto>builder()
        .add(Veto.insufficientResources("CPU", 1))
        .add(Veto.insufficientResources("DISK", 1)).build();
    nearestFit.vetoed(new PubsubEvent.Vetoed(taskGroupKey1, vetoes1));
    replay(pendingTaskGroups);

    // Testing.
    pendingTaskGroups.taskChangedState(taskStateChange0);
    pendingTaskGroups.taskChangedState(taskStateChange1);
    PendingTasks pendingTasks0 = new PendingTasks(pendingTaskGroups, nearestFit);
    String[] taskIds0 = {"task0"};
    String[] taskIds1 = {"task1"};
    String[] reasonsArr0 = {"Insufficient: CPU", "Insufficient: RAM"};
    String[] reasonsArr1 = {"Insufficient: CPU", "Insufficient: DISK"};
    List<String> reasons0 = Arrays.stream(reasonsArr0).collect(Collectors.toList());
    List<String> reasons1 = Arrays.stream(reasonsArr1).collect(Collectors.toList());
    JsonNode mimicResponseTwoPendingTasksJson = new ObjectMapper().createArrayNode();
    JsonNode mimicJson0 = getMimicResponseJson(0, taskIds0, "role/test/jobA", reasons0);
    JsonNode mimicJson1 = getMimicResponseJson(0, taskIds1, "role/test/jobB", reasons1);
    ((ArrayNode) mimicResponseTwoPendingTasksJson).add(mimicJson0);
    ((ArrayNode) mimicResponseTwoPendingTasksJson).add(mimicJson1);
    JsonNode actualResponseJson = new ObjectMapper().valueToTree(
        pendingTasks0.getOffers().getEntity());
    assertEquals(mimicResponseTwoPendingTasksJson, actualResponseJson);
  }
}

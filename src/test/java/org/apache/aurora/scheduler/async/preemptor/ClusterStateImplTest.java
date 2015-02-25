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
import com.google.common.collect.ImmutableMultimap;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.ASSIGNED;
import static org.apache.aurora.gen.ScheduleStatus.FAILED;
import static org.apache.aurora.gen.ScheduleStatus.FINISHED;
import static org.apache.aurora.gen.ScheduleStatus.KILLED;
import static org.apache.aurora.gen.ScheduleStatus.KILLING;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.gen.ScheduleStatus.THROTTLED;
import static org.junit.Assert.assertEquals;

public class ClusterStateImplTest {

  private ClusterStateImpl state;

  @Before
  public void setUp() {
    state = new ClusterStateImpl();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testImmutable() {
    state.getSlavesToActiveTasks().clear();
  }

  @Test
  public void testTaskLifecycle() {
    IAssignedTask a = makeTask("a", "s1");

    assertVictims();
    changeState(a, THROTTLED);
    assertVictims();
    changeState(a, PENDING);
    assertVictims();
    changeState(a, ASSIGNED);
    assertVictims(a);
    changeState(a, RUNNING);
    assertVictims(a);
    changeState(a, KILLING);
    assertVictims(a);
    changeState(a, FINISHED);
    assertVictims();
  }

  @Test
  public void testTaskChangesSlaves() {
    // We do not intend to handle the case of an external failure leading to the same task ID
    // on a different slave.
    IAssignedTask a = makeTask("a", "s1");
    IAssignedTask a1 = makeTask("a", "s2");
    changeState(a, RUNNING);
    changeState(a1, RUNNING);
    assertVictims(a, a1);
  }

  @Test
  public void testMultipleTasks() {
    IAssignedTask a = makeTask("a", "s1");
    IAssignedTask b = makeTask("b", "s1");
    IAssignedTask c = makeTask("c", "s2");
    IAssignedTask d = makeTask("d", "s3");
    IAssignedTask e = makeTask("e", "s3");
    IAssignedTask f = makeTask("f", "s1");
    changeState(a, RUNNING);
    assertVictims(a);
    changeState(b, RUNNING);
    assertVictims(a, b);
    changeState(c, RUNNING);
    assertVictims(a, b, c);
    changeState(d, RUNNING);
    assertVictims(a, b, c, d);
    changeState(e, RUNNING);
    assertVictims(a, b, c, d, e);
    changeState(c, FINISHED);
    assertVictims(a, b, d, e);
    changeState(a, FAILED);
    changeState(e, KILLED);
    assertVictims(b, d);
    changeState(f, RUNNING);
    assertVictims(b, d, f);
  }

  private void assertVictims(IAssignedTask... tasks) {
    ImmutableMultimap.Builder<String, PreemptionVictim> victims = ImmutableMultimap.builder();
    for (IAssignedTask task : tasks) {
      victims.put(task.getSlaveId(), PreemptionVictim.fromTask(task));
    }
    assertEquals(HashMultimap.create(victims.build()), state.getSlavesToActiveTasks());
  }

  private IAssignedTask makeTask(String taskId, String slaveId) {
    return IAssignedTask.build(new AssignedTask()
        .setTaskId(taskId)
        .setSlaveId(slaveId)
        .setSlaveHost(slaveId + "host")
        .setTask(new TaskConfig()
            .setOwner(new Identity().setRole("role"))));
  }

  private void changeState(IAssignedTask assignedTask, ScheduleStatus status) {
    IScheduledTask task = IScheduledTask.build(new ScheduledTask()
        .setStatus(status)
        .setAssignedTask(assignedTask.newBuilder()));
    state.taskChangedState(TaskStateChange.transition(task, ScheduleStatus.INIT));
  }
}

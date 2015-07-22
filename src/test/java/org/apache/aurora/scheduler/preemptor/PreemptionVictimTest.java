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
package org.apache.aurora.scheduler.preemptor;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class PreemptionVictimTest {

  @Test
  public void testBeanMethods() {
    PreemptionVictim a = makeVictim("a");
    PreemptionVictim a1 = makeVictim("a");
    PreemptionVictim b = makeVictim("b");
    assertEquals(a, a1);
    assertEquals(a.hashCode(), a1.hashCode());
    assertEquals(a.toString(), a1.toString());
    assertNotEquals(a, b);
    assertNotEquals(a.toString(), b.toString());
    assertEquals(ImmutableSet.of(a, b), ImmutableSet.of(a, a1, b));
  }

  private PreemptionVictim makeVictim(String taskId) {
    return PreemptionVictim.fromTask(IAssignedTask.build(new AssignedTask()
        .setTaskId(taskId)
        .setSlaveId(taskId + "slave")
        .setSlaveHost(taskId + "host")
        .setTask(new TaskConfig().setJob(new JobKey("role", "env", "job")))));
  }
}

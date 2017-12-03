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
package org.apache.aurora.scheduler.storage.durability;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.RemoveTasks;
import org.apache.aurora.gen.storage.SaveTasks;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TransactionRecorderTest {
  @Test
  public void testCoalesce() throws Exception {
    // No coalescing - different operation types.
    assertEquals(
        ImmutableList.of(
            Op.removeTasks(createRemoveTasks("1", "2")),
            Op.saveTasks(createSaveTasks("4", "5"))),
        record(
            Op.removeTasks(createRemoveTasks("1", "2")),
            Op.saveTasks(createSaveTasks("4", "5"))));

    assertEquals(
        ImmutableList.of(Op.removeTasks(createRemoveTasks("1", "2", "3", "4"))),
        record(
            Op.removeTasks(createRemoveTasks("1", "2")),
            Op.removeTasks(createRemoveTasks("3", "4"))));

    assertEquals(
        ImmutableList.of(Op.saveTasks(createSaveTasks("3", "2", "1"))),
        record(Op.saveTasks(createSaveTasks("1", "2")), Op.saveTasks(createSaveTasks("1", "3"))));

    assertEquals(
        ImmutableList.of(Op.removeTasks(createRemoveTasks("3", "4", "5"))),
        record(
            Op.removeTasks(createRemoveTasks("3")),
            Op.removeTasks(createRemoveTasks("4", "5"))));
  }

  private static List<Op> record(Op... ops) {
    TransactionRecorder recorder = new TransactionRecorder();
    Stream.of(ops).forEach(recorder::add);
    return recorder.getOps();
  }

  private static SaveTasks createSaveTasks(String... taskIds) {
    return new SaveTasks().setTasks(
        Stream.of(taskIds)
            .map(id -> new ScheduledTask().setAssignedTask(new AssignedTask().setTaskId(id)))
            .collect(Collectors.toSet())
    );
  }

  private RemoveTasks createRemoveTasks(String... taskIds) {
    return new RemoveTasks(ImmutableSet.copyOf(taskIds));
  }
}

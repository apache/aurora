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
package org.apache.aurora.scheduler.storage.log;

import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.storage.DeduplicatedScheduledTask;
import org.apache.aurora.gen.storage.DeduplicatedSnapshot;
import org.apache.aurora.gen.storage.SchedulerMetadata;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.scheduler.storage.log.SnapshotDeduplicator.SnapshotDeduplicatorImpl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SnapshotDeduplicatorImplTest {
  private final SnapshotDeduplicator snapshotDeduplicator = new SnapshotDeduplicatorImpl();

  private final Map<String, TaskConfig> taskIdToConfig = ImmutableMap.of(
      "task1", makeConfig("a"),
      "task2", makeConfig("a"),
      "task3", makeConfig("b"));

  private TaskConfig makeConfig(String data) {
    return new TaskConfig()
        .setExecutorConfig(new ExecutorConfig()
            .setData(data));
  }

  private ScheduledTask makeTask(String taskId, TaskConfig config) {
    return new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setTaskId(taskId)
            .setTask(config));
  }

  private Snapshot makeSnapshot() {
    Snapshot snapshot = new Snapshot()
        .setSchedulerMetadata(new SchedulerMetadata()
            .setFrameworkId("test"));

    for (Entry<String, TaskConfig> entry : taskIdToConfig.entrySet()) {
      snapshot.addToTasks(makeTask(entry.getKey(), entry.getValue()));
    }

    return snapshot;
  }

  @Test
  public void testRoundTrip() throws Exception {
    Snapshot snapshot = makeSnapshot();

    assertEquals(
        snapshot,
        snapshotDeduplicator.reduplicate(snapshotDeduplicator.deduplicate(snapshot)));
  }

  @Test
  public void testDeduplicatedFormat() {
    DeduplicatedSnapshot deduplicatedSnapshot = snapshotDeduplicator.deduplicate(makeSnapshot());

    assertEquals(
        "The tasks field of the partial snapshot should be empty.",
        0,
        deduplicatedSnapshot.getPartialSnapshot().getTasksSize());

    assertEquals(
        "The total number of task configs should be equal to the number of unique task configs.",
        2,
        deduplicatedSnapshot.getTaskConfigsSize());

    assertEquals(
        ImmutableSet.of(makeConfig("a"), makeConfig("b")),
        ImmutableSet.copyOf(deduplicatedSnapshot.getTaskConfigs()));

    for (DeduplicatedScheduledTask task : deduplicatedSnapshot.getPartialTasks()) {
      assertEquals(
          "The deduplicated task should have the correct index into the taskConfigs table.",
          taskIdToConfig.get(task.getPartialScheduledTask().getAssignedTask().getTaskId()),
          deduplicatedSnapshot.getTaskConfigs().get(task.getTaskConfigId()));

      assertNull(
          "The task config field of partial scheduled tasks should be null.",
          task.getPartialScheduledTask().getAssignedTask().getTask());
    }
  }

  @Test(expected = CodingException.class)
  public void testReduplicateFailure() throws Exception {
    DeduplicatedSnapshot corrupt = new DeduplicatedSnapshot()
        .setPartialSnapshot(new Snapshot().setSchedulerMetadata(new SchedulerMetadata()))
        .setPartialTasks(ImmutableList.of(
            new DeduplicatedScheduledTask()
                .setPartialScheduledTask(new ScheduledTask())
                .setTaskConfigId(1)))
        .setTaskConfigs(ImmutableList.of(new TaskConfig()));

    snapshotDeduplicator.reduplicate(corrupt);
  }

  @Test
  public void testEmptyRoundTrip() throws Exception {
    Snapshot snapshot = new Snapshot();

    assertEquals(
        snapshot,
        snapshotDeduplicator.reduplicate(snapshotDeduplicator.deduplicate(snapshot)));
  }
}

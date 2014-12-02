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

import com.google.common.collect.ImmutableMultimap;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LiveClusterStateTest extends EasyMockTest {

  private StorageTestUtil storageUtil;
  private ClusterState clusterState;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    clusterState = new LiveClusterState(storageUtil.storage);
  }

  @Test
  public void testEmptyStorage() {
    storageUtil.expectTaskFetch(LiveClusterState.CANDIDATE_QUERY);

    control.replay();

    assertEquals(
        ImmutableMultimap.<String, PreemptionVictim>of(),
        clusterState.getSlavesToActiveTasks());
  }

  private IScheduledTask makeTask(String taskId, String slaveId) {
    return IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setTaskId(taskId)
            .setSlaveId(slaveId)
            .setSlaveHost(slaveId + "-host")
            .setTask(new TaskConfig()
                .setOwner(new Identity("owner", "role"))
                .setNumCpus(1)
                .setRamMb(1)
                .setDiskMb(1))));
  }

  private static PreemptionVictim fromTask(IScheduledTask task) {
    return PreemptionVictim.fromTask(task.getAssignedTask());
  }

  @Test
  public void testGetActiveTasks() {
    IScheduledTask a = makeTask("a", "1");
    IScheduledTask b = makeTask("b", "1");
    IScheduledTask c = makeTask("c", "2");

    storageUtil.expectTaskFetch(LiveClusterState.CANDIDATE_QUERY, a, b, c);

    control.replay();

    assertEquals(
        ImmutableMultimap.<String, PreemptionVictim>builder()
            .putAll("1", fromTask(a), fromTask(b))
            .putAll("2", fromTask(c))
            .build(),
        clusterState.getSlavesToActiveTasks());
  }
}

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

import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult.Quiet;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.mem.MemStorageModule;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.MaintenanceMode.DRAINED;
import static org.apache.aurora.gen.MaintenanceMode.DRAINING;
import static org.apache.aurora.gen.MaintenanceMode.SCHEDULED;
import static org.junit.Assert.assertEquals;

public class MaintenanceTest {

  private Storage storage;
  private Maintenance maintenance;

  @Before
  public void setUp() {
    storage = MemStorageModule.newEmptyStorage();
    maintenance = new Maintenance(storage);
  }

  @Test
  public void testNoDrainingHosts() {
    // Test for regression of AURORA-1652.

    storage.write((Quiet) storeProvider -> {
      ScheduledTask pending = TaskTestUtil.makeTask("a", TaskTestUtil.JOB)
          .newBuilder()
          .setStatus(ScheduleStatus.PENDING);

      storeProvider.getAttributeStore().saveHostAttributes(IHostAttributes.build(
          new HostAttributes()
              .setHost("b")
              .setSlaveId("b")
              .setMode(MaintenanceMode.NONE)
      ));
      ScheduledTask assigned = TaskTestUtil.makeTask("b", TaskTestUtil.JOB)
          .newBuilder()
          .setStatus(ScheduleStatus.ASSIGNED);
      assigned.getAssignedTask()
          .setInstanceId(0)
          .setSlaveHost("b")
          .setSlaveId("b");

      storeProvider.getUnsafeTaskStore().saveTasks(
          IScheduledTask.setFromBuilders(ImmutableList.of(pending, assigned)));
    });

    @SuppressWarnings("unchecked")
    Map<MaintenanceMode, Object> result =
        (Map<MaintenanceMode, Object>) maintenance.getHosts().getEntity();

    assertEquals(ImmutableSet.of(), result.get(SCHEDULED));
    assertEquals(ImmutableSet.of(), result.get(DRAINED));
    assertEquals(ImmutableMap.of(), result.get(DRAINING));
  }
}

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
package org.apache.aurora.scheduler.updater;

import com.google.common.base.Optional;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.InstanceKey;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.state.StateChangeResult;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateInstructions;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;

public class KillTaskTest extends EasyMockTest {
  private static final IJobUpdateInstructions INSTRUCTIONS = IJobUpdateInstructions.build(
      new JobUpdateInstructions()
          .setSettings(
              new JobUpdateSettings()
                  .setMinWaitInInstanceRunningMs(1000)));
  private static final IJobKey JOB = JobKeys.from("role", "env", "job");
  private static final IInstanceKey INSTANCE =
      IInstanceKey.build(new InstanceKey(JOB.newBuilder(), 0));
  private static final IJobUpdateKey UPDATE_ID =
          IJobUpdateKey.build(new JobUpdateKey(JOB.newBuilder(), "update_id"));

  private StorageTestUtil storageUtil;
  private StateManager stateManager;
  private InstanceActionHandler handler;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    stateManager = createMock(StateManager.class);
    handler = new InstanceActionHandler.KillTask();
  }

  @Test
  public void testInstanceKill() throws Exception {
    String id = "task_id";
    storageUtil.expectTaskFetch(
        Query.instanceScoped(INSTANCE).active(),
        TaskTestUtil.makeTask(id, INSTANCE.getJobKey()));

    expect(stateManager.changeState(
        storageUtil.mutableStoreProvider,
        id,
        Optional.absent(),
        ScheduleStatus.KILLING,
        Optional.of("Killed for job update " + UPDATE_ID.getId())))
            .andReturn(StateChangeResult.SUCCESS);

    control.replay();

    handler.getReevaluationDelay(
        INSTANCE,
        INSTRUCTIONS,
        storageUtil.mutableStoreProvider,
        stateManager,
        JobUpdateStatus.ROLLING_BACK,
        UPDATE_ID);
  }

  @Test
  public void testInstanceNotFoundDoesNotThrow() throws Exception {
    storageUtil.expectTaskFetch(Query.instanceScoped(INSTANCE).active());

    control.replay();

    handler.getReevaluationDelay(
        INSTANCE,
        INSTRUCTIONS,
        storageUtil.mutableStoreProvider,
        stateManager,
        JobUpdateStatus.ROLLING_BACK,
        UPDATE_ID);
  }
}

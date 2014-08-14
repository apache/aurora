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
package org.apache.aurora.scheduler.state;

import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.Clock;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateConfiguration;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateRequest;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.state.JobUpdater.UpdaterException;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateRequest;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;

public class JobUpdaterImplTest extends EasyMockTest {

  private static final String UPDATE_ID = "82d6d790-3212-11e3-aa6e-0800200c9a74";
  private static final UUID ID = UUID.fromString(UPDATE_ID);
  private static final IJobKey JOB = JobKeys.from("role", "env", "name");
  private static final Identity IDENTITY = new Identity("role", "user");
  private static final Long TIMESTAMP = 1234L;

  private JobUpdater updater;
  private StorageTestUtil storageUtil;
  private Clock clock;
  private UUIDGenerator idGenerator;

  @Before
  public void setUp() throws Exception {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();

    clock = createMock(Clock.class);
    idGenerator = createMock(UUIDGenerator.class);

    updater = new JobUpdaterImpl(storageUtil.storage, clock, idGenerator);
  }

  @Test
  public void testSaveUpdate() throws Exception {
    IScheduledTask oldTask1 = buildScheduledTask(0, 5);
    IScheduledTask oldTask2 = buildScheduledTask(1, 5);
    IScheduledTask oldTask3 = buildScheduledTask(2, 7);
    IScheduledTask oldTask4 = buildScheduledTask(3, 7);
    IScheduledTask oldTask5 = buildScheduledTask(4, 5);
    IScheduledTask oldTask6 = buildScheduledTask(5, 5);
    IScheduledTask oldTask7 = buildScheduledTask(6, 5);

    ITaskConfig newTask = buildScheduledTask(0, 8).getAssignedTask().getTask();

    IJobUpdate update = buildJobUpdate(6, newTask, ImmutableMap.of(
        oldTask1.getAssignedTask().getTask(), ImmutableSet.of(new Range(0, 1), new Range(4, 6)),
        oldTask3.getAssignedTask().getTask(), ImmutableSet.of(new Range(2, 3))
    ));

    expect(idGenerator.createNew()).andReturn(ID);
    expect(clock.nowMillis()).andReturn(TIMESTAMP);

    storageUtil.expectTaskFetch(
        Query.unscoped().byJob(JOB).active(),
        oldTask1,
        oldTask2,
        oldTask3,
        oldTask4,
        oldTask5,
        oldTask6,
        oldTask7);

    storageUtil.updateStore.saveJobUpdate(update);
    storageUtil.updateStore.saveJobUpdateEvent(buildUpdateEvent(), UPDATE_ID);

    control.replay();

    assertEquals(UPDATE_ID, updater.startJobUpdate(buildJobRequest(update), IDENTITY.getUser()));
  }

  @Test(expected = UpdaterException.class)
  public void testSaveUpdateFails() throws Exception {
    ITaskConfig newTask = buildScheduledTask(0, 8).getAssignedTask().getTask();

    IJobUpdate update = buildJobUpdate(
        6,
        newTask,
        ImmutableMap.<ITaskConfig, ImmutableSet<Range>>of());

    expect(idGenerator.createNew()).andReturn(ID);
    expect(clock.nowMillis()).andReturn(TIMESTAMP);

    storageUtil.expectTaskFetch(Query.unscoped().byJob(JOB).active());

    storageUtil.updateStore.saveJobUpdate(update);
    expectLastCall().andThrow(new Storage.StorageException("fail"));

    control.replay();

    updater.startJobUpdate(buildJobRequest(update), IDENTITY.getUser());
  }

  private static IJobUpdateRequest buildJobRequest(IJobUpdate update) {
    return IJobUpdateRequest.build(new JobUpdateRequest()
        .setInstanceCount(update.getConfiguration().getInstanceCount())
        .setJobKey(update.getSummary().getJobKey().newBuilder())
        .setSettings(update.getConfiguration().getSettings().newBuilder())
        .setTaskConfig(update.getConfiguration().getNewTaskConfig().newBuilder()));
  }

  private static IScheduledTask buildScheduledTask(int instanceId, long ramMb) {
    return IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setInstanceId(instanceId)
            .setTask(new TaskConfig()
                .setRamMb(ramMb) // Simulates unique task config.
                .setOwner(IDENTITY)
                .setEnvironment(JOB.getEnvironment())
                .setJobName(JOB.getName()))));
  }

  private static IJobUpdate buildJobUpdate(
      int instanceCount,
      ITaskConfig newConfig,
      ImmutableMap<ITaskConfig, ImmutableSet<Range>> oldConfigMap) {

    ImmutableSet.Builder<InstanceTaskConfig> builder = ImmutableSet.builder();
    for (Map.Entry<ITaskConfig, ImmutableSet<Range>> entry : oldConfigMap.entrySet()) {
      builder.add(new InstanceTaskConfig(entry.getKey().newBuilder(), entry.getValue()));
    }

    return IJobUpdate.build(new JobUpdate()
        .setSummary(new JobUpdateSummary()
            .setJobKey(JOB.newBuilder())
            .setUpdateId(UPDATE_ID)
            .setUser(IDENTITY.getUser()))
        .setConfiguration(new JobUpdateConfiguration()
            .setSettings(new JobUpdateSettings())
            .setInstanceCount(instanceCount)
            .setNewTaskConfig(newConfig.newBuilder())
            .setOldTaskConfigs(builder.build())));
  }

  private static IJobUpdateEvent buildUpdateEvent() {
    return IJobUpdateEvent.build(new JobUpdateEvent()
        .setStatus(JobUpdateStatus.ROLLING_FORWARD)
        .setTimestampMs(TIMESTAMP));
  }
}

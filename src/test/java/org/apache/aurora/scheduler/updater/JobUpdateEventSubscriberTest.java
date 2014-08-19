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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.InstanceKey;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.events.PubsubEvent.SchedulerActive;
import static org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import static org.apache.aurora.scheduler.events.PubsubEvent.TasksDeleted;
import static org.easymock.EasyMock.expect;

public class JobUpdateEventSubscriberTest extends EasyMockTest {

  private static final IJobKey JOB_A = JobKeys.from("role", "env", "name");

  private static final IScheduledTask TASK_A = IScheduledTask.build(
      new ScheduledTask().setAssignedTask(
          new AssignedTask()
              .setInstanceId(5)
              .setTask(new TaskConfig()
                  .setOwner(new Identity().setRole(JOB_A.getRole()))
                  .setEnvironment(JOB_A.getEnvironment())
                  .setJobName(JOB_A.getName()))));
  private static final IInstanceKey INSTANCE_A = IInstanceKey.build(
      new InstanceKey(JOB_A.newBuilder(), TASK_A.getAssignedTask().getInstanceId()));

  private StorageTestUtil storageUtil;
  private JobUpdateController updater;

  private EventBus eventBus;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    updater = createMock(JobUpdateController.class);

    eventBus = new EventBus();
    eventBus.register(new JobUpdateEventSubscriber(updater, storageUtil.storage));
  }

  @Test
  public void testStateChange() throws Exception {
    updater.instanceChangedState(INSTANCE_A);

    control.replay();

    eventBus.post(TaskStateChange.initialized(TASK_A));
  }

  @Test
  public void testDeleted() throws Exception {
    updater.instanceChangedState(INSTANCE_A);

    control.replay();

    eventBus.post(new TasksDeleted(ImmutableSet.of(TASK_A)));
  }

  private static final IJobUpdateSummary SUMMARY = IJobUpdateSummary.build(new JobUpdateSummary()
      .setJobKey(JOB_A.newBuilder()));

  @Test
  public void testSchedulerStartup() throws Exception {
    expect(storageUtil.jobUpdateStore.fetchJobUpdateSummaries(
        JobUpdateEventSubscriber.ACTIVE_QUERY)).andReturn(ImmutableList.of(SUMMARY));

    updater.systemResume(JOB_A);

    control.replay();

    eventBus.post(new SchedulerActive());
  }

  @Test
  public void testSchedulerStartupNoUpdates() throws Exception {
    expect(storageUtil.jobUpdateStore.fetchJobUpdateSummaries(
        JobUpdateEventSubscriber.ACTIVE_QUERY)).andReturn(ImmutableList.<IJobUpdateSummary>of());

    control.replay();

    eventBus.post(new SchedulerActive());
  }
}

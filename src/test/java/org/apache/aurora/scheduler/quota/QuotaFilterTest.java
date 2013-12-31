/**
 * Copyright 2013 Apache Software Foundation
 *
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
package org.apache.aurora.scheduler.quota;

import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.Quota;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.quota.QuotaManager.QuotaManagerImpl;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IQuota;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;

import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.quota.QuotaComparisonResult.Result.INSUFFICIENT_QUOTA;
import static org.apache.aurora.scheduler.quota.QuotaComparisonResult.Result.SUFFICIENT_QUOTA;

import static org.easymock.EasyMock.expect;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QuotaFilterTest extends EasyMockTest {
  private static final int DEFAULT_TASKS_IN_QUOTA = 10;
  private static final String ROLE = "test";
  private static final String JOB_NAME = "test_job";
  private static final String ENV = "test_env";
  private static final IJobKey JOB_KEY = JobKeys.from(ROLE, ENV, JOB_NAME);
  private static final Query.Builder QUERY = Query.jobScoped(JOB_KEY).active();
  private static final IQuota QUOTA = IQuota.build(new Quota()
      .setNumCpus(1.0)
      .setRamMb(256L)
      .setDiskMb(512L));
  private static final ITaskConfig TASK_CONFIG = ITaskConfig.build(new TaskConfig()
      .setNumCpus(QUOTA.getNumCpus())
      .setRamMb(QUOTA.getRamMb())
      .setDiskMb(QUOTA.getDiskMb()));
  private static final IJobConfiguration JOB = IJobConfiguration.build(new JobConfiguration()
      .setKey(JOB_KEY.newBuilder())
      .setInstanceCount(1)
      .setTaskConfig(TASK_CONFIG.newBuilder()));

  private QuotaFilter quotaFilter;

  private QuotaManagerImpl quotaManager;
  private StorageTestUtil storageTestUtil;
  private QuotaComparisonResult quotaCompResult;

  @Before
  public void setUp() {
    quotaManager = createMock(QuotaManagerImpl.class);
    quotaCompResult = createMock(QuotaComparisonResult.class);
    storageTestUtil = new StorageTestUtil(this);

    quotaFilter = new QuotaFilter(quotaManager, storageTestUtil.storage);
  }

  @Test
  public void testNonProductionPasses() {
    JobConfiguration jobBuilder = JOB.newBuilder();
    jobBuilder.getTaskConfig().setProduction(false);
    IJobConfiguration job = IJobConfiguration.build(jobBuilder);

    control.replay();

    assertTrue(quotaFilter.filter(job).isPass());
  }

  @Test
  public void testCreateProductionJobChecksQuota() {
    JobConfiguration jobBuilder = JOB.newBuilder();
    jobBuilder.getTaskConfig().setProduction(true);
    IJobConfiguration job = IJobConfiguration.build(jobBuilder);

    storageTestUtil.expectOperations();
    storageTestUtil.expectTaskFetch(QUERY).times(2);

    expect(quotaManager.checkQuota(ROLE, QUOTA)).andReturn(quotaCompResult);
    expect(quotaCompResult.result()).andReturn(SUFFICIENT_QUOTA);
    expect(quotaManager.checkQuota(ROLE, QUOTA)).andReturn(quotaCompResult);
    expect(quotaCompResult.result()).andReturn(INSUFFICIENT_QUOTA);
    expect(quotaCompResult.details()).andReturn("Details");

    control.replay();

    assertTrue(quotaFilter.filter(job).isPass());
    assertFalse(quotaFilter.filter(job).isPass());
  }

  @Test
  public void testUpdateProductionJobChecksQuota() {
    JobConfiguration jobBuilder = JOB.newBuilder();
    jobBuilder.getTaskConfig().setProduction(true);

    storageTestUtil.expectOperations();
    storageTestUtil.expectTaskFetch(QUERY,
        IScheduledTask.build(new ScheduledTask().setAssignedTask(
            new AssignedTask().setTask(jobBuilder.getTaskConfig()))));

    expect(quotaManager.checkQuota(ROLE, IQuota.build(new Quota(0, 0, 0))))
        .andReturn(quotaCompResult);
    expect(quotaCompResult.result()).andReturn(SUFFICIENT_QUOTA);

    control.replay();

    assertTrue(quotaFilter.filter(IJobConfiguration.build(jobBuilder)).isPass());
  }

  @Test
  public void testIncreaseShardsExceedsQuota() {
    int numTasks = DEFAULT_TASKS_IN_QUOTA;
    int additionalTasks = 1;

    JobConfiguration jobBuilder = JOB.newBuilder().setInstanceCount(numTasks + additionalTasks);
    jobBuilder.getTaskConfig().setProduction(true);

    IScheduledTask[] scheduledTasks = new IScheduledTask[numTasks];
    for (int i = 0; i < numTasks; i++) {
      ScheduledTask builder = new ScheduledTask().setAssignedTask(
          new AssignedTask().setTask(jobBuilder.getTaskConfig()));
      builder.getAssignedTask().setInstanceId(i);
      scheduledTasks[i] = IScheduledTask.build(builder);
    }

    storageTestUtil.expectOperations();
    storageTestUtil.expectTaskFetch(QUERY, scheduledTasks);

    expect(quotaManager.checkQuota(ROLE, QUOTA)).andReturn(quotaCompResult);
    expect(quotaCompResult.result()).andReturn(INSUFFICIENT_QUOTA);
    expect(quotaCompResult.details()).andReturn("Details");

    control.replay();

    assertFalse(quotaFilter.filter(IJobConfiguration.build(jobBuilder)).isPass());
  }

  @Test
  public void testUpdateProductionTasksChecksQuota() {
    JobConfiguration jobBuilder = JOB.newBuilder();
    TaskConfig config = jobBuilder.getTaskConfig()
        .setProduction(true)
        .setOwner(new Identity(ROLE, "user"))
        .setEnvironment(ENV)
        .setJobName(JOB_NAME);

    storageTestUtil.expectOperations();
    storageTestUtil.expectTaskFetch(QUERY,
        IScheduledTask.build(new ScheduledTask().setAssignedTask(
            new AssignedTask().setTask(jobBuilder.getTaskConfig()))));

    expect(quotaManager.checkQuota(ROLE, IQuota.build(new Quota(0, 0, 0))))
        .andReturn(quotaCompResult);
    expect(quotaCompResult.result()).andReturn(SUFFICIENT_QUOTA);

    control.replay();

    assertTrue(quotaFilter.filter(ITaskConfig.build(config), 1).isPass());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUpdateProductionTasksFailsJobKeyCreation() {
    JobConfiguration jobBuilder = JOB.newBuilder();
    jobBuilder.getTaskConfig().setOwner(new Identity(ROLE, "user"));
    control.replay();

    quotaFilter.filter(ITaskConfig.build(jobBuilder.getTaskConfig()), 1);
  }
}

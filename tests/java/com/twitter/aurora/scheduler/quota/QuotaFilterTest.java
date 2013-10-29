package com.twitter.aurora.scheduler.quota;

import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.gen.AssignedTask;
import com.twitter.aurora.gen.Identity;
import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.Quota;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.quota.QuotaManager.QuotaManagerImpl;
import com.twitter.aurora.scheduler.storage.entities.IJobConfiguration;
import com.twitter.aurora.scheduler.storage.entities.IJobKey;
import com.twitter.aurora.scheduler.storage.entities.IQuota;
import com.twitter.aurora.scheduler.storage.entities.IScheduledTask;
import com.twitter.aurora.scheduler.storage.entities.ITaskConfig;
import com.twitter.aurora.scheduler.storage.testing.StorageTestUtil;
import com.twitter.common.testing.easymock.EasyMockTest;

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

  @Before
  public void setUp() {
    quotaManager = createMock(QuotaManagerImpl.class);
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

    expect(quotaManager.hasRemaining(ROLE, QUOTA)).andReturn(true);
    expect(quotaManager.hasRemaining(ROLE, QUOTA)).andReturn(false);

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

    expect(quotaManager.hasRemaining(ROLE, IQuota.build(new Quota(0, 0, 0)))).andReturn(true);

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

    expect(quotaManager.hasRemaining(ROLE, QUOTA)).andReturn(false);

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

    expect(quotaManager.hasRemaining(ROLE, IQuota.build(new Quota(0, 0, 0)))).andReturn(true);

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

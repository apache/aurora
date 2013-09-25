package com.twitter.aurora.scheduler.quota;

import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.gen.AssignedTask;
import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.JobKey;
import com.twitter.aurora.gen.Quota;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.storage.testing.StorageTestUtil;
import com.twitter.common.testing.easymock.EasyMockTest;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QuotaFilterTest extends EasyMockTest {
  private static final int DEFAULT_TASKS_IN_QUOTA = 10;
  private static final String ROLE = "test";
  private static final JobKey JOB_KEY = JobKeys.from(ROLE, "test", "test");
  private static final Query.Builder QUERY = Query.jobScoped(JOB_KEY).active();
  private static final Quota QUOTA = new Quota()
      .setNumCpus(1.0)
      .setRamMb(256L)
      .setDiskMb(512L);
  private static final TaskConfig TASK_CONFIG = new TaskConfig()
      .setNumCpus(QUOTA.getNumCpus())
      .setRamMb(QUOTA.getRamMb())
      .setDiskMb(QUOTA.getDiskMb());
  private static final JobConfiguration JOB = new JobConfiguration()
      .setKey(JOB_KEY.deepCopy())
      .setShardCount(1)
      .setTaskConfig(TASK_CONFIG.deepCopy());

  private QuotaFilter quotaFilter;

  private QuotaManager quotaManager;
  private StorageTestUtil storageTestUtil;

  @Before
  public void setUp() {
    quotaManager = createMock(QuotaManager.class);
    storageTestUtil = new StorageTestUtil(this);

    quotaFilter = new QuotaFilter(quotaManager, storageTestUtil.storage);
  }

  @Test
  public void testNonProductionPasses() {
    JobConfiguration job = JOB.deepCopy();
    job.getTaskConfig().setProduction(false);

    control.replay();

    assertTrue(quotaFilter.filter(job).isPass());
  }

  @Test
  public void testCreateProductionJobChecksQuota() {
    JobConfiguration job = JOB.deepCopy();
    job.getTaskConfig().setProduction(true);

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
    JobConfiguration job = JOB.deepCopy();
    job.getTaskConfig().setProduction(true);

    storageTestUtil.expectOperations();
    storageTestUtil.expectTaskFetch(QUERY,
        new ScheduledTask().setAssignedTask(
            new AssignedTask().setTask(
                job.getTaskConfig().deepCopy())));

    expect(quotaManager.hasRemaining(ROLE, new Quota(0, 0, 0))).andReturn(true);

    control.replay();

    assertTrue(quotaFilter.filter(job).isPass());
  }

  @Test
  public void testIncreaseShardsExceedsQuota() {
    int numTasks = DEFAULT_TASKS_IN_QUOTA;
    int additionalTasks = 1;

    JobConfiguration job = JOB.deepCopy().setShardCount(numTasks + additionalTasks);
    job.getTaskConfig().setProduction(true);

    ScheduledTask[] scheduledTasks = new ScheduledTask[numTasks];
    for (int i = 0; i < numTasks; i++) {
      scheduledTasks[i] = new ScheduledTask().setAssignedTask(
          new AssignedTask().setTask(
              job.getTaskConfig().deepCopy()));
      scheduledTasks[i].getAssignedTask().getTask().setShardId(i);
    }

    storageTestUtil.expectOperations();
    storageTestUtil.expectTaskFetch(QUERY, scheduledTasks);

    expect(quotaManager.hasRemaining(ROLE, QUOTA)).andReturn(false);

    control.replay();

    assertFalse(quotaFilter.filter(job).isPass());
  }
}

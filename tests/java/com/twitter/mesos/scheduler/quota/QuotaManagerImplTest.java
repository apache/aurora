package com.twitter.mesos.scheduler.quota;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.testing.junit4.TearDownTestCase;

import org.junit.Before;
import org.junit.Test;

import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.quota.QuotaManager.QuotaManagerImpl;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.Storage.Work.NoResult;
import com.twitter.mesos.scheduler.storage.db.DbStorageTestUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author William Farner
 */
public class QuotaManagerImplTest extends TearDownTestCase {

  private static final String ROLE = "foo";

  private Storage storage;
  private QuotaManager quotaManager;

  @Before
  public void setUp() throws Exception {
    storage = DbStorageTestUtil.setupStorage(this);
    quotaManager = new QuotaManagerImpl(storage);
    storage.start(new NoResult.Quiet() {
      @Override protected void execute(Storage.StoreProvider storeProvider) {}
    });
  }

  @Test
  public void testGetEmptyQuota() {
    assertEquals(Quotas.NO_QUOTA, quotaManager.getQuota(ROLE));
    assertEquals(Quotas.NO_QUOTA, quotaManager.getConsumption(ROLE));
  }

  @Test
  public void testSetQuota() {
    Quota quota = new Quota(2, 2, 2);
    quotaManager.setQuota(ROLE, quota);
    assertEquals(quota, quotaManager.getQuota(ROLE));
  }

  @Test
  public void testConsumeNoQuota() {
    assertTrue(quotaManager.hasRemaining(ROLE, Quotas.NO_QUOTA));
  }

  @Test
  public void testNoQuotaExhausted() {
    assertFalse(quotaManager.hasRemaining(ROLE, new Quota(1, 1, 1)));
  }

  private ScheduledTask createTask(String jobName, String taskId, int cpus, int ramMb, int diskMb) {
    return new ScheduledTask()
        .setStatus(ScheduleStatus.RUNNING)
        .setAssignedTask(
            new AssignedTask()
                .setTaskId(taskId)
                .setTask(new TwitterTaskInfo()
                    .setOwner(new Identity(ROLE, ROLE))
                    .setJobName(jobName)
                    .setNumCpus(cpus)
                    .setRamMb(ramMb)
                    .setDiskMb(diskMb)
                    .setProduction(true)
                ));
  }

  private void addTasks(final Set<ScheduledTask> tasks) {
    storage.doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(StoreProvider storeProvider) {
        storeProvider.getTaskStore().saveTasks(tasks);
      }
    });
  }

  @Test
  public void testUseAllQuota() {
    Quota fullQuota = new Quota(2, 2, 2);
    Quota half = new Quota(1, 1, 1);
    quotaManager.setQuota(ROLE, fullQuota);
    addTasks(ImmutableSet.of(createTask("foo", "id1", 1, 1, 1)));
    assertTrue(quotaManager.hasRemaining(ROLE, half));
    addTasks(ImmutableSet.of(createTask("foo", "id2", 1, 1, 1)));
    assertFalse(quotaManager.hasRemaining(ROLE, half));
  }

  @Test
  public void testExhaustCpu() {
    Quota fullQuota = new Quota(2, 2, 2);
    quotaManager.setQuota(ROLE, fullQuota);
    addTasks(ImmutableSet.of(createTask("foo", "id1", 1, 1, 1)));
    assertFalse(quotaManager.hasRemaining(ROLE, new Quota(2, 1, 1)));
  }

  @Test
  public void testExhaustRam() {
    Quota fullQuota = new Quota(2, 2, 2);
    quotaManager.setQuota(ROLE, fullQuota);
    addTasks(ImmutableSet.of(createTask("foo", "id1", 1, 1, 1)));
    assertFalse(quotaManager.hasRemaining(ROLE, new Quota(1, 2, 1)));
  }

  @Test
  public void testExhaustDisk() {
    Quota fullQuota = new Quota(2, 2, 2);
    quotaManager.setQuota(ROLE, fullQuota);
    addTasks(ImmutableSet.of(createTask("foo", "id1", 1, 1, 1)));
    assertFalse(quotaManager.hasRemaining(ROLE, new Quota(1, 1, 2)));
  }

  @Test
  public void testNonproductionUnaccounted() {
    Quota fullQuota = new Quota(2, 2, 2);
    quotaManager.setQuota(ROLE, fullQuota);

    ScheduledTask task = createTask("foo", "id1", 3, 3, 3);
    task.getAssignedTask().getTask().setProduction(false);

    addTasks(ImmutableSet.of(task));
    assertTrue(quotaManager.hasRemaining(ROLE, new Quota(2, 2, 2)));
  }
}

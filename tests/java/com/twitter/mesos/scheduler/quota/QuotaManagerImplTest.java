package com.twitter.mesos.scheduler.quota;

import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.storage.JobUpdateConfiguration;
import com.twitter.mesos.gen.storage.TaskUpdateConfiguration;
import com.twitter.mesos.scheduler.quota.QuotaManager.QuotaManagerImpl;
import com.twitter.mesos.scheduler.storage.testing.StorageTestUtil;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author William Farner
 */
public class QuotaManagerImplTest extends EasyMockTest {

  private static final String ROLE = "foo";
  private static final TaskQuery ACTIVE_QUERY = new TaskQuery()
          .setOwner(new Identity().setRole(ROLE))
          .setStatuses(Tasks.ACTIVE_STATES);

  private StorageTestUtil storageUtil;
  private QuotaManager quotaManager;

  @Before
  public void setUp() throws Exception {
    storageUtil = new StorageTestUtil(this);
    quotaManager = new QuotaManagerImpl(storageUtil.storage);
  }

  @Test
  public void testGetEmptyQuota() {
    storageUtil.expectTransactions();
    noActiveUpdates();
    expect(storageUtil.quotaStore.fetchQuota(ROLE)).andReturn(Optional.<Quota>absent());
    returnNoTasks().atLeastOnce();

    control.replay();

    assertEquals(Quotas.NO_QUOTA, quotaManager.getQuota(ROLE));
    assertEquals(Quotas.NO_QUOTA, quotaManager.getConsumption(ROLE));
  }

  @Test
  public void testConsumeNoQuota() {
    storageUtil.expectTransactions();
    noActiveUpdates();
    applyQuota(new Quota(1, 1, 1));
    returnNoTasks();

    control.replay();

    assertTrue(quotaManager.hasRemaining(ROLE, Quotas.NO_QUOTA));
  }

  @Test
  public void testNoQuotaExhausted() {
    storageUtil.expectTransactions();
    returnNoTasks();
    noActiveUpdates();
    expect(storageUtil.quotaStore.fetchQuota(ROLE)).andReturn(Optional.<Quota>absent());

    control.replay();

    assertFalse(quotaManager.hasRemaining(ROLE, new Quota(1, 1, 1)));
  }

  @Test
  public void testSetQuota() {
    Quota quota = new Quota(1, 2, 3);

    storageUtil.expectTransactions();
    storageUtil.quotaStore.saveQuota(ROLE, quota);

    control.replay();

    quotaManager.setQuota(ROLE, quota);
  }

  @Test
  public void testUseAllQuota() {
    ScheduledTask task1 = createTask("foo", "id1", 1, 1, 1);
    ScheduledTask task2 = createTask("foo", "id2", 1, 1, 1);

    storageUtil.expectTransactions();
    applyQuota(new Quota(2, 2, 2)).anyTimes();
    noActiveUpdates();
    returnTasks(task1);
    returnTasks(task1, task2);

    control.replay();

    Quota half = new Quota(1, 1, 1);
    assertTrue(quotaManager.hasRemaining(ROLE, half));
    assertFalse(quotaManager.hasRemaining(ROLE, half));
  }

  @Test
  public void testExhaustCpu() {
    storageUtil.expectTransactions();
    applyQuota(new Quota(2, 2, 2));
    noActiveUpdates();
    returnTasks(createTask("foo", "id1", 1, 1, 1));

    control.replay();

    assertFalse(quotaManager.hasRemaining(ROLE, new Quota(2, 1, 1)));
  }

  @Test
  public void testExhaustRam() {
    storageUtil.expectTransactions();
    applyQuota(new Quota(2, 2, 2));
    noActiveUpdates();
    returnTasks(createTask("foo", "id1", 1, 1, 1));

    control.replay();

    assertFalse(quotaManager.hasRemaining(ROLE, new Quota(1, 2, 1)));
  }

  @Test
  public void testExhaustDisk() {
    storageUtil.expectTransactions();
    applyQuota(new Quota(2, 2, 2));
    noActiveUpdates();
    returnTasks(createTask("foo", "id1", 1, 1, 1));

    control.replay();

    assertFalse(quotaManager.hasRemaining(ROLE, new Quota(1, 1, 2)));
  }

  @Test
  public void testNonproductionUnaccounted() {
    ScheduledTask task = createTask("foo", "id1", 3, 3, 3);
    task.getAssignedTask().getTask().setProduction(false);

    storageUtil.expectTransactions();
    applyQuota(new Quota(2, 2, 2));
    noActiveUpdates();
    returnTasks(task);

    control.replay();

    assertTrue(quotaManager.hasRemaining(ROLE, new Quota(2, 2, 2)));
  }

  @Test
  public void testUpdating() {
    ScheduledTask task = createTask("bar", "id1", 1, 1, 1);
    ScheduledTask updatingTask = createTask("foo", "id1", 1, 1, 1);

    storageUtil.expectTransactions();
    applyQuota(new Quota(4, 4, 4)).anyTimes();
    returnTasks(task, updatingTask).anyTimes();

    // Simulate a job update that increases the job quota consumption.
    expectUpdateQuery().andReturn(
        ImmutableSet.of(new JobUpdateConfiguration(ROLE, "foo", "token",
            ImmutableSet.of(new TaskUpdateConfiguration(
                updatingTask.getAssignedTask().getTask(),
                createTaskConfig("foo", 2, 2, 2)))))).anyTimes();

    control.replay();

    assertTrue(quotaManager.hasRemaining(ROLE, new Quota(1, 1, 1)));
    assertFalse(quotaManager.hasRemaining(ROLE, new Quota(2, 2, 2)));
  }

  private IExpectationSetters<ImmutableSet<ScheduledTask>> expectTaskQuery() {
    return expect(storageUtil.taskStore.fetchTasks(ACTIVE_QUERY));
  }

  private IExpectationSetters<ImmutableSet<ScheduledTask>> returnTasks(ScheduledTask... tasks) {
    return expectTaskQuery().andReturn(ImmutableSet.<ScheduledTask>builder().add(tasks).build());
  }

  private IExpectationSetters<ImmutableSet<ScheduledTask>> returnNoTasks() {
    return returnTasks();
  }

  private IExpectationSetters<Set<JobUpdateConfiguration>> expectUpdateQuery() {
    return expect(storageUtil.updateStore.fetchUpdateConfigs(ROLE));
  }

  private void noActiveUpdates() {
    expectUpdateQuery().andReturn(ImmutableSet.<JobUpdateConfiguration>of()).anyTimes();
  }

  private IExpectationSetters<Optional<Quota>> applyQuota(Quota quota) {
    return expect(storageUtil.quotaStore.fetchQuota(ROLE)).andReturn(Optional.of(quota));
  }

  private ScheduledTask createTask(String jobName, String taskId, int cpus, int ramMb, int diskMb) {
    return new ScheduledTask()
        .setStatus(ScheduleStatus.RUNNING)
        .setAssignedTask(
            new AssignedTask()
                .setTaskId(taskId)
                .setTask(createTaskConfig(jobName, cpus, ramMb, diskMb)));
  }

  private TwitterTaskInfo createTaskConfig(String jobName, int cpus, int ramMb, int diskMb) {
    return new TwitterTaskInfo()
        .setOwner(new Identity(ROLE, ROLE))
        .setJobName(jobName)
        .setNumCpus(cpus)
        .setRamMb(ramMb)
        .setDiskMb(diskMb)
        .setProduction(true);
  }
}

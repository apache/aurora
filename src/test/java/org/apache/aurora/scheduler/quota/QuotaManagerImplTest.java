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

import com.google.common.base.Optional;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.Quota;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.quota.QuotaManager.QuotaManagerImpl;
import org.apache.aurora.scheduler.storage.entities.IQuota;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.quota.QuotaComparisonResult.Result.INSUFFICIENT_QUOTA;
import static org.apache.aurora.scheduler.quota.QuotaComparisonResult.Result.SUFFICIENT_QUOTA;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QuotaManagerImplTest extends EasyMockTest {
  private static final String ROLE = "foo";
  private static final Query.Builder ACTIVE_QUERY = Query.roleScoped(ROLE).active();

  private StorageTestUtil storageUtil;
  // TODO(maximk): Move checkQuota to QuotaFilter along with tests.
  private QuotaManagerImpl quotaManager;

  @Before
  public void setUp() throws Exception {
    storageUtil = new StorageTestUtil(this);
    quotaManager = new QuotaManagerImpl(storageUtil.storage);
  }

  @Test
  public void testGetEmptyQuota() {
    storageUtil.expectOperations();
    returnNoTasks();

    control.replay();

    assertEquals(Quotas.noQuota(), quotaManager.getConsumption(ROLE));
  }

  @Test
  public void testConsumeNoQuota() {
    storageUtil.expectOperations();
    applyQuota(new Quota(1, 1, 1));
    returnNoTasks();

    control.replay();

    assertEquals(SUFFICIENT_QUOTA, quotaManager.checkQuota(ROLE, Quotas.noQuota()).result());
  }

  @Test
  public void testNoQuotaExhausted() {
    storageUtil.expectOperations();
    returnNoTasks();
    expect(storageUtil.quotaStore.fetchQuota(ROLE)).andReturn(Optional.<IQuota>absent());

    control.replay();

    QuotaComparisonResult result =
        quotaManager.checkQuota(ROLE, IQuota.build(new Quota(1, 1, 1)));

    assertEquals(INSUFFICIENT_QUOTA, result.result());
    assertTrue(result.details().length() > 0);
  }

  @Test
  public void testUseAllQuota() {
    IScheduledTask task1 = createTask("foo", "id1", 1, 1, 1);
    IScheduledTask task2 = createTask("foo", "id2", 1, 1, 1);

    storageUtil.expectOperations();
    applyQuota(new Quota(2, 2, 2)).anyTimes();
    returnTasks(task1);
    returnTasks(task1, task2);

    control.replay();

    IQuota half = IQuota.build(new Quota(1, 1, 1));
    assertEquals(SUFFICIENT_QUOTA, quotaManager.checkQuota(ROLE, half).result());
    assertEquals(INSUFFICIENT_QUOTA, quotaManager.checkQuota(ROLE, half).result());
  }

  @Test
  public void testExhaustCpu() {
    storageUtil.expectOperations();
    applyQuota(new Quota(2, 2, 2));
    returnTasks(createTask("foo", "id1", 1, 1, 1));

    control.replay();

    assertEquals(
        INSUFFICIENT_QUOTA,
        quotaManager.checkQuota(ROLE, IQuota.build(new Quota(2, 1, 1))).result());
  }

  @Test
  public void testExhaustRam() {
    storageUtil.expectOperations();
    applyQuota(new Quota(2, 2, 2));
    returnTasks(createTask("foo", "id1", 1, 1, 1));

    control.replay();

    assertEquals(
        INSUFFICIENT_QUOTA,
        quotaManager.checkQuota(ROLE, IQuota.build(new Quota(1, 2, 1))).result());
  }

  @Test
  public void testExhaustDisk() {
    storageUtil.expectOperations();
    applyQuota(new Quota(2, 2, 2));
    returnTasks(createTask("foo", "id1", 1, 1, 1));

    control.replay();

    assertEquals(
        INSUFFICIENT_QUOTA,
        quotaManager.checkQuota(ROLE, IQuota.build(new Quota(1, 1, 2))).result());
  }

  @Test
  public void testNonproductionUnaccounted() {
    ScheduledTask builder = createTask("foo", "id1", 3, 3, 3).newBuilder();
    builder.getAssignedTask().getTask().setProduction(false);
    IScheduledTask task = IScheduledTask.build(builder);

    storageUtil.expectOperations();
    applyQuota(new Quota(2, 2, 2));
    returnTasks(task);

    control.replay();

    assertEquals(
        SUFFICIENT_QUOTA,
        quotaManager.checkQuota(ROLE, IQuota.build(new Quota(2, 2, 2))).result());
  }

  private IExpectationSetters<?> returnTasks(IScheduledTask... tasks) {
    return storageUtil.expectTaskFetch(ACTIVE_QUERY, tasks);
  }

  private IExpectationSetters<?> returnNoTasks() {
    return returnTasks();
  }

  private IExpectationSetters<Optional<IQuota>> applyQuota(Quota quota) {
    return expect(storageUtil.quotaStore.fetchQuota(ROLE))
        .andReturn(Optional.of(IQuota.build(quota)));
  }

  private IScheduledTask createTask(
      String jobName,
      String taskId,
      int cpus,
      int ramMb,
      int diskMb) {

    return IScheduledTask.build(new ScheduledTask()
        .setStatus(ScheduleStatus.RUNNING)
        .setAssignedTask(
            new AssignedTask()
                .setTaskId(taskId)
                .setTask(createTaskConfig(jobName, cpus, ramMb, diskMb))));
  }

  private TaskConfig createTaskConfig(String jobName, int cpus, int ramMb, int diskMb) {
    return new TaskConfig()
        .setOwner(new Identity(ROLE, ROLE))
        .setJobName(jobName)
        .setNumCpus(cpus)
        .setRamMb(ramMb)
        .setDiskMb(diskMb)
        .setProduction(true);
  }
}

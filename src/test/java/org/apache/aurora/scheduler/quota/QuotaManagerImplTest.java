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
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.quota.QuotaManager.QuotaException;
import org.apache.aurora.scheduler.quota.QuotaManager.QuotaManagerImpl;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.INSUFFICIENT_QUOTA;
import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.SUFFICIENT_QUOTA;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QuotaManagerImplTest extends EasyMockTest {
  private static final String ROLE = "test";
  private static final String ENV = "test_env";
  private static final IResourceAggregate QUOTA = IResourceAggregate.build(new ResourceAggregate()
      .setNumCpus(1.0)
      .setRamMb(100L)
      .setDiskMb(200L));
  private static final Query.Builder ACTIVE_QUERY = Query.roleScoped(ROLE).active();

  private StorageTestUtil storageUtil;
  private QuotaManagerImpl quotaManager;

  @Before
  public void setUp() throws Exception {
    storageUtil = new StorageTestUtil(this);
    quotaManager = new QuotaManagerImpl(storageUtil.storage);
  }

  @Test
  public void testGetQuotaInfo() {
    IScheduledTask prodTask = createTask("foo", "id1", 3, 3, 3, true);
    IScheduledTask nonProdTask = createTask("bar", "id1", 2, 2, 2, false);
    IResourceAggregate quota = IResourceAggregate.build(new ResourceAggregate(4, 4, 4));

    expectQuota(quota);
    expectTasks(prodTask, nonProdTask);
    storageUtil.expectOperations();

    control.replay();

    QuotaInfo quotaInfo = quotaManager.getQuotaInfo(ROLE);
    assertEquals(quota, quotaInfo.guota());
    assertEquals(
        IResourceAggregate.build(new ResourceAggregate(3, 3, 3)), quotaInfo.prodConsumption());
    assertEquals(
        IResourceAggregate.build(new ResourceAggregate(2, 2, 2)), quotaInfo.nonProdConsumption());
  }

  @Test
  public void testGetQuotaInfoNoTasks() {
    IResourceAggregate quota = IResourceAggregate.build(new ResourceAggregate(4, 4, 4));

    expectQuota(quota);
    expectNoTasks();
    storageUtil.expectOperations();

    control.replay();

    QuotaInfo quotaInfo = quotaManager.getQuotaInfo(ROLE);
    assertEquals(quota, quotaInfo.guota());
    assertEquals(ResourceAggregates.none(), quotaInfo.prodConsumption());
    assertEquals(ResourceAggregates.none(), quotaInfo.nonProdConsumption());
  }

  @Test
  public void testCheckQuotaPasses() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(4, 4, 4)));
    expectTasks(createTask("foo", "id1", 3, 3, 3, true));
    storageUtil.expectOperations();

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkQuota(createTaskConfig(1, 1, 1, true), 1);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaPassesNoTasks() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(4, 4, 4)));
    expectNoTasks();
    storageUtil.expectOperations();

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkQuota(createTaskConfig(1, 1, 1, true), 1);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaPassesNonProdUnaccounted() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(4, 4, 4)));
    expectTasks(createTask("foo", "id1", 3, 3, 3, true), createTask("bar", "id2", 5, 5, 5, false));
    storageUtil.expectOperations();

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkQuota(createTaskConfig(1, 1, 1, true), 1);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaSkippedForNonProdRequest() {
    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkQuota(createTaskConfig(1, 1, 1, false), 1);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaNoQuotaSet() {
    expect(storageUtil.quotaStore.fetchQuota(ROLE))
        .andReturn(Optional.<IResourceAggregate>absent());

    expectNoTasks();
    storageUtil.expectOperations();

    control.replay();
    QuotaCheckResult checkQuota = quotaManager.checkQuota(createTaskConfig(1, 1, 1, true), 1);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaExceedsCpu() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(4, 4, 4)));
    expectTasks(createTask("foo", "id1", 3, 3, 3, true));
    storageUtil.expectOperations();

    control.replay();
    QuotaCheckResult checkQuota = quotaManager.checkQuota(createTaskConfig(2, 1, 1, true), 1);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertTrue(checkQuota.getDetails().get().contains("CPU"));
  }

  @Test
  public void testCheckQuotaExceedsRam() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(4, 4, 4)));
    expectTasks(createTask("foo", "id1", 3, 3, 3, true));
    storageUtil.expectOperations();

    control.replay();
    QuotaCheckResult checkQuota = quotaManager.checkQuota(createTaskConfig(1, 2, 1, true), 1);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertTrue(checkQuota.getDetails().get().contains("RAM"));
  }

  @Test
  public void testCheckQuotaExceedsDisk() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(4, 4, 4)));
    expectTasks(createTask("foo", "id1", 3, 3, 3, true));
    storageUtil.expectOperations();

    control.replay();
    QuotaCheckResult checkQuota = quotaManager.checkQuota(createTaskConfig(1, 1, 2, true), 1);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertTrue(checkQuota.getDetails().get().contains("DISK"));
  }

  @Test
  public void testSaveQuotaPasses() throws Exception {
    storageUtil.quotaStore.saveQuota(ROLE, QUOTA);
    storageUtil.expectOperations();

    control.replay();
    quotaManager.saveQuota(ROLE, QUOTA);
  }

  @Test(expected = QuotaException.class)
  public void testSaveQuotaFailsMissingSpecs() throws Exception {
    storageUtil.expectOperations();

    control.replay();
    quotaManager.saveQuota(ROLE, IResourceAggregate.build(new ResourceAggregate()));
  }

  @Test(expected = QuotaException.class)
  public void testSaveQuotaFailsNegativeValues() throws Exception {
    storageUtil.expectOperations();

    control.replay();
    quotaManager.saveQuota(ROLE, IResourceAggregate.build(new ResourceAggregate(-2.0, 4, 5)));
  }

  private IExpectationSetters<?> expectTasks(IScheduledTask... tasks) {
    return storageUtil.expectTaskFetch(ACTIVE_QUERY, tasks);
  }

  private IExpectationSetters<?> expectNoTasks() {
    return expectTasks();
  }

  private IExpectationSetters<Optional<IResourceAggregate>> expectQuota(IResourceAggregate quota) {
    return expect(storageUtil.quotaStore.fetchQuota(ROLE))
        .andReturn(Optional.of(quota));
  }

  private ITaskConfig createTaskConfig(int cpus, int ramMb, int diskMb, boolean production) {
    return createTask("newTask", "newId", cpus, ramMb, diskMb, production)
        .getAssignedTask()
        .getTask();
  }

  private IScheduledTask createTask(
      String jobName,
      String taskId,
      int cpus,
      int ramMb,
      int diskMb,
      boolean production) {

    return IScheduledTask.build(new ScheduledTask()
        .setStatus(ScheduleStatus.RUNNING)
        .setAssignedTask(
            new AssignedTask()
                .setTaskId(taskId)
                .setTask(new TaskConfig()
                    .setOwner(new Identity(ROLE, ROLE))
                    .setEnvironment(ENV)
                    .setJobName(jobName)
                    .setNumCpus(cpus)
                    .setRamMb(ramMb)
                    .setDiskMb(diskMb)
                    .setProduction(production))));
  }
}

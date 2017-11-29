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
package org.apache.aurora.scheduler.quota;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.ValueConstraint;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.quota.QuotaManager.QuotaException;
import org.apache.aurora.scheduler.quota.QuotaManager.QuotaManagerImpl;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.Resource.diskMb;
import static org.apache.aurora.gen.Resource.namedPort;
import static org.apache.aurora.gen.Resource.numCpus;
import static org.apache.aurora.gen.Resource.ramMb;
import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.INSUFFICIENT_QUOTA;
import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.SUFFICIENT_QUOTA;
import static org.apache.aurora.scheduler.quota.QuotaManager.QuotaManagerImpl.updateQuery;
import static org.apache.aurora.scheduler.resources.ResourceBag.EMPTY;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.aggregate;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.bag;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QuotaManagerImplTest extends EasyMockTest {
  private static final String ROLE = "test";
  private static final String ENV = "test_env";
  private static final String JOB_NAME = "job";
  private static final IJobUpdateKey UPDATE_KEY =
      IJobUpdateKey.build(new JobUpdateKey(JobKeys.from(ROLE, ENV, JOB_NAME).newBuilder(), "u1"));
  private static final IResourceAggregate QUOTA = aggregate(1.0, 100L, 200L);
  private static final Query.Builder ACTIVE_QUERY = Query.roleScoped(ROLE).active();

  private StorageTestUtil storageUtil;
  private JobUpdateStore jobUpdateStore;
  private QuotaManagerImpl quotaManager;
  private StoreProvider storeProvider;

  @Before
  public void setUp() throws Exception {
    storageUtil = new StorageTestUtil(this);
    storeProvider = storageUtil.storeProvider;
    jobUpdateStore = storageUtil.jobUpdateStore;
    quotaManager = new QuotaManagerImpl();
    storageUtil.expectOperations();
  }

  @Test
  public void testGetQuotaInfo() {
    IScheduledTask prodSharedTask = prodTask("foo1", 3, 3, 3);
    IScheduledTask prodDedicatedTask = prodDedicatedTask("foo2", 5, 5, 5);
    IScheduledTask nonProdSharedTask = nonProdTask("bar1", 2, 2, 2);
    IScheduledTask nonProdDedicatedTask = nonProdDedicatedTask("bar2", 7, 7, 7);

    expectQuota(aggregate(4, 4, 4));
    expectTasks(prodSharedTask, nonProdSharedTask, prodDedicatedTask, nonProdDedicatedTask);
    expectJobUpdates(taskConfig(1, 1, 1, true), taskConfig(1, 1, 1, true));
    expectCronJobs(
        createJob(prodTask("pc", 1, 1, 1), 2),
        createJob(nonProdTask("npc", 7, 7, 7), 1));

    control.replay();

    assertEquals(
        new QuotaInfo(bag(4, 4, 4), bag(6, 6, 6), bag(5, 5, 5), bag(9, 9, 9), bag(7, 7, 7)),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testGetQuotaInfoWithCronTasks() {
    IScheduledTask prodTask = prodTask("pc", 6, 6, 6);
    IScheduledTask nonProdTask = prodTask("npc", 7, 7, 7);

    expectQuota(aggregate(4, 4, 4));
    expectTasks(prodTask, nonProdTask);
    expectJobUpdates(taskConfig(1, 1, 1, true), taskConfig(1, 1, 1, true));

    final String pcRole = "pc-role";
    ScheduledTask ignoredProdTask = prodTask(pcRole, 20, 20, 20).newBuilder();
    ignoredProdTask.getAssignedTask().getTask()
        .setJob(new JobKey(pcRole, ENV, pcRole));

    final String npcRole = "npc-role";
    ScheduledTask ignoredNonProdTask = nonProdTask(npcRole, 20, 20, 20).newBuilder();
    ignoredNonProdTask.getAssignedTask().getTask()
        .setJob(new JobKey(npcRole, ENV, npcRole));

    expectCronJobs(
        createJob(prodTask("pc", 3, 3, 3), 1),
        createJob(nonProdTask("npc", 5, 5, 5), 2),
        createJob(IScheduledTask.build(ignoredProdTask), 2),
        createJob(IScheduledTask.build(ignoredNonProdTask), 3));

    control.replay();

    assertEquals(
        new QuotaInfo(bag(4, 4, 4), bag(7, 7, 7), EMPTY, bag(10, 10, 10), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testGetQuotaInfoPartialUpdate() {
    IScheduledTask prodTask = prodTask("foo", 3, 3, 3);
    IScheduledTask updatingProdTask = createTask(JOB_NAME, "id1", 3, 3, 3, true, 1);
    IScheduledTask updatingFilteredProdTask = createTask(JOB_NAME, "id0", 3, 3, 3, true, 0);
    IScheduledTask nonProdTask = createTask("bar", "id1", 2, 2, 2, false, 0);

    expectQuota(aggregate(4, 4, 4));
    expectTasks(prodTask, updatingProdTask, updatingFilteredProdTask, nonProdTask);
    expectJobUpdates(taskConfig(1, 1, 1, true), taskConfig(1, 1, 1, true));
    expectNoCronJobs();

    control.replay();

    // Expected consumption from: prodTask + updatingProdTask + job update.
    assertEquals(
        new QuotaInfo(bag(4, 4, 4), bag(7, 7, 7), EMPTY, bag(2, 2, 2), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testGetQuotaInfoNoTasksNoUpdatesNoCronJobs() {
    expectQuota(aggregate(4, 4, 4));
    expectNoTasks();
    expectNoJobUpdates();
    expectNoCronJobs();

    control.replay();

    assertEquals(
        new QuotaInfo(bag(4, 4, 4), bag(0, 0, 0), EMPTY, bag(0, 0, 0), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaPasses() {
    expectQuota(aggregate(4, 4, 4));
    expectTasks(prodTask("foo", 2, 2, 2));
    expectJobUpdates(taskConfig(1, 1, 1, true), taskConfig(1, 1, 1, true));
    expectNoCronJobs();

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaPassesNoTasks() {
    expectQuota(aggregate(4, 4, 4));
    expectNoTasks();
    expectJobUpdates(taskConfig(1, 1, 1, true), taskConfig(1, 1, 1, true));
    expectNoCronJobs();

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaPassesNoUpdates() {
    expectQuota(aggregate(4, 4, 4));
    expectTasks(prodTask("foo", 2, 2, 2));
    expectNoJobUpdates();
    expectNoCronJobs();

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaPassesNoTasksNoUpdates() {
    expectQuota(aggregate(4, 4, 4));
    expectNoTasks();
    expectNoJobUpdates();
    expectNoCronJobs();

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaPassesNonProdUnaccounted() {
    expectQuota(aggregate(4, 4, 4));
    expectTasks(prodTask("foo", 2, 2, 2), createTask("bar", "id2", 5, 5, 5, false, 0));

    expectNoJobUpdates();
    expectNoCronJobs();

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaPassesDedicatedUnaccounted() {
    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkInstanceAddition(
        prodDedicatedTask("dedicatedJob", 1, 1, 1).getAssignedTask().getTask(),
        1,
        storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaSkippedForNonProdRequest() {
    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, false), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaNoQuotaSet() {
    expect(storageUtil.quotaStore.fetchQuota(ROLE))
        .andReturn(Optional.absent());

    expectNoTasks();
    expectNoJobUpdates();
    expectNoCronJobs();

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaExceedsCpu() {
    expectQuota(aggregate(4, 4, 4));
    expectTasks(prodTask("foo", 3, 3, 3));
    expectNoJobUpdates();
    expectNoCronJobs();

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(2, 1, 1, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertTrue(checkQuota.getDetails().get().contains("CPU"));
  }

  @Test
  public void testCheckQuotaExceedsRam() {
    expectQuota(aggregate(4, 4, 4));
    expectTasks(prodTask("foo", 3, 3, 3));
    expectNoJobUpdates();
    expectNoCronJobs();

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 2, 1, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertTrue(checkQuota.getDetails().get().contains("RAM"));
  }

  @Test
  public void testCheckQuotaExceedsDisk() {
    expectQuota(aggregate(4, 4, 4));
    expectTasks(prodTask("foo", 3, 3, 3));
    expectNoJobUpdates();
    expectNoCronJobs();

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 2, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertTrue(checkQuota.getDetails().get().contains(ResourceType.DISK_MB.getAuroraName()));
  }

  @Test
  public void testCheckQuotaExceedsCron() {
    expectQuota(aggregate(5, 5, 5)).times(2);
    expectNoTasks().times(2);
    expectNoJobUpdates().times(2);
    expectCronJobs(
        createJob(prodTask("pc", 4, 4, 4), 1),
        createJob(nonProdTask("npc", 7, 7, 7), 1)).times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(2, 2, 2, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(bag(5, 5, 5), bag(4, 4, 4), EMPTY, bag(7, 7, 7), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaUpdatingTasksFilteredOut() {
    expectQuota(aggregate(5, 5, 5)).times(2);
    expectTasks(prodTask("foo", 2, 2, 2), createTask(JOB_NAME, "id2", 3, 3, 3, true, 0))
        .times(2);

    expectJobUpdates(taskConfig(1, 1, 1, true), taskConfig(2, 2, 2, true), 2);
    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(bag(5, 5, 5), bag(4, 4, 4), EMPTY, bag(0, 0, 0), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaNonProdUpdatesUnaccounted() {
    expectQuota(aggregate(5, 5, 5)).times(2);
    expectTasks(prodTask("foo", 2, 2, 2), prodTask("bar", 2, 2, 2)).times(2);

    expectJobUpdates(taskConfig(8, 8, 8, false), taskConfig(4, 4, 4, false), 2);
    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(bag(5, 5, 5), bag(4, 4, 4), EMPTY, bag(8, 8, 8), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaProdToNonUpdateUnaccounted() {
    expectQuota(aggregate(5, 5, 5)).times(2);
    expectTasks(prodTask("foo", 2, 2, 2), prodTask("bar", 1, 1, 1)).times(2);

    expectJobUpdates(taskConfig(1, 1, 1, true), taskConfig(7, 7, 7, false), 2);
    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(bag(5, 5, 5), bag(4, 4, 4), EMPTY, bag(7, 7, 7), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaNonToProdUpdateExceedsQuota() {
    expectQuota(aggregate(5, 5, 5)).times(2);
    expectTasks(prodTask("foo", 2, 2, 2), prodTask("bar", 2, 2, 2)).times(2);

    expectJobUpdates(taskConfig(1, 1, 1, false), taskConfig(1, 1, 1, true), 2);
    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(bag(5, 5, 5), bag(5, 5, 5), EMPTY, bag(1, 1, 1), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaOldJobUpdateConfigMatters() {
    expectQuota(aggregate(6, 6, 6)).times(2);
    expectTasks(prodTask("foo", 2, 2, 2), prodTask("bar", 2, 2, 2)).times(2);
    expectJobUpdates(taskConfig(2, 2, 2, true), taskConfig(1, 1, 1, true), 2);
    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(bag(6, 6, 6), bag(6, 6, 6), EMPTY, bag(0, 0, 0), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaUpdateAddsInstances() {
    expectQuota(aggregate(6, 6, 6)).times(2);
    expectTasks(prodTask("foo", 2, 2, 2), prodTask("bar", 2, 2, 2)).times(2);
    expectJobUpdates(taskConfig(1, 1, 1, true), 1, taskConfig(1, 1, 1, true), 2, 2);
    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(bag(6, 6, 6), bag(6, 6, 6), EMPTY, bag(0, 0, 0), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaUpdateRemovesInstances() {
    expectQuota(aggregate(6, 6, 6)).times(2);
    expectTasks(prodTask("foo", 2, 2, 2), prodTask("bar", 2, 2, 2)).times(2);
    expectJobUpdates(taskConfig(1, 1, 1, true), 2, taskConfig(1, 1, 1, true), 1, 2);
    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(bag(6, 6, 6), bag(6, 6, 6), EMPTY, bag(0, 0, 0), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaUpdateInitialConfigsUsedForFiltering() {
    expectQuota(aggregate(6, 6, 6)).times(2);
    expectTasks(prodTask("foo", 2, 2, 2), prodTask(JOB_NAME, 2, 2, 2)).times(2);

    ITaskConfig config = taskConfig(2, 2, 2, true);
    IJobUpdateDetails update = buildJobUpdate(UPDATE_KEY, config, 1, config, 1);
    JobUpdateDetails builder = update.newBuilder();
    builder.getUpdate().getInstructions().unsetDesiredState();
    expect(jobUpdateStore.fetchJobUpdates(updateQuery(config.getJob().getRole())))
        .andReturn(ImmutableList.of(IJobUpdateDetails.build(builder))).times(2);

    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(bag(6, 6, 6), bag(4, 4, 4), EMPTY, bag(0, 0, 0), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaUpdateDesiredConfigsUsedForFiltering() {
    expectQuota(aggregate(6, 6, 6)).times(2);
    expectTasks(prodTask("foo", 2, 2, 2), prodTask(JOB_NAME, 2, 2, 2)).times(2);

    ITaskConfig config = taskConfig(2, 2, 2, true);
    IJobUpdateDetails update = buildJobUpdate(UPDATE_KEY, config, 1, config, 1);
    JobUpdateDetails builder = update.newBuilder();
    builder.getUpdate().getInstructions().setInitialState(ImmutableSet.of());
    expect(jobUpdateStore.fetchJobUpdates(updateQuery(config.getJob().getRole())))
        .andReturn(ImmutableList.of(IJobUpdateDetails.build(builder))).times(2);

    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(bag(6, 6, 6), bag(4, 4, 4), EMPTY, bag(0, 0, 0), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaNoDesiredState() {
    expectQuota(aggregate(6, 6, 6)).times(2);
    expectTasks(prodTask("foo", 2, 2, 2), prodTask("bar", 2, 2, 2)).times(2);

    ITaskConfig config = taskConfig(2, 2, 2, true);
    IJobUpdateDetails update = buildJobUpdate(UPDATE_KEY, config, 1, config, 1);
    JobUpdateDetails builder = update.newBuilder();
    builder.getUpdate().getInstructions().unsetDesiredState();
    expect(jobUpdateStore.fetchJobUpdates(updateQuery(config.getJob().getRole())))
        .andReturn(ImmutableList.of(IJobUpdateDetails.build(builder))).times(2);

    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(bag(6, 6, 6), bag(6, 6, 6), EMPTY, bag(0, 0, 0), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaNewInPlaceUpdate() {
    expectQuota(aggregate(6, 6, 6)).times(2);
    expectTasks(
        prodTask("foo", 2, 2, 2),
        createTask(JOB_NAME, "id1", 2, 2, 2, true, 0),
        createTask(JOB_NAME, "id12", 2, 2, 2, true, 12)).times(2);
    expectNoJobUpdates().times(2);

    ITaskConfig config = taskConfig(1, 1, 1, true);
    IJobUpdateDetails update = buildJobUpdate(
        UPDATE_KEY,
        taskConfig(2, 2, 2, true),
        1,
        config,
        1);

    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkJobUpdate(update.getUpdate(), storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(bag(6, 6, 6), bag(6, 6, 6), EMPTY, bag(0, 0, 0), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaNewUpdateAddsInstances() {
    expectQuota(aggregate(6, 6, 6)).times(2);
    expectTasks(prodTask("foo", 2, 2, 2), prodTask(JOB_NAME, 2, 2, 2)).times(2);
    expectNoJobUpdates().times(2);

    ITaskConfig config = taskConfig(2, 2, 2, true);
    IJobUpdateDetails update = buildJobUpdate(
        UPDATE_KEY,
        config,
        1,
        config,
        3);

    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkJobUpdate(update.getUpdate(), storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(bag(6, 6, 6), bag(4, 4, 4), EMPTY, bag(0, 0, 0), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaNewUpdateRemovesInstances() {
    expectQuota(aggregate(6, 6, 6)).times(2);
    expectTasks(
        prodTask("foo", 2, 2, 2),
        createTask(JOB_NAME, "id1", 2, 2, 2, true, 0),
        createTask(JOB_NAME, "id2", 2, 2, 2, true, 1)).times(2);
    expectNoJobUpdates().times(2);

    ITaskConfig config = taskConfig(2, 2, 2, true);
    IJobUpdateDetails update = buildJobUpdate(
        UPDATE_KEY,
        config,
        1,
        config,
        1);

    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkJobUpdate(update.getUpdate(), storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(bag(6, 6, 6), bag(6, 6, 6), EMPTY, bag(0, 0, 0), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaNewUpdateSkippedForNonProdDesiredState() {
    ITaskConfig config = taskConfig(2, 2, 2, false);
    IJobUpdateDetails update = buildJobUpdate(
        UPDATE_KEY,
        taskConfig(2, 2, 2, true),
        1,
        config,
        1);

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkJobUpdate(update.getUpdate(), storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaNewUpdateSkippedForDedicatedDesiredState() {
    ITaskConfig config = taskConfig(2, 2, 2, false);
    IJobUpdateDetails update = buildJobUpdate(
        UPDATE_KEY,
        prodDedicatedTask("dedicatedJob", 1, 1, 1).getAssignedTask().getTask(),
        1,
        config,
        1);

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkJobUpdate(update.getUpdate(), storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaNewUpdateSkippedForEmptyDesiredState() {
    ITaskConfig config = taskConfig(2, 2, 2, true);
    IJobUpdateDetails update = buildJobUpdate(
        UPDATE_KEY,
        config,
        1,
        config,
        1);
    JobUpdate updateBuilder = update.getUpdate().newBuilder();
    updateBuilder.getInstructions().unsetDesiredState();

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkJobUpdate(IJobUpdate.build(updateBuilder), storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testSaveQuotaPasses() throws Exception {
    expectNoJobUpdates();
    expectNoCronJobs();
    IScheduledTask prodTask = prodTask("foo", 1, 1, 1);
    expectTasks(prodTask);
    expectQuota(aggregate(1, 1, 1));

    storageUtil.quotaStore.saveQuota(ROLE, QUOTA);

    control.replay();
    quotaManager.saveQuota(
        ROLE,
        QUOTA,
        storageUtil.mutableStoreProvider);
  }

  @Test
  public void testRemoveQuota() throws Exception {
    expectNoJobUpdates();
    expectNoCronJobs();
    expectNoTasks();
    expectQuota(aggregate(1, 1, 1));

    storageUtil.quotaStore.saveQuota(ROLE, aggregate(0, 0, 0));

    control.replay();
    quotaManager.saveQuota(ROLE, aggregate(0, 0, 0), storageUtil.mutableStoreProvider);
  }

  @Test(expected = QuotaException.class)
  public void testSaveQuotaFailsNegativeValues() throws Exception {
    control.replay();
    quotaManager.saveQuota(ROLE, aggregate(-2.0, 4, 5), storageUtil.mutableStoreProvider);
  }

  @Test(expected = QuotaException.class)
  public void testSaveQuotaFailsWhenBelowCurrentReservation() throws Exception {
    expectNoJobUpdates();
    expectNoCronJobs();
    IScheduledTask prodTask = prodTask("foo", 10, 100, 100);
    expectTasks(prodTask);
    expectQuota(aggregate(20, 200, 200));

    control.replay();

    quotaManager.saveQuota(ROLE, aggregate(1, 1, 1), storageUtil.mutableStoreProvider);
  }

  @Test
  public void testCheckQuotaCronUpdateDownsize() {
    expectQuota(aggregate(5, 5, 5)).times(2);
    expectNoTasks().times(2);
    expectNoJobUpdates().times(2);

    IJobConfiguration job = createJob(prodTask("pc", 4, 4, 4), 1);
    expectCronJobs(job, createJob(nonProdTask("npc", 7, 7, 7), 1)).times(2);
    expectCronJob(job);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkCronUpdate(createJob(prodTask("pc", 1, 1, 1), 2), storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(bag(5, 5, 5), bag(4, 4, 4), EMPTY, bag(7, 7, 7), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaCronUpdateUpsize() {
    expectQuota(aggregate(5, 5, 5)).times(2);
    expectNoTasks().times(2);
    expectNoJobUpdates().times(2);

    IJobConfiguration job = createJob(prodTask("pc", 4, 4, 4), 1);
    expectCronJobs(job, createJob(nonProdTask("npc", 7, 7, 7), 1)).times(2);
    expectCronJob(job);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkCronUpdate(createJob(prodTask("pc", 5, 5, 5), 1), storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(bag(5, 5, 5), bag(4, 4, 4), EMPTY, bag(7, 7, 7), EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaCronUpdateFails() {
    expectQuota(aggregate(5, 5, 5)).times(2);
    expectNoTasks().times(2);
    expectNoJobUpdates().times(2);

    IJobConfiguration job = createJob(prodTask("pc", 4, 4, 4), 1);
    expectCronJobs(job).times(2);
    expectCronJob(job);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkCronUpdate(createJob(prodTask("pc", 5, 5, 5), 2), storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(bag(5, 5, 5), bag(4, 4, 4), EMPTY, EMPTY, EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaCronCreate() {
    expectQuota(aggregate(5, 5, 5)).times(2);
    expectNoTasks().times(2);
    expectNoJobUpdates().times(2);
    expectNoCronJobs().times(2);
    expectNoCronJob();

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkCronUpdate(createJob(prodTask("pc", 5, 5, 5), 1), storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(bag(5, 5, 5), EMPTY, EMPTY, EMPTY, EMPTY),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaNonProdCron() {
    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkCronUpdate(createJob(nonProdTask("np", 5, 5, 5), 1), storeProvider);

    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  private IExpectationSetters<?> expectTasks(IScheduledTask... tasks) {
    return storageUtil.expectTaskFetch(ACTIVE_QUERY, tasks);
  }

  private void expectJobUpdates(ITaskConfig initial, ITaskConfig desired) {
    expectJobUpdates(initial, 1, desired, 1, 1);
  }

  private void expectJobUpdates(ITaskConfig initial, ITaskConfig desired, int times) {
    expectJobUpdates(initial, 1, desired, 1, times);
  }

  private void expectJobUpdates(
      ITaskConfig initial,
      int initialInstances,
      ITaskConfig desired,
      int desiredInstances,
      int times) {

    IJobUpdateKey key = IJobUpdateKey.build(new JobUpdateKey(initial.getJob().newBuilder(), "u1"));
    expect(jobUpdateStore.fetchJobUpdates(updateQuery(initial.getJob().getRole())))
        .andReturn(ImmutableList.of(
            buildJobUpdate(key, initial, initialInstances, desired, desiredInstances)))
        .times(times);
  }

  private IJobUpdateDetails buildJobUpdate(
      IJobUpdateKey key,
      ITaskConfig initial,
      int intialInstances,
      ITaskConfig desired,
      int desiredInstances) {

    return IJobUpdateDetails.build(new JobUpdateDetails()
        .setUpdate(new JobUpdate()
            .setSummary(new JobUpdateSummary().setKey(key.newBuilder()))
            .setInstructions(new JobUpdateInstructions()
                .setDesiredState(new InstanceTaskConfig()
                    .setTask(desired.newBuilder())
                    .setInstances(ImmutableSet.of(new Range(0, desiredInstances - 1))))
                .setInitialState(ImmutableSet.of(new InstanceTaskConfig()
                    .setTask(initial.newBuilder())
                    .setInstances(ImmutableSet.of(new Range(0, intialInstances - 1))))))));
  }

  private IExpectationSetters<?> expectNoJobUpdates() {
    return expect(jobUpdateStore.fetchJobUpdates(updateQuery(ROLE))).andReturn(ImmutableList.of());
  }

  private IExpectationSetters<?> expectNoTasks() {
    return expectTasks();
  }

  private IExpectationSetters<?> expectNoCronJobs() {
    return expect(storageUtil.jobStore.fetchJobs()).andReturn(ImmutableSet.of());
  }

  private IExpectationSetters<?> expectCronJobs(IJobConfiguration... jobs) {
    ImmutableSet.Builder<IJobConfiguration> builder = ImmutableSet.builder();
    for (IJobConfiguration job : jobs) {
      builder.add(job);
    }

    return expect(storageUtil.jobStore.fetchJobs()).andReturn(builder.build());
  }

  private IExpectationSetters<?> expectCronJob(IJobConfiguration job) {
    return expect(storageUtil.jobStore.fetchJob(job.getKey())).andReturn(Optional.of(job));
  }

  private IExpectationSetters<?> expectNoCronJob() {
    return expect(storageUtil.jobStore.fetchJob(anyObject(IJobKey.class)))
        .andReturn(Optional.absent());
  }

  private IExpectationSetters<Optional<IResourceAggregate>> expectQuota(IResourceAggregate quota) {
    return expect(storageUtil.quotaStore.fetchQuota(ROLE))
        .andReturn(Optional.of(quota));
  }

  private ITaskConfig taskConfig(int cpus, int ramMb, int diskMb, boolean production) {
    return createTask(JOB_NAME, "newId", cpus, ramMb, diskMb, production, 0)
        .getAssignedTask()
        .getTask();
  }

  private IScheduledTask prodTask(String jobName, int cpus, int ramMb, int diskMb) {
    return createTask(jobName, jobName + "id1", cpus, ramMb, diskMb, true, 0);
  }

  private IScheduledTask prodDedicatedTask(String jobName, int cpus, int ramMb, int diskMb) {
    return makeDedicated(prodTask(jobName, cpus, ramMb, diskMb));
  }

  private IScheduledTask nonProdDedicatedTask(String jobName, int cpus, int ramMb, int diskMb) {
    return makeDedicated(nonProdTask(jobName, cpus, ramMb, diskMb));
  }

  private static IScheduledTask makeDedicated(IScheduledTask task) {
    ScheduledTask builder = task.newBuilder();
    builder.getAssignedTask().getTask().setConstraints(ImmutableSet.of(
        new Constraint(
            "dedicated",
            TaskConstraint.value(new ValueConstraint(false, ImmutableSet.of("host"))))));
    return IScheduledTask.build(builder);
  }

  private IScheduledTask nonProdTask(String jobName, int cpus, int ramMb, int diskMb) {
    return createTask(jobName, jobName + "id1", cpus, ramMb, diskMb, false, 0);
  }

  private IScheduledTask createTask(
      String jobName,
      String taskId,
      int cpus,
      int ramMb,
      int diskMb,
      boolean production,
      int instanceId) {

    ScheduledTask builder = TaskTestUtil.makeTask(taskId, JobKeys.from(ROLE, ENV, jobName))
        .newBuilder();
    builder.getAssignedTask().setInstanceId(instanceId);
    builder.getAssignedTask().getTask()
        .setResources(ImmutableSet.of(numCpus(cpus), ramMb(ramMb), diskMb(diskMb), namedPort("a")))
        .setProduction(production);
    return IScheduledTask.build(builder);
  }

  private IJobConfiguration createJob(IScheduledTask scheduledTask, int instanceCount) {
    TaskConfig task = scheduledTask.newBuilder().getAssignedTask().getTask();
    return IJobConfiguration.build(new JobConfiguration()
        .setKey(task.getJob())
        .setTaskConfig(task)
        .setInstanceCount(instanceCount));
  }
}

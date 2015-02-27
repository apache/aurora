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

import java.util.List;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.quota.QuotaManager.QuotaException;
import org.apache.aurora.scheduler.quota.QuotaManager.QuotaManagerImpl;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.INSUFFICIENT_QUOTA;
import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.SUFFICIENT_QUOTA;
import static org.apache.aurora.scheduler.quota.QuotaManager.QuotaManagerImpl.updateQuery;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QuotaManagerImplTest extends EasyMockTest {
  private static final String ROLE = "test";
  private static final String ENV = "test_env";
  private static final String JOB_NAME = "job";
  private static final IJobUpdateKey UPDATE_KEY =
      IJobUpdateKey.build(new JobUpdateKey(JobKeys.from(ROLE, ENV, JOB_NAME).newBuilder(), "u1"));
  private static final IResourceAggregate QUOTA = IResourceAggregate.build(new ResourceAggregate()
      .setNumCpus(1.0)
      .setRamMb(100L)
      .setDiskMb(200L));
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
    IScheduledTask prodTask = createProdTask("foo", 3, 3, 3);
    IScheduledTask nonProdTask = createTask("bar", "id1", 2, 2, 2, false, 0);
    IResourceAggregate quota = IResourceAggregate.build(new ResourceAggregate(4, 4, 4));

    expectQuota(quota);
    expectTasks(prodTask, nonProdTask);
    expectJobUpdates(taskConfig(1, 1, 1, true), taskConfig(1, 1, 1, true));
    expectCronJobs(
        createJob(createProdTask("pc", 1, 1, 1), 2),
        createJob(createNonProdTask("npc", 7, 7, 7), 1));

    control.replay();

    assertEquals(
        new QuotaInfo(from(4, 4, 4), from(6, 6, 6), from(9, 9, 9)),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testGetQuotaInfoWithCronTasks() {
    IScheduledTask prodTask = createProdTask("pc", 6, 6, 6);
    IScheduledTask nonProdTask = createProdTask("npc", 7, 7, 7);
    IResourceAggregate quota = IResourceAggregate.build(new ResourceAggregate(4, 4, 4));

    expectQuota(quota);
    expectTasks(prodTask, nonProdTask);
    expectJobUpdates(taskConfig(1, 1, 1, true), taskConfig(1, 1, 1, true));

    final String pcRole = "pc-role";
    ScheduledTask ignoredProdTask = createProdTask(pcRole, 20, 20, 20).newBuilder();
    ignoredProdTask.getAssignedTask().getTask()
        .setOwner(new Identity(pcRole, "ignored"))
        .setJob(new JobKey(pcRole, ENV, pcRole));

    final String npcRole = "npc-role";
    ScheduledTask ignoredNonProdTask = createNonProdTask(npcRole, 20, 20, 20).newBuilder();
    ignoredNonProdTask.getAssignedTask().getTask()
        .setOwner(new Identity(npcRole, "ignored"))
        .setJob(new JobKey(npcRole, ENV, npcRole));

    expectCronJobs(
        createJob(createProdTask("pc", 3, 3, 3), 1),
        createJob(createNonProdTask("npc", 5, 5, 5), 2),
        createJob(IScheduledTask.build(ignoredProdTask), 2),
        createJob(IScheduledTask.build(ignoredNonProdTask), 3));

    control.replay();

    assertEquals(
        new QuotaInfo(from(4, 4, 4), from(7, 7, 7), from(10, 10, 10)),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testGetQuotaInfoPartialUpdate() {
    IScheduledTask prodTask = createProdTask("foo", 3, 3, 3);
    IScheduledTask updatingProdTask = createTask(JOB_NAME, "id1", 3, 3, 3, true, 1);
    IScheduledTask updatingFilteredProdTask = createTask(JOB_NAME, "id0", 3, 3, 3, true, 0);
    IScheduledTask nonProdTask = createTask("bar", "id1", 2, 2, 2, false, 0);
    IResourceAggregate quota = IResourceAggregate.build(new ResourceAggregate(4, 4, 4));

    expectQuota(quota);
    expectTasks(prodTask, updatingProdTask, updatingFilteredProdTask, nonProdTask);
    expectJobUpdates(taskConfig(1, 1, 1, true), taskConfig(1, 1, 1, true));
    expectNoCronJobs();

    control.replay();

    // Expected consumption from: prodTask + updatingProdTask + job update.
    assertEquals(
        new QuotaInfo(from(4, 4, 4), from(7, 7, 7), from(2, 2, 2)),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testGetQuotaInfoNoTasksNoUpdatesNoCronJobs() {
    IResourceAggregate quota = IResourceAggregate.build(new ResourceAggregate(4, 4, 4));

    expectQuota(quota);
    expectNoTasks();
    expectNoJobUpdates();
    expectNoCronJobs();

    control.replay();

    assertEquals(
        new QuotaInfo(from(4, 4, 4), from(0, 0, 0), from(0, 0, 0)),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaPasses() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(4, 4, 4)));
    expectTasks(createProdTask("foo", 2, 2, 2));
    expectJobUpdates(taskConfig(1, 1, 1, true), taskConfig(1, 1, 1, true));
    expectNoCronJobs();

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaPassesNoTasks() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(4, 4, 4)));
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
    expectQuota(IResourceAggregate.build(new ResourceAggregate(4, 4, 4)));
    expectTasks(createProdTask("foo", 2, 2, 2));
    expectNoJobUpdates();
    expectNoCronJobs();

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaPassesNoTasksNoUpdates() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(4, 4, 4)));
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
    expectQuota(IResourceAggregate.build(new ResourceAggregate(4, 4, 4)));
    expectTasks(createProdTask("foo", 2, 2, 2), createTask("bar", "id2", 5, 5, 5, false, 0));

    expectNoJobUpdates();
    expectNoCronJobs();

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
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
        .andReturn(Optional.<IResourceAggregate>absent());

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
    expectQuota(IResourceAggregate.build(new ResourceAggregate(4, 4, 4)));
    expectTasks(createProdTask("foo", 3, 3, 3));
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
    expectQuota(IResourceAggregate.build(new ResourceAggregate(4, 4, 4)));
    expectTasks(createProdTask("foo", 3, 3, 3));
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
    expectQuota(IResourceAggregate.build(new ResourceAggregate(4, 4, 4)));
    expectTasks(createProdTask("foo", 3, 3, 3));
    expectNoJobUpdates();
    expectNoCronJobs();

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 2, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertTrue(checkQuota.getDetails().get().contains("DISK"));
  }

  @Test
  public void testCheckQuotaExceedsCron() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(5, 5, 5))).times(2);
    expectNoTasks().times(2);
    expectNoJobUpdates().times(2);
    expectCronJobs(
        createJob(createProdTask("pc", 4, 4, 4), 1),
        createJob(createNonProdTask("npc", 7, 7, 7), 1)).times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(2, 2, 2, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(5, 5, 5), from(4, 4, 4), from(7, 7, 7)),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaUpdatingTasksFilteredOut() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(5, 5, 5))).times(2);
    expectTasks(createProdTask("foo", 2, 2, 2), createTask(JOB_NAME, "id2", 3, 3, 3, true, 0))
        .times(2);

    expectJobUpdates(taskConfig(1, 1, 1, true), taskConfig(2, 2, 2, true), 2);
    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(5, 5, 5), from(4, 4, 4), from(0, 0, 0)),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaNonProdUpdatesUnaccounted() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(5, 5, 5))).times(2);
    expectTasks(createProdTask("foo", 2, 2, 2), createProdTask("bar", 2, 2, 2)).times(2);

    expectJobUpdates(taskConfig(8, 8, 8, false), taskConfig(4, 4, 4, false), 2);
    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(5, 5, 5), from(4, 4, 4), from(8, 8, 8)),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaProdToNonUpdateUnaccounted() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(5, 5, 5))).times(2);
    expectTasks(createProdTask("foo", 2, 2, 2), createProdTask("bar", 1, 1, 1)).times(2);

    expectJobUpdates(taskConfig(1, 1, 1, true), taskConfig(7, 7, 7, false), 2);
    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(5, 5, 5), from(4, 4, 4), from(7, 7, 7)),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaNonToProdUpdateExceedsQuota() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(5, 5, 5))).times(2);
    expectTasks(createProdTask("foo", 2, 2, 2), createProdTask("bar", 2, 2, 2)).times(2);

    expectJobUpdates(taskConfig(1, 1, 1, false), taskConfig(1, 1, 1, true), 2);
    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(5, 5, 5), from(5, 5, 5), from(1, 1, 1)),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaOldJobUpdateConfigMatters() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(6, 6, 6))).times(2);
    expectTasks(createProdTask("foo", 2, 2, 2), createProdTask("bar", 2, 2, 2)).times(2);
    expectJobUpdates(taskConfig(2, 2, 2, true), taskConfig(1, 1, 1, true), 2);
    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(6, 6, 6), from(6, 6, 6), from(0, 0, 0)),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaUpdateAddsInstances() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(6, 6, 6))).times(2);
    expectTasks(createProdTask("foo", 2, 2, 2), createProdTask("bar", 2, 2, 2)).times(2);
    expectJobUpdates(taskConfig(1, 1, 1, true), 1, taskConfig(1, 1, 1, true), 2, 2);
    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(6, 6, 6), from(6, 6, 6), from(0, 0, 0)),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaUpdateRemovesInstances() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(6, 6, 6))).times(2);
    expectTasks(createProdTask("foo", 2, 2, 2), createProdTask("bar", 2, 2, 2)).times(2);
    expectJobUpdates(taskConfig(1, 1, 1, true), 2, taskConfig(1, 1, 1, true), 1, 2);
    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(6, 6, 6), from(6, 6, 6), from(0, 0, 0)),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaUpdateInitialConfigsUsedForFiltering() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(6, 6, 6))).times(2);
    expectTasks(createProdTask("foo", 2, 2, 2), createProdTask(JOB_NAME, 2, 2, 2)).times(2);

    ITaskConfig config = taskConfig(2, 2, 2, true);
    List<IJobUpdateSummary> summaries = buildJobUpdateSummaries(UPDATE_KEY);
    IJobUpdate update = buildJobUpdate(summaries.get(0), config, 1, config, 1);
    JobUpdate builder = update.newBuilder();
    builder.getInstructions().unsetDesiredState();

    expect(jobUpdateStore.fetchJobUpdateSummaries(updateQuery(config.getJob().getRole())))
        .andReturn(summaries).times(2);

    expect(jobUpdateStore.fetchJobUpdate(UPDATE_KEY))
        .andReturn(Optional.of(IJobUpdate.build(builder))).times(2);

    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(6, 6, 6), from(4, 4, 4), from(0, 0, 0)),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaUpdateDesiredConfigsUsedForFiltering() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(6, 6, 6))).times(2);
    expectTasks(createProdTask("foo", 2, 2, 2), createProdTask(JOB_NAME, 2, 2, 2)).times(2);

    ITaskConfig config = taskConfig(2, 2, 2, true);
    List<IJobUpdateSummary> summaries = buildJobUpdateSummaries(UPDATE_KEY);
    IJobUpdate update = buildJobUpdate(summaries.get(0), config, 1, config, 1);
    JobUpdate builder = update.newBuilder();
    builder.getInstructions().setInitialState(ImmutableSet.<InstanceTaskConfig>of());

    expect(jobUpdateStore.fetchJobUpdateSummaries(updateQuery(config.getJob().getRole())))
        .andReturn(summaries).times(2);

    expect(jobUpdateStore.fetchJobUpdate(UPDATE_KEY))
        .andReturn(Optional.of(IJobUpdate.build(builder))).times(2);

    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(6, 6, 6), from(4, 4, 4), from(0, 0, 0)),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaNoDesiredState() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(6, 6, 6))).times(2);
    expectTasks(createProdTask("foo", 2, 2, 2), createProdTask("bar", 2, 2, 2)).times(2);

    ITaskConfig config = taskConfig(2, 2, 2, true);
    List<IJobUpdateSummary> summaries = buildJobUpdateSummaries(UPDATE_KEY);
    IJobUpdate update = buildJobUpdate(summaries.get(0), config, 1, config, 1);
    JobUpdate builder = update.newBuilder();
    builder.getInstructions().unsetDesiredState();

    expect(jobUpdateStore.fetchJobUpdateSummaries(updateQuery(config.getJob().getRole())))
        .andReturn(summaries).times(2);

    expect(jobUpdateStore.fetchJobUpdate(UPDATE_KEY))
        .andReturn(Optional.of(IJobUpdate.build(builder))).times(2);

    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkInstanceAddition(taskConfig(1, 1, 1, true), 1, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(6, 6, 6), from(6, 6, 6), from(0, 0, 0)),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaNewInPlaceUpdate() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(6, 6, 6))).times(2);
    expectTasks(
        createProdTask("foo", 2, 2, 2),
        createTask(JOB_NAME, "id1", 2, 2, 2, true, 0),
        createTask(JOB_NAME, "id12", 2, 2, 2, true, 12)).times(2);
    expectNoJobUpdates().times(2);

    ITaskConfig config = taskConfig(1, 1, 1, true);
    IJobUpdate update = buildJobUpdate(
        buildJobUpdateSummaries(UPDATE_KEY).get(0),
        taskConfig(2, 2, 2, true),
        1,
        config,
        1);

    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkJobUpdate(update, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(6, 6, 6), from(6, 6, 6), from(0, 0, 0)),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaNewUpdateAddsInstances() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(6, 6, 6))).times(2);
    expectTasks(createProdTask("foo", 2, 2, 2), createProdTask(JOB_NAME, 2, 2, 2)).times(2);
    expectNoJobUpdates().times(2);

    ITaskConfig config = taskConfig(2, 2, 2, true);
    IJobUpdate update = buildJobUpdate(
        buildJobUpdateSummaries(UPDATE_KEY).get(0),
        config,
        1,
        config,
        3);

    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkJobUpdate(update, storeProvider);
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(6, 6, 6), from(4, 4, 4), from(0, 0, 0)),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaNewUpdateRemovesInstances() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(6, 6, 6))).times(2);
    expectTasks(
        createProdTask("foo", 2, 2, 2),
        createTask(JOB_NAME, "id1", 2, 2, 2, true, 0),
        createTask(JOB_NAME, "id2", 2, 2, 2, true, 1)).times(2);
    expectNoJobUpdates().times(2);

    ITaskConfig config = taskConfig(2, 2, 2, true);
    IJobUpdate update = buildJobUpdate(
        buildJobUpdateSummaries(UPDATE_KEY).get(0),
        config,
        1,
        config,
        1);

    expectNoCronJobs().times(2);

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkJobUpdate(update, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        new QuotaInfo(from(6, 6, 6), from(6, 6, 6), from(0, 0, 0)),
        quotaManager.getQuotaInfo(ROLE, storeProvider));
  }

  @Test
  public void testCheckQuotaNewUpdateSkippedForNonProdDesiredState() {
    ITaskConfig config = taskConfig(2, 2, 2, false);
    IJobUpdate update = buildJobUpdate(
        buildJobUpdateSummaries(UPDATE_KEY).get(0),
        taskConfig(2, 2, 2, true),
        1,
        config,
        1);

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkJobUpdate(update, storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaNewUpdateSkippedForEmptyDesiredState() {
    ITaskConfig config = taskConfig(2, 2, 2, true);
    IJobUpdate update = buildJobUpdate(
        buildJobUpdateSummaries(UPDATE_KEY).get(0),
        config,
        1,
        config,
        1);
    JobUpdate updateBuilder = update.newBuilder();
    updateBuilder.getInstructions().unsetDesiredState();

    control.replay();

    QuotaCheckResult checkQuota =
        quotaManager.checkJobUpdate(IJobUpdate.build(updateBuilder), storeProvider);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testSaveQuotaPasses() throws Exception {
    storageUtil.quotaStore.saveQuota(ROLE, QUOTA);

    control.replay();
    quotaManager.saveQuota(ROLE, QUOTA, storageUtil.mutableStoreProvider.getQuotaStore());
  }

  @Test(expected = QuotaException.class)
  public void testSaveQuotaFailsMissingSpecs() throws Exception {
    control.replay();
    quotaManager.saveQuota(
        ROLE,
        IResourceAggregate.build(new ResourceAggregate()),
        storageUtil.mutableStoreProvider.getQuotaStore());
  }

  @Test(expected = QuotaException.class)
  public void testSaveQuotaFailsNegativeValues() throws Exception {
    control.replay();
    quotaManager.saveQuota(
        ROLE,
        IResourceAggregate.build(new ResourceAggregate(-2.0, 4, 5)),
        storageUtil.mutableStoreProvider.getQuotaStore());
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
      int intialInstances,
      ITaskConfig desired,
      int desiredInstances,
      int times) {

    IJobUpdateKey key = IJobUpdateKey.build(new JobUpdateKey(initial.getJob().newBuilder(), "u1"));
    List<IJobUpdateSummary> summaries = buildJobUpdateSummaries(key);
    IJobUpdate update =
        buildJobUpdate(summaries.get(0), initial, intialInstances, desired, desiredInstances);

    expect(jobUpdateStore.fetchJobUpdateSummaries(updateQuery(initial.getJob().getRole())))
        .andReturn(summaries)
        .times(times);

    expect(jobUpdateStore.fetchJobUpdate(key)).andReturn(Optional.of(update)).times(times);

  }

  private List<IJobUpdateSummary> buildJobUpdateSummaries(IJobUpdateKey key) {
    return ImmutableList.of(IJobUpdateSummary.build(
        new JobUpdateSummary().setKey(key.newBuilder())));
  }

  private IJobUpdate buildJobUpdate(
      IJobUpdateSummary summary,
      ITaskConfig initial,
      int intialInstances,
      ITaskConfig desired,
      int desiredInstances) {

    return IJobUpdate.build(new JobUpdate()
        .setSummary(summary.newBuilder())
        .setInstructions(new JobUpdateInstructions()
            .setDesiredState(new InstanceTaskConfig()
                .setTask(desired.newBuilder())
                .setInstances(ImmutableSet.of(new Range(0, desiredInstances - 1))))
            .setInitialState(ImmutableSet.of(new InstanceTaskConfig()
                .setTask(initial.newBuilder())
                .setInstances(ImmutableSet.of(new Range(0, intialInstances - 1)))))));
  }

  private IExpectationSetters<?> expectNoJobUpdates() {
    return expect(jobUpdateStore.fetchJobUpdateSummaries(
        QuotaManagerImpl.updateQuery(ROLE))).andReturn(ImmutableList.<IJobUpdateSummary>of());
  }

  private IExpectationSetters<?> expectNoTasks() {
    return expectTasks();
  }

  private IExpectationSetters<?> expectNoCronJobs() {
    return expect(storageUtil.jobStore.fetchJobs()).andReturn(ImmutableSet.<IJobConfiguration>of());
  }

  private IExpectationSetters<?> expectCronJobs(IJobConfiguration... jobs) {
    ImmutableSet.Builder<IJobConfiguration> builder = ImmutableSet.builder();
    for (IJobConfiguration job : jobs) {
      builder.add(job);
    }

    return expect(storageUtil.jobStore.fetchJobs()).andReturn(builder.build());
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

  private IScheduledTask createProdTask(String jobName, int cpus, int ramMb, int diskMb) {
    return createTask(jobName, jobName + "id1", cpus, ramMb, diskMb, true, 0);
  }

  private IScheduledTask createNonProdTask(String jobName, int cpus, int ramMb, int diskMb) {
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

    return IScheduledTask.build(new ScheduledTask()
        .setStatus(ScheduleStatus.RUNNING)
        .setAssignedTask(
            new AssignedTask()
                .setTaskId(taskId)
                .setInstanceId(instanceId)
                .setTask(new TaskConfig()
                    .setJob(new JobKey(ROLE, ENV, jobName))
                    .setOwner(new Identity(ROLE, ROLE))
                    .setEnvironment(ENV)
                    .setJobName(jobName)
                    .setNumCpus(cpus)
                    .setRamMb(ramMb)
                    .setDiskMb(diskMb)
                    .setProduction(production))));
  }

  private IJobConfiguration createJob(IScheduledTask scheduledTask, int instanceCount) {
    TaskConfig task = scheduledTask.newBuilder().getAssignedTask().getTask();
    return IJobConfiguration.build(new JobConfiguration()
        .setKey(task.getJob())
        .setTaskConfig(task)
        .setInstanceCount(instanceCount));
  }

  private static IResourceAggregate from(double cpu, int ramMb, int diskMb) {
    return IResourceAggregate.build(new ResourceAggregate(cpu, ramMb, diskMb));
  }
}

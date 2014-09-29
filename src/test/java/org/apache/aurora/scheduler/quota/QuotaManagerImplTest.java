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
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.ResourceAggregates;
import org.apache.aurora.scheduler.quota.QuotaManager.QuotaException;
import org.apache.aurora.scheduler.quota.QuotaManager.QuotaManagerImpl;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
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
  private static final IResourceAggregate QUOTA = IResourceAggregate.build(new ResourceAggregate()
      .setNumCpus(1.0)
      .setRamMb(100L)
      .setDiskMb(200L));
  private static final Query.Builder ACTIVE_QUERY = Query.roleScoped(ROLE).active();

  private StorageTestUtil storageUtil;
  private JobUpdateStore jobUpdateStore;
  private QuotaManagerImpl quotaManager;

  @Before
  public void setUp() throws Exception {
    storageUtil = new StorageTestUtil(this);
    jobUpdateStore = storageUtil.jobUpdateStore;
    quotaManager = new QuotaManagerImpl(storageUtil.storage);
    storageUtil.expectOperations();
  }

  @Test
  public void testGetQuotaInfo() {
    IScheduledTask prodTask = createProdTask("foo", 3, 3, 3);
    IScheduledTask nonProdTask = createTask("bar", "id1", 2, 2, 2, false, 0);
    IResourceAggregate quota = IResourceAggregate.build(new ResourceAggregate(4, 4, 4));

    expectQuota(quota);
    expectTasks(prodTask, nonProdTask);
    expectJobUpdates(createTaskConfig(1, 1, 1, true), createTaskConfig(1, 1, 1, true));

    control.replay();

    QuotaInfo quotaInfo = quotaManager.getQuotaInfo(ROLE);
    assertEquals(quota, quotaInfo.getQuota());
    assertEquals(
        IResourceAggregate.build(new ResourceAggregate(4, 4, 4)), quotaInfo.getProdConsumption());
    assertEquals(
        IResourceAggregate.build(new ResourceAggregate(2, 2, 2)),
        quotaInfo.getNonProdConsumption());
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
    expectJobUpdates(createTaskConfig(1, 1, 1, true), createTaskConfig(1, 1, 1, true));

    control.replay();

    QuotaInfo quotaInfo = quotaManager.getQuotaInfo(ROLE);
    assertEquals(quota, quotaInfo.getQuota());

    // Expected consumption from: prodTask + updatingProdTask + job update.
    assertEquals(
        IResourceAggregate.build(new ResourceAggregate(7, 7, 7)), quotaInfo.getProdConsumption());

    assertEquals(
        IResourceAggregate.build(new ResourceAggregate(2, 2, 2)),
        quotaInfo.getNonProdConsumption());
  }

  @Test
  public void testGetQuotaInfoNoTasksNoUpdates() {
    IResourceAggregate quota = IResourceAggregate.build(new ResourceAggregate(4, 4, 4));

    expectQuota(quota);
    expectNoTasks();
    expectNoJobUpdates();

    control.replay();

    QuotaInfo quotaInfo = quotaManager.getQuotaInfo(ROLE);
    assertEquals(quota, quotaInfo.getQuota());
    assertEquals(ResourceAggregates.none(), quotaInfo.getProdConsumption());
    assertEquals(ResourceAggregates.none(), quotaInfo.getNonProdConsumption());
  }

  @Test
  public void testCheckQuotaPasses() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(4, 4, 4)));
    expectTasks(createProdTask("foo", 2, 2, 2));
    expectJobUpdates(createTaskConfig(1, 1, 1, true), createTaskConfig(1, 1, 1, true));

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkQuota(ROLE, prodResource(1, 1, 1));
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaPassesNoTasks() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(4, 4, 4)));
    expectNoTasks();
    expectJobUpdates(createTaskConfig(1, 1, 1, true), createTaskConfig(1, 1, 1, true));

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkQuota(ROLE, prodResource(1, 1, 1));
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaPassesNoUpdates() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(4, 4, 4)));
    expectTasks(createProdTask("foo", 2, 2, 2));
    expectNoJobUpdates();

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkQuota(ROLE, prodResource(1, 1, 1));
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaPassesNoTasksNoUpdates() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(4, 4, 4)));
    expectNoTasks();
    expectNoJobUpdates();

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkQuota(ROLE, prodResource(1, 1, 1));
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaPassesNonProdUnaccounted() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(4, 4, 4)));
    expectTasks(createProdTask("foo", 2, 2, 2), createTask("bar", "id2", 5, 5, 5, false, 0));

    expectNoJobUpdates();

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkQuota(ROLE, prodResource(1, 1, 1));
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaSkippedForNonProdRequest() {
    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkQuota(ROLE, ResourceAggregates.EMPTY);
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaNoQuotaSet() {
    expect(storageUtil.quotaStore.fetchQuota(ROLE))
        .andReturn(Optional.<IResourceAggregate>absent());

    expectNoTasks();
    expectNoJobUpdates();

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkQuota(ROLE, prodResource(1, 1, 1));
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
  }

  @Test
  public void testCheckQuotaExceedsCpu() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(4, 4, 4)));
    expectTasks(createProdTask("foo", 3, 3, 3));
    expectNoJobUpdates();

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkQuota(ROLE, prodResource(2, 1, 1));
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertTrue(checkQuota.getDetails().get().contains("CPU"));
  }

  @Test
  public void testCheckQuotaExceedsRam() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(4, 4, 4)));
    expectTasks(createProdTask("foo", 3, 3, 3));
    expectNoJobUpdates();

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkQuota(ROLE, prodResource(1, 2, 1));
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertTrue(checkQuota.getDetails().get().contains("RAM"));
  }

  @Test
  public void testCheckQuotaExceedsDisk() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(4, 4, 4)));
    expectTasks(createProdTask("foo", 3, 3, 3));
    expectNoJobUpdates();

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkQuota(ROLE, prodResource(1, 1, 2));
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertTrue(checkQuota.getDetails().get().contains("DISK"));
  }

  @Test
  public void testCheckQuotaUpdatingTasksFilteredOut() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(5, 5, 5))).times(2);
    expectTasks(createProdTask("foo", 2, 2, 2), createTask(JOB_NAME, "id2", 3, 3, 3, true, 0))
        .times(2);

    expectJobUpdates(createTaskConfig(1, 1, 1, true), createTaskConfig(2, 2, 2, true), 2);

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkQuota(ROLE, prodResource(1, 1, 1));
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        IResourceAggregate.build(new ResourceAggregate(4, 4, 4)),
        quotaManager.getQuotaInfo(ROLE).getProdConsumption());
  }

  @Test
  public void testCheckQuotaNonProdUpdatesUnaccounted() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(5, 5, 5))).times(2);
    expectTasks(createProdTask("foo", 2, 2, 2), createProdTask("bar", 2, 2, 2)).times(2);

    expectJobUpdates(createTaskConfig(8, 8, 8, false), createTaskConfig(4, 4, 4, false), 2);

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkQuota(ROLE, prodResource(1, 1, 1));
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        IResourceAggregate.build(new ResourceAggregate(4, 4, 4)),
        quotaManager.getQuotaInfo(ROLE).getProdConsumption());
  }

  @Test
  public void testCheckQuotaProdToNonUpdateUnaccounted() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(5, 5, 5))).times(2);
    expectTasks(createProdTask("foo", 2, 2, 2), createProdTask("bar", 1, 1, 1)).times(2);

    expectJobUpdates(createTaskConfig(1, 1, 1, true), createTaskConfig(4, 4, 4, false), 2);

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkQuota(ROLE, prodResource(1, 1, 1));
    assertEquals(SUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        IResourceAggregate.build(new ResourceAggregate(4, 4, 4)),
        quotaManager.getQuotaInfo(ROLE).getProdConsumption());
  }

  @Test
  public void testCheckQuotaNonToProdUpdateExceedsQuota() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(5, 5, 5))).times(2);
    expectTasks(createProdTask("foo", 2, 2, 2), createProdTask("bar", 2, 2, 2)).times(2);

    expectJobUpdates(createTaskConfig(1, 1, 1, false), createTaskConfig(1, 1, 1, true), 2);

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkQuota(ROLE, prodResource(1, 1, 1));
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        IResourceAggregate.build(new ResourceAggregate(5, 5, 5)),
        quotaManager.getQuotaInfo(ROLE).getProdConsumption());
  }

  @Test
  public void testCheckQuotaOldJobUpdateConfigMatters() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(6, 6, 6))).times(2);
    expectTasks(createProdTask("foo", 2, 2, 2), createProdTask("bar", 2, 2, 2)).times(2);
    expectJobUpdates(createTaskConfig(2, 2, 2, true), createTaskConfig(1, 1, 1, true), 2);

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkQuota(ROLE, prodResource(1, 1, 1));
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        IResourceAggregate.build(new ResourceAggregate(6, 6, 6)),
        quotaManager.getQuotaInfo(ROLE).getProdConsumption());
  }

  @Test
  public void testCheckQuotaUpdateAddsInstances() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(6, 6, 6))).times(2);
    expectTasks(createProdTask("foo", 2, 2, 2), createProdTask("bar", 2, 2, 2)).times(2);
    expectJobUpdates(createTaskConfig(1, 1, 1, true), 1, createTaskConfig(1, 1, 1, true), 2, 2);

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkQuota(ROLE, prodResource(1, 1, 1));
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        IResourceAggregate.build(new ResourceAggregate(6, 6, 6)),
        quotaManager.getQuotaInfo(ROLE).getProdConsumption());
  }

  @Test
  public void testCheckQuotaUpdateRemovesInstances() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(6, 6, 6))).times(2);
    expectTasks(createProdTask("foo", 2, 2, 2), createProdTask("bar", 2, 2, 2)).times(2);
    expectJobUpdates(createTaskConfig(1, 1, 1, true), 2, createTaskConfig(1, 1, 1, true), 1, 2);

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkQuota(ROLE, prodResource(1, 1, 1));
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        IResourceAggregate.build(new ResourceAggregate(6, 6, 6)),
        quotaManager.getQuotaInfo(ROLE).getProdConsumption());
  }

  @Test
  public void testCheckQuotaNoDesiredState() {
    expectQuota(IResourceAggregate.build(new ResourceAggregate(6, 6, 6))).times(2);
    expectTasks(createProdTask("foo", 2, 2, 2), createProdTask("bar", 2, 2, 2)).times(2);

    String updateId = "u1";
    ITaskConfig config = createTaskConfig(2, 2, 2, true);
    List<IJobUpdateSummary> summaries = buildJobUpdateSummaries(updateId, JobKeys.from(config));
    IJobUpdate update = buildJobUpdate(summaries.get(0), config, 1, config, 1);
    JobUpdate builder = update.newBuilder();
    builder.getInstructions().unsetDesiredState();

    expect(jobUpdateStore.fetchJobUpdateSummaries(updateQuery(config.getOwner().getRole())))
        .andReturn(summaries).times(2);

    expect(jobUpdateStore.fetchJobUpdate(updateId))
        .andReturn(Optional.of(IJobUpdate.build(builder))).times(2);

    control.replay();

    QuotaCheckResult checkQuota = quotaManager.checkQuota(ROLE, prodResource(1, 1, 1));
    assertEquals(INSUFFICIENT_QUOTA, checkQuota.getResult());
    assertEquals(
        IResourceAggregate.build(new ResourceAggregate(6, 6, 6)),
        quotaManager.getQuotaInfo(ROLE).getProdConsumption());
  }

  @Test
  public void testSaveQuotaPasses() throws Exception {
    storageUtil.quotaStore.saveQuota(ROLE, QUOTA);

    control.replay();
    quotaManager.saveQuota(ROLE, QUOTA);
  }

  @Test(expected = QuotaException.class)
  public void testSaveQuotaFailsMissingSpecs() throws Exception {
    control.replay();
    quotaManager.saveQuota(ROLE, IResourceAggregate.build(new ResourceAggregate()));
  }

  @Test(expected = QuotaException.class)
  public void testSaveQuotaFailsNegativeValues() throws Exception {
    control.replay();
    quotaManager.saveQuota(ROLE, IResourceAggregate.build(new ResourceAggregate(-2.0, 4, 5)));
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

    String updateId = "u1";
    List<IJobUpdateSummary> summaries = buildJobUpdateSummaries(updateId, JobKeys.from(initial));
    IJobUpdate update =
        buildJobUpdate(summaries.get(0), initial, intialInstances, desired, desiredInstances);

    expect(jobUpdateStore.fetchJobUpdateSummaries(updateQuery(initial.getOwner().getRole())))
        .andReturn(summaries)
        .times(times);

    expect(jobUpdateStore.fetchJobUpdate(updateId)).andReturn(Optional.of(update)).times(times);

  }

  private List<IJobUpdateSummary> buildJobUpdateSummaries(String updateId, IJobKey jobKey) {
    return ImmutableList.of(IJobUpdateSummary.build(new JobUpdateSummary()
        .setJobKey(jobKey.newBuilder())
        .setUpdateId(updateId)));
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

  private void expectNoJobUpdates() {
    expect(jobUpdateStore.fetchJobUpdateSummaries(
        QuotaManagerImpl.updateQuery(ROLE))).andReturn(ImmutableList.<IJobUpdateSummary>of());
  }

  private IExpectationSetters<?> expectNoTasks() {
    return expectTasks();
  }

  private IExpectationSetters<Optional<IResourceAggregate>> expectQuota(IResourceAggregate quota) {
    return expect(storageUtil.quotaStore.fetchQuota(ROLE))
        .andReturn(Optional.of(quota));
  }

  private IResourceAggregate prodResource(double cpu, long ram, long disk) {
    return IResourceAggregate.build(new ResourceAggregate(cpu, ram, disk));
  }

  private ITaskConfig createTaskConfig(int cpus, int ramMb, int diskMb, boolean production) {
    return createTask(JOB_NAME, "newId", cpus, ramMb, diskMb, production, 0)
        .getAssignedTask()
        .getTask();
  }

  private IScheduledTask createProdTask(String jobName, int cpus, int ramMb, int diskMb) {
    return createTask(jobName, jobName + "id1", cpus, ramMb, diskMb, true, 0);
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
                    .setOwner(new Identity(ROLE, ROLE))
                    .setEnvironment(ENV)
                    .setJobName(jobName)
                    .setNumCpus(cpus)
                    .setRamMb(ramMb)
                    .setDiskMb(diskMb)
                    .setProduction(production))));
  }
}

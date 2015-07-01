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
package org.apache.aurora.scheduler.thrift;

import java.util.Date;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ConfigGroup;
import org.apache.aurora.gen.ConfigSummary;
import org.apache.aurora.gen.ConfigSummaryResult;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobStats;
import org.apache.aurora.gen.JobSummary;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.PendingReason;
import org.apache.aurora.gen.PopulateJobResult;
import org.apache.aurora.gen.ReadOnlyScheduler;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.Result;
import org.apache.aurora.gen.RoleSummary;
import org.apache.aurora.gen.RoleSummaryResult;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Query.Builder;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.SanitizedConfiguration;
import org.apache.aurora.scheduler.cron.CronPredictor;
import org.apache.aurora.scheduler.cron.CrontabEntry;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.metadata.NearestFit;
import org.apache.aurora.scheduler.quota.QuotaInfo;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.state.LockManager;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ResponseCode.INVALID_REQUEST;
import static org.apache.aurora.scheduler.thrift.Fixtures.CONSUMED;
import static org.apache.aurora.scheduler.thrift.Fixtures.CRON_SCHEDULE;
import static org.apache.aurora.scheduler.thrift.Fixtures.JOB_KEY;
import static org.apache.aurora.scheduler.thrift.Fixtures.LOCK;
import static org.apache.aurora.scheduler.thrift.Fixtures.QUOTA;
import static org.apache.aurora.scheduler.thrift.Fixtures.ROLE;
import static org.apache.aurora.scheduler.thrift.Fixtures.ROLE_IDENTITY;
import static org.apache.aurora.scheduler.thrift.Fixtures.UPDATE_KEY;
import static org.apache.aurora.scheduler.thrift.Fixtures.USER;
import static org.apache.aurora.scheduler.thrift.Fixtures.assertOkResponse;
import static org.apache.aurora.scheduler.thrift.Fixtures.assertResponse;
import static org.apache.aurora.scheduler.thrift.Fixtures.defaultTask;
import static org.apache.aurora.scheduler.thrift.Fixtures.jobSummaryResponse;
import static org.apache.aurora.scheduler.thrift.Fixtures.makeDefaultScheduledTasks;
import static org.apache.aurora.scheduler.thrift.Fixtures.makeJob;
import static org.apache.aurora.scheduler.thrift.Fixtures.nonProductionTask;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReadOnlySchedulerImplTest extends EasyMockTest {
  private StorageTestUtil storageUtil;
  private NearestFit nearestFit;
  private CronPredictor cronPredictor;
  private QuotaManager quotaManager;
  private LockManager lockManager;

  private ReadOnlyScheduler.Iface thrift;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    nearestFit = createMock(NearestFit.class);
    cronPredictor = createMock(CronPredictor.class);
    quotaManager = createMock(QuotaManager.class);
    lockManager = createMock(LockManager.class);

    thrift = new ReadOnlySchedulerImpl(
        storageUtil.storage,
        nearestFit,
        cronPredictor,
        quotaManager,
        lockManager);
  }

  @Test
  public void testGetJobSummary() throws Exception {
    long nextCronRunMs = 100;
    TaskConfig ownedCronJobTask = nonProductionTask()
        .setJob(JOB_KEY.newBuilder())
        .setJobName(JobKeys.TO_JOB_NAME.apply(JOB_KEY))
        .setOwner(ROLE_IDENTITY)
        .setEnvironment(JobKeys.TO_ENVIRONMENT.apply(JOB_KEY));
    JobConfiguration ownedCronJob = makeJob()
        .setCronSchedule(CRON_SCHEDULE)
        .setTaskConfig(ownedCronJobTask);
    IScheduledTask ownedCronJobScheduledTask = IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(ownedCronJobTask))
        .setStatus(ScheduleStatus.ASSIGNED));
    Identity otherOwner = new Identity("other", "other");
    JobConfiguration unownedCronJob = makeJob()
        .setOwner(otherOwner)
        .setCronSchedule(CRON_SCHEDULE)
        .setKey(JOB_KEY.newBuilder().setRole("other"))
        .setTaskConfig(ownedCronJobTask.deepCopy().setOwner(otherOwner));
    TaskConfig ownedImmediateTaskInfo = defaultTask(false)
        .setJob(JOB_KEY.newBuilder().setName("immediate"))
        .setJobName("immediate")
        .setOwner(ROLE_IDENTITY);
    Set<JobConfiguration> ownedCronJobOnly = ImmutableSet.of(ownedCronJob);
    Set<JobSummary> ownedCronJobSummaryOnly = ImmutableSet.of(
        new JobSummary()
            .setJob(ownedCronJob)
            .setStats(new JobStats())
            .setNextCronRunMs(nextCronRunMs));
    Set<JobSummary> ownedCronJobSummaryWithRunningTask = ImmutableSet.of(
        new JobSummary()
            .setJob(ownedCronJob)
            .setStats(new JobStats().setActiveTaskCount(1))
            .setNextCronRunMs(nextCronRunMs));
    Set<JobConfiguration> unownedCronJobOnly = ImmutableSet.of(unownedCronJob);
    Set<JobConfiguration> bothCronJobs = ImmutableSet.of(ownedCronJob, unownedCronJob);

    IScheduledTask ownedImmediateTask = IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(ownedImmediateTaskInfo))
        .setStatus(ScheduleStatus.ASSIGNED));
    JobConfiguration ownedImmediateJob = new JobConfiguration()
        .setKey(JOB_KEY.newBuilder().setName("immediate"))
        .setOwner(ROLE_IDENTITY)
        .setInstanceCount(1)
        .setTaskConfig(ownedImmediateTaskInfo);
    Builder query = Query.roleScoped(ROLE);

    Set<JobSummary> ownedImmedieteJobSummaryOnly = ImmutableSet.of(
        new JobSummary().setJob(ownedImmediateJob).setStats(new JobStats().setActiveTaskCount(1)));

    expect(cronPredictor.predictNextRun(CrontabEntry.parse(CRON_SCHEDULE)))
        .andReturn(new Date(nextCronRunMs))
        .anyTimes();

    storageUtil.expectTaskFetch(query);
    expect(storageUtil.jobStore.fetchJobs())
        .andReturn(IJobConfiguration.setFromBuilders(ownedCronJobOnly));

    storageUtil.expectTaskFetch(query);
    expect(storageUtil.jobStore.fetchJobs())
        .andReturn(IJobConfiguration.setFromBuilders(bothCronJobs));

    storageUtil.expectTaskFetch(query, ownedImmediateTask);
    expect(storageUtil.jobStore.fetchJobs())
        .andReturn(IJobConfiguration.setFromBuilders(unownedCronJobOnly));

    storageUtil.expectTaskFetch(query);
    expect(storageUtil.jobStore.fetchJobs()).andReturn(ImmutableSet.of());

    // Handle the case where a cron job has a running task (same JobKey present in both stores).
    storageUtil.expectTaskFetch(query, ownedCronJobScheduledTask);
    expect(storageUtil.jobStore.fetchJobs())
        .andReturn(IJobConfiguration.setFromBuilders(ImmutableSet.of(ownedCronJob)));

    control.replay();

    assertEquals(jobSummaryResponse(ownedCronJobSummaryOnly), thrift.getJobSummary(ROLE));

    assertEquals(jobSummaryResponse(ownedCronJobSummaryOnly), thrift.getJobSummary(ROLE));

    Response jobSummaryResponse = thrift.getJobSummary(ROLE);
    assertEquals(jobSummaryResponse(ownedImmedieteJobSummaryOnly), jobSummaryResponse);
    assertEquals(ownedImmediateTaskInfo,
        Iterables.getOnlyElement(
            jobSummaryResponse.getResult().getJobSummaryResult().getSummaries())
            .getJob()
            .getTaskConfig());

    assertEquals(jobSummaryResponse(ImmutableSet.of()), thrift.getJobSummary(ROLE));

    assertEquals(jobSummaryResponse(ownedCronJobSummaryWithRunningTask),
        thrift.getJobSummary(ROLE));
  }

  @Test
  public void testGetPendingReason() throws Exception {
    Builder query = Query.unscoped().byJob(JOB_KEY);
    Builder filterQuery = Query.unscoped().byJob(JOB_KEY).byStatus(ScheduleStatus.PENDING);
    String taskId1 = "task_id_test1";
    String taskId2 = "task_id_test2";
    ImmutableSet<Veto> result = ImmutableSet.of(
        Veto.constraintMismatch("first"),
        Veto.constraintMismatch("second"));

    ITaskConfig taskConfig = ITaskConfig.build(defaultTask(true));
    IScheduledTask pendingTask1 = IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setTaskId(taskId1)
            .setTask(taskConfig.newBuilder()))
        .setStatus(ScheduleStatus.PENDING));

    IScheduledTask pendingTask2 = IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setTaskId(taskId2)
            .setTask(taskConfig.newBuilder()))
        .setStatus(ScheduleStatus.PENDING));

    storageUtil.expectTaskFetch(filterQuery, pendingTask1, pendingTask2);
    expect(nearestFit.getNearestFit(TaskGroupKey.from(taskConfig))).andReturn(result).times(2);

    control.replay();

    String reason = "Constraint not satisfied: first,Constraint not satisfied: second";
    Set<PendingReason> expected = ImmutableSet.of(
        new PendingReason().setTaskId(taskId1).setReason(reason),
        new PendingReason().setTaskId(taskId2).setReason(reason));

    Response response = assertOkResponse(thrift.getPendingReason(query.get()));
    assertEquals(expected, response.getResult().getGetPendingReasonResult().getReasons());
  }

  @Test
  public void testPopulateJobConfig() throws Exception {
    IJobConfiguration job = IJobConfiguration.build(makeJob());
    SanitizedConfiguration sanitized = SanitizedConfiguration.fromUnsanitized(job);
    control.replay();

    Response response = assertOkResponse(thrift.populateJobConfig(job.newBuilder()));
    assertEquals(
        Result.populateJobResult(new PopulateJobResult(
            sanitized.getJobConfig().getTaskConfig().newBuilder())),
        response.getResult());
  }

  @Test
  public void testPopulateJobConfigFails() throws Exception {
    IJobConfiguration job = IJobConfiguration.build(makeJob(null));
    control.replay();

    assertResponse(INVALID_REQUEST, thrift.populateJobConfig(job.newBuilder()));
  }

  @Test
  public void testGetLocks() throws Exception {
    expect(lockManager.getLocks()).andReturn(ImmutableSet.of(LOCK));

    control.replay();

    Response response = thrift.getLocks();
    assertEquals(
        LOCK.newBuilder(),
        Iterables.getOnlyElement(response.getResult().getGetLocksResult().getLocks()));
  }

  @Test
  public void testGetQuota() throws Exception {
    QuotaInfo infoMock = createMock(QuotaInfo.class);
    expect(quotaManager.getQuotaInfo(ROLE, storageUtil.storeProvider)).andReturn(infoMock);
    expect(infoMock.getQuota()).andReturn(QUOTA);
    expect(infoMock.getProdConsumption()).andReturn(CONSUMED);
    IResourceAggregate nonProdConsumed = IResourceAggregate.build(new ResourceAggregate(1, 0, 0));
    expect(infoMock.getNonProdConsumption()).andReturn(nonProdConsumed);
    control.replay();

    Response response = assertOkResponse(thrift.getQuota(ROLE));
    assertEquals(QUOTA.newBuilder(), response.getResult().getGetQuotaResult().getQuota());
    assertEquals(
        CONSUMED.newBuilder(),
        response.getResult().getGetQuotaResult().getProdConsumption());
    assertEquals(
        nonProdConsumed.newBuilder(),
        response.getResult().getGetQuotaResult().getNonProdConsumption());
  }

  @Test
  public void testGetTasksWithoutConfigs() throws Exception {
    Builder query = Query.unscoped();
    storageUtil.expectTaskFetch(query, ImmutableSet.copyOf(makeDefaultScheduledTasks(10)));

    control.replay();

    ImmutableList<ScheduledTask> expected = IScheduledTask.toBuildersList(makeDefaultScheduledTasks(
        10,
        defaultTask(true).setExecutorConfig(null)));

    Response response = assertOkResponse(thrift.getTasksWithoutConfigs(new TaskQuery()));
    assertEquals(expected, response.getResult().getScheduleStatusResult().getTasks());
  }

  @Test
  public void testGetPendingReasonFailsSlavesSet() throws Exception {
    Builder query = Query.unscoped().bySlave("host1");

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.getPendingReason(query.get()));
  }

  @Test
  public void testGetAllJobs() throws Exception {
    JobConfiguration cronJobOne = makeJob()
        .setCronSchedule("1 * * * *")
        .setKey(JOB_KEY.newBuilder())
        .setTaskConfig(nonProductionTask());
    JobKey jobKey2 = JOB_KEY.newBuilder().setRole("other_role");
    JobConfiguration cronJobTwo = makeJob()
        .setCronSchedule("2 * * * *")
        .setKey(jobKey2)
        .setTaskConfig(nonProductionTask());
    TaskConfig immediateTaskConfig = defaultTask(false)
        .setJob(JOB_KEY.newBuilder().setName("immediate"))
        .setJobName("immediate")
        .setOwner(ROLE_IDENTITY);
    IScheduledTask immediateTask = IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(immediateTaskConfig))
        .setStatus(ScheduleStatus.ASSIGNED));
    JobConfiguration immediateJob = new JobConfiguration()
        .setKey(JOB_KEY.newBuilder().setName("immediate"))
        .setOwner(ROLE_IDENTITY)
        .setInstanceCount(1)
        .setTaskConfig(immediateTaskConfig);

    Set<JobConfiguration> crons = ImmutableSet.of(cronJobOne, cronJobTwo);
    expect(storageUtil.jobStore.fetchJobs()).andReturn(IJobConfiguration.setFromBuilders(crons));
    storageUtil.expectTaskFetch(Query.unscoped().active(), immediateTask);

    control.replay();

    Set<JobConfiguration> allJobs =
        ImmutableSet.<JobConfiguration>builder().addAll(crons).add(immediateJob).build();
    assertEquals(allJobs, thrift.getJobs(null).getResult().getGetJobsResult().getConfigs());
  }

  @Test
  public void testGetJobs() throws Exception {
    TaskConfig ownedCronJobTask = nonProductionTask()
        .setJobName(JobKeys.TO_JOB_NAME.apply(JOB_KEY))
        .setOwner(ROLE_IDENTITY)
        .setEnvironment(JobKeys.TO_ENVIRONMENT.apply(JOB_KEY));
    JobConfiguration ownedCronJob = makeJob()
        .setCronSchedule(CRON_SCHEDULE)
        .setTaskConfig(ownedCronJobTask);
    IScheduledTask ownedCronJobScheduledTask = IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(ownedCronJobTask))
        .setStatus(ScheduleStatus.ASSIGNED));
    Identity otherOwner = new Identity("other", "other");
    JobConfiguration unownedCronJob = makeJob()
        .setOwner(otherOwner)
        .setCronSchedule(CRON_SCHEDULE)
        .setKey(JOB_KEY.newBuilder().setRole("other"))
        .setTaskConfig(ownedCronJobTask.deepCopy().setOwner(otherOwner));
    TaskConfig ownedImmediateTaskInfo = defaultTask(false)
        .setJob(JOB_KEY.newBuilder().setName("immediate"))
        .setJobName("immediate")
        .setOwner(ROLE_IDENTITY);
    Set<JobConfiguration> ownedCronJobOnly = ImmutableSet.of(ownedCronJob);
    Set<JobConfiguration> unownedCronJobOnly = ImmutableSet.of(unownedCronJob);
    Set<JobConfiguration> bothCronJobs = ImmutableSet.of(ownedCronJob, unownedCronJob);
    IScheduledTask ownedImmediateTask = IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(ownedImmediateTaskInfo))
        .setStatus(ScheduleStatus.ASSIGNED));
    JobConfiguration ownedImmediateJob = new JobConfiguration()
        .setKey(JOB_KEY.newBuilder().setName("immediate"))
        .setOwner(ROLE_IDENTITY)
        .setInstanceCount(1)
        .setTaskConfig(ownedImmediateTaskInfo);
    Query.Builder query = Query.roleScoped(ROLE).active();

    storageUtil.expectTaskFetch(query);
    expect(storageUtil.jobStore.fetchJobs())
        .andReturn(IJobConfiguration.setFromBuilders(ownedCronJobOnly));

    storageUtil.expectTaskFetch(query);
    expect(storageUtil.jobStore.fetchJobs())
        .andReturn(IJobConfiguration.setFromBuilders(bothCronJobs));

    storageUtil.expectTaskFetch(query, ownedImmediateTask);
    expect(storageUtil.jobStore.fetchJobs())
        .andReturn(IJobConfiguration.setFromBuilders(unownedCronJobOnly));

    expect(storageUtil.jobStore.fetchJobs()).andReturn(ImmutableSet.of());
    storageUtil.expectTaskFetch(query);

    // Handle the case where a cron job has a running task (same JobKey present in both stores).
    storageUtil.expectTaskFetch(query, ownedCronJobScheduledTask);
    expect(storageUtil.jobStore.fetchJobs())
        .andReturn(IJobConfiguration.setFromBuilders(ImmutableSet.of(ownedCronJob)));

    control.replay();

    assertEquals(ownedCronJob, Iterables.getOnlyElement(thrift.getJobs(ROLE)
        .getResult().getGetJobsResult().getConfigs()));

    assertEquals(ownedCronJob, Iterables.getOnlyElement(thrift.getJobs(ROLE)
        .getResult().getGetJobsResult().getConfigs()));

    Set<JobConfiguration> queryResult3 =
        thrift.getJobs(ROLE).getResult().getGetJobsResult().getConfigs();
    assertEquals(ownedImmediateJob, Iterables.getOnlyElement(queryResult3));
    assertEquals(ownedImmediateTaskInfo, Iterables.getOnlyElement(queryResult3).getTaskConfig());

    assertTrue(thrift.getJobs(ROLE)
        .getResult().getGetJobsResult().getConfigs().isEmpty());

    assertEquals(ownedCronJob, Iterables.getOnlyElement(thrift.getJobs(ROLE)
        .getResult().getGetJobsResult().getConfigs()));
  }

  @Test
  public void testGetTasksStatusPagination() throws Exception {
    Iterable<IScheduledTask> tasks = makeDefaultScheduledTasks(10);

    TaskQuery page1Query = setupPaginatedQuery(tasks, 0, 4);
    TaskQuery page2Query = setupPaginatedQuery(tasks, 4, 4);
    TaskQuery page3Query = setupPaginatedQuery(tasks, 8, 4);

    control.replay();

    Response page1Response = assertOkResponse(thrift.getTasksStatus(page1Query));
    Response page2Response = assertOkResponse(thrift.getTasksStatus(page2Query));
    Response page3Response = assertOkResponse(thrift.getTasksStatus(page3Query));

    Iterable<Integer> page1Ids = Lists.newArrayList(Iterables.transform(
        page1Response.getResult().getScheduleStatusResult().getTasks(), TO_INSTANCE_ID));
    Iterable<Integer> page2Ids = Lists.newArrayList(Iterables.transform(
        page2Response.getResult().getScheduleStatusResult().getTasks(), TO_INSTANCE_ID));
    Iterable<Integer> page3Ids = Lists.newArrayList(Iterables.transform(
        page3Response.getResult().getScheduleStatusResult().getTasks(), TO_INSTANCE_ID));

    assertEquals(Lists.newArrayList(0, 1, 2, 3), page1Ids);
    assertEquals(Lists.newArrayList(4, 5, 6, 7), page2Ids);
    assertEquals(Lists.newArrayList(8, 9), page3Ids);
  }

  private TaskQuery setupPaginatedQuery(Iterable<IScheduledTask> tasks, int offset, int limit) {
    TaskQuery query = new TaskQuery().setOffset(offset).setLimit(limit);
    Builder builder = Query.arbitrary(query);
    storageUtil.expectTaskFetch(builder, ImmutableSet.copyOf(tasks));
    return query;
  }

  private static final Function<ScheduledTask, Integer> TO_INSTANCE_ID =
      new Function<ScheduledTask, Integer>() {
        @Nullable
        @Override
        public Integer apply(@Nullable ScheduledTask input) {
          return input.getAssignedTask().getInstanceId();
        }
      };

  @Test
  public void testGetConfigSummary() throws Exception {
    IJobKey key = JobKeys.from("test", "test", "test");

    TaskConfig firstGroupTask = defaultTask(true);
    TaskConfig secondGroupTask = defaultTask(true).setNumCpus(2);

    IScheduledTask first1 = IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(firstGroupTask).setInstanceId(0)));

    IScheduledTask first2 = IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(firstGroupTask).setInstanceId(1)));

    IScheduledTask second = IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(secondGroupTask).setInstanceId(2)));

    storageUtil.expectTaskFetch(Query.jobScoped(key).active(), first1, first2, second);

    ConfigGroup group1 = new ConfigGroup()
        .setConfig(firstGroupTask)
        .setInstanceIds(Sets.newHashSet(0, 1));
    ConfigGroup group2 = new ConfigGroup()
        .setConfig(secondGroupTask)
        .setInstanceIds(Sets.newHashSet(2));

    ConfigSummary summary = new ConfigSummary()
        .setKey(key.newBuilder())
        .setGroups(Sets.newHashSet(group1, group2));

    ConfigSummaryResult expected = new ConfigSummaryResult().setSummary(summary);

    control.replay();

    Response response = assertOkResponse(thrift.getConfigSummary(key.newBuilder()));
    assertEquals(expected, response.getResult().getConfigSummaryResult());
  }

  @Test
  public void testGetTasksStatus() throws Exception {
    Builder query = Query.unscoped();
    Iterable<IScheduledTask> tasks = makeDefaultScheduledTasks(10);
    storageUtil.expectTaskFetch(query, ImmutableSet.copyOf(tasks));

    control.replay();

    ImmutableList<ScheduledTask> expected = IScheduledTask.toBuildersList(tasks);
    Response response = assertOkResponse(thrift.getTasksStatus(new TaskQuery()));
    assertEquals(expected, response.getResult().getScheduleStatusResult().getTasks());
  }

  @Test
  public void testGetPendingReasonFailsStatusSet() throws Exception {
    Builder query = Query.unscoped().byStatus(ScheduleStatus.ASSIGNED);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.getPendingReason(query.get()));
  }

  @Test
  public void testGetJobUpdateSummaries() throws Exception {
    JobUpdateQuery query = new JobUpdateQuery().setRole(ROLE);
    List<JobUpdateSummary> summaries = createJobUpdateSummaries(5);
    expect(storageUtil.jobUpdateStore.fetchJobUpdateSummaries(IJobUpdateQuery.build(query)))
        .andReturn(IJobUpdateSummary.listFromBuilders(summaries));

    control.replay();

    Response response = assertOkResponse(thrift.getJobUpdateSummaries(query));
    assertEquals(
        summaries,
        response.getResult().getGetJobUpdateSummariesResult().getUpdateSummaries());
  }

  @Test
  public void testGetJobUpdateDetails() throws Exception {
    JobUpdateDetails details = createJobUpdateDetails();
    expect(storageUtil.jobUpdateStore.fetchJobUpdateDetails(UPDATE_KEY))
        .andReturn(Optional.of(IJobUpdateDetails.build(details)));

    control.replay();

    Response response = assertOkResponse(thrift.getJobUpdateDetails(UPDATE_KEY.newBuilder()));
    assertEquals(
        details,
        response.getResult().getGetJobUpdateDetailsResult().getDetails());
  }

  private static List<JobUpdateSummary> createJobUpdateSummaries(int count) {
    ImmutableList.Builder<JobUpdateSummary> builder = ImmutableList.builder();
    for (int i = 0; i < count; i++) {
      builder.add(new JobUpdateSummary()
          .setKey(new JobUpdateKey(JOB_KEY.newBuilder(), "id" + 1))
          .setUser(USER));
    }
    return builder.build();
  }

  private static JobUpdateDetails createJobUpdateDetails() {
    return new JobUpdateDetails()
        .setUpdate(new JobUpdate().setSummary(createJobUpdateSummaries(1).get(0)));
  }

  @Test
  public void testGetRoleSummary() throws Exception {
    final String BAZ_ROLE = "baz_role";
    final Identity BAZ_ROLE_IDENTITY = new Identity(BAZ_ROLE, USER);

    JobConfiguration cronJobOne = makeJob()
        .setCronSchedule("1 * * * *")
        .setKey(JOB_KEY.newBuilder())
        .setTaskConfig(nonProductionTask());
    JobConfiguration cronJobTwo = makeJob()
        .setCronSchedule("2 * * * *")
        .setKey(JOB_KEY.newBuilder().setName("cronJob2"))
        .setTaskConfig(nonProductionTask());

    JobConfiguration cronJobThree = makeJob()
        .setCronSchedule("3 * * * *")
        .setKey(JOB_KEY.newBuilder().setRole(BAZ_ROLE))
        .setTaskConfig(nonProductionTask())
        .setOwner(BAZ_ROLE_IDENTITY);

    Set<JobConfiguration> crons = ImmutableSet.of(cronJobOne, cronJobTwo, cronJobThree);

    TaskConfig immediateTaskConfig = defaultTask(false)
        .setJob(JOB_KEY.newBuilder().setName("immediate"))
        .setJobName("immediate")
        .setOwner(ROLE_IDENTITY);
    IScheduledTask task1 = IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(immediateTaskConfig)));
    IScheduledTask task2 = IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(immediateTaskConfig.setNumCpus(2))));

    TaskConfig immediateTaskConfigTwo = defaultTask(false)
        .setJob(JOB_KEY.newBuilder().setRole(BAZ_ROLE_IDENTITY.getRole()).setName("immediateTwo"))
        .setJobName("immediateTwo")
        .setOwner(BAZ_ROLE_IDENTITY);
    IScheduledTask task3 = IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(immediateTaskConfigTwo)));

    TaskConfig immediateTaskConfigThree = defaultTask(false)
        .setJob(JOB_KEY.newBuilder().setRole(BAZ_ROLE_IDENTITY.getRole()).setName("immediateThree"))
        .setJobName("immediateThree")
        .setOwner(BAZ_ROLE_IDENTITY);
    IScheduledTask task4 = IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(immediateTaskConfigThree)));

    expect(storageUtil.taskStore.getJobKeys()).andReturn(
        FluentIterable.from(ImmutableSet.of(task1, task2, task3, task4))
            .transform(Tasks.SCHEDULED_TO_JOB_KEY)
            .toSet());
    expect(storageUtil.jobStore.fetchJobs()).andReturn(IJobConfiguration.setFromBuilders(crons));

    RoleSummaryResult expectedResult = new RoleSummaryResult();
    expectedResult.addToSummaries(
        new RoleSummary().setRole(ROLE).setCronJobCount(2).setJobCount(1));
    expectedResult.addToSummaries(
        new RoleSummary().setRole(BAZ_ROLE).setCronJobCount(1).setJobCount(2));

    control.replay();

    Response response = assertOkResponse(thrift.getRoleSummary());
    assertEquals(expectedResult, response.getResult().getRoleSummaryResult());
  }

  @Test
  public void testEmptyConfigSummary() throws Exception {
    IJobKey key = JobKeys.from("test", "test", "test");

    storageUtil.expectTaskFetch(Query.jobScoped(key).active(), ImmutableSet.of());

    ConfigSummary summary = new ConfigSummary()
        .setKey(key.newBuilder())
        .setGroups(Sets.newHashSet());

    ConfigSummaryResult expected = new ConfigSummaryResult().setSummary(summary);

    control.replay();

    Response response = assertOkResponse(thrift.getConfigSummary(key.newBuilder()));
    assertEquals(expected, response.getResult().getConfigSummaryResult());
  }

  @Test
  public void testGetJobUpdateDetailsInvalidId() throws Exception {
    expect(storageUtil.jobUpdateStore.fetchJobUpdateDetails(UPDATE_KEY))
        .andReturn(Optional.absent());

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.getJobUpdateDetails(UPDATE_KEY.newBuilder()));
  }
}

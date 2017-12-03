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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.ExplicitReconciliationSettings;
import org.apache.aurora.gen.HostStatus;
import org.apache.aurora.gen.Hosts;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdatePulseStatus;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateRequest;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.LimitConstraint;
import org.apache.aurora.gen.ListBackupsResult;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.MesosContainer;
import org.apache.aurora.gen.Metadata;
import org.apache.aurora.gen.PulseJobUpdateResult;
import org.apache.aurora.gen.QueryRecoveryResult;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.ReadOnlyScheduler;
import org.apache.aurora.gen.Resource;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ResponseCode;
import org.apache.aurora.gen.ResponseDetail;
import org.apache.aurora.gen.Result;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.StartJobUpdateResult;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.gen.ValueConstraint;
import org.apache.aurora.gen.apiConstants;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import org.apache.aurora.scheduler.configuration.SanitizedConfiguration;
import org.apache.aurora.scheduler.cron.CronException;
import org.apache.aurora.scheduler.cron.CronJobManager;
import org.apache.aurora.scheduler.cron.SanitizedCronJob;
import org.apache.aurora.scheduler.quota.QuotaCheckResult;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.reconciliation.TaskReconciler;
import org.apache.aurora.scheduler.state.MaintenanceController;
import org.apache.aurora.scheduler.state.StateChangeResult;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.state.UUIDGenerator;
import org.apache.aurora.scheduler.storage.DistributedSnapshotStore;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.backup.Recovery;
import org.apache.aurora.scheduler.storage.backup.StorageBackup;
import org.apache.aurora.scheduler.storage.entities.IHostStatus;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IMetadata;
import org.apache.aurora.scheduler.storage.entities.IRange;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.aurora.scheduler.updater.JobUpdateController;
import org.apache.aurora.scheduler.updater.JobUpdateController.AuditData;
import org.apache.aurora.scheduler.updater.JobUpdateController.JobUpdatingException;
import org.apache.aurora.scheduler.updater.UpdateInProgressException;
import org.apache.aurora.scheduler.updater.UpdateStateException;
import org.apache.thrift.TException;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.MaintenanceMode.DRAINING;
import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.apache.aurora.gen.MaintenanceMode.SCHEDULED;
import static org.apache.aurora.gen.Resource.diskMb;
import static org.apache.aurora.gen.Resource.numCpus;
import static org.apache.aurora.gen.Resource.ramMb;
import static org.apache.aurora.gen.ResponseCode.INVALID_REQUEST;
import static org.apache.aurora.gen.ResponseCode.JOB_UPDATING_ERROR;
import static org.apache.aurora.gen.ResponseCode.OK;
import static org.apache.aurora.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;
import static org.apache.aurora.scheduler.storage.backup.Recovery.RecoveryException;
import static org.apache.aurora.scheduler.thrift.Fixtures.CRON_JOB;
import static org.apache.aurora.scheduler.thrift.Fixtures.ENOUGH_QUOTA;
import static org.apache.aurora.scheduler.thrift.Fixtures.IDENTITY;
import static org.apache.aurora.scheduler.thrift.Fixtures.INSTANCE_KEY;
import static org.apache.aurora.scheduler.thrift.Fixtures.INVALID_TASK_CONFIG;
import static org.apache.aurora.scheduler.thrift.Fixtures.JOB_KEY;
import static org.apache.aurora.scheduler.thrift.Fixtures.JOB_NAME;
import static org.apache.aurora.scheduler.thrift.Fixtures.NOT_ENOUGH_QUOTA;
import static org.apache.aurora.scheduler.thrift.Fixtures.ROLE;
import static org.apache.aurora.scheduler.thrift.Fixtures.TASK_ID;
import static org.apache.aurora.scheduler.thrift.Fixtures.UPDATE_KEY;
import static org.apache.aurora.scheduler.thrift.Fixtures.USER;
import static org.apache.aurora.scheduler.thrift.Fixtures.UU_ID;
import static org.apache.aurora.scheduler.thrift.Fixtures.assertOkResponse;
import static org.apache.aurora.scheduler.thrift.Fixtures.assertResponse;
import static org.apache.aurora.scheduler.thrift.Fixtures.defaultTask;
import static org.apache.aurora.scheduler.thrift.Fixtures.makeJob;
import static org.apache.aurora.scheduler.thrift.Fixtures.makeProdJob;
import static org.apache.aurora.scheduler.thrift.Fixtures.nonProductionTask;
import static org.apache.aurora.scheduler.thrift.Fixtures.okResponse;
import static org.apache.aurora.scheduler.thrift.Fixtures.productionTask;
import static org.apache.aurora.scheduler.thrift.Fixtures.response;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.ADD_INSTANCES;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.CREATE_JOB;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.CREATE_OR_UPDATE_CRON;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.DRAIN_HOSTS;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.END_MAINTENANCE;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.KILL_TASKS;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.MAINTENANCE_STATUS;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.NOOP_JOB_UPDATE_MESSAGE;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.NO_CRON;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.RESTART_SHARDS;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.START_JOB_UPDATE;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.START_MAINTENANCE;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.jobAlreadyExistsMessage;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.noCronScheduleMessage;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.notScheduledCronMessage;
import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SchedulerThriftInterfaceTest extends EasyMockTest {

  private static final String AUDIT_MESSAGE = "message";
  private static final AuditData AUDIT = new AuditData(USER, Optional.of(AUDIT_MESSAGE));
  private static final Thresholds THRESHOLDS = new Thresholds(1000, 2000);
  private static final String EXECUTOR_NAME = apiConstants.AURORA_EXECUTOR_NAME;
  private static final ImmutableSet<Metadata> METADATA =
      ImmutableSet.of(new Metadata("k1", "v1"), new Metadata("k2", "v2"));

  private StorageTestUtil storageUtil;
  private DistributedSnapshotStore snapshotStore;
  private StorageBackup backup;
  private Recovery recovery;
  private MaintenanceController maintenance;
  private AuroraAdmin.Iface thrift;
  private CronJobManager cronJobManager;
  private QuotaManager quotaManager;
  private StateManager stateManager;
  private UUIDGenerator uuidGenerator;
  private JobUpdateController jobUpdateController;
  private ReadOnlyScheduler.Iface readOnlyScheduler;
  private AuditMessages auditMessages;
  private TaskReconciler taskReconciler;
  private FakeStatsProvider statsProvider;

  @Before
  public void setUp() throws Exception {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    snapshotStore = createMock(DistributedSnapshotStore.class);
    backup = createMock(StorageBackup.class);
    recovery = createMock(Recovery.class);
    maintenance = createMock(MaintenanceController.class);
    cronJobManager = createMock(CronJobManager.class);
    quotaManager = createMock(QuotaManager.class);
    stateManager = createMock(StateManager.class);
    uuidGenerator = createMock(UUIDGenerator.class);
    jobUpdateController = createMock(JobUpdateController.class);
    readOnlyScheduler = createMock(ReadOnlyScheduler.Iface.class);
    auditMessages = createMock(AuditMessages.class);
    taskReconciler = createMock(TaskReconciler.class);
    statsProvider = new FakeStatsProvider();

    thrift = getResponseProxy(
        new SchedulerThriftInterface(
            TaskTestUtil.CONFIGURATION_MANAGER,
            THRESHOLDS,
            storageUtil.storage,
            snapshotStore,
            backup,
            recovery,
            cronJobManager,
            maintenance,
            quotaManager,
            stateManager,
            uuidGenerator,
            jobUpdateController,
            readOnlyScheduler,
            auditMessages,
            taskReconciler,
            statsProvider));
  }

  private static AuroraAdmin.Iface getResponseProxy(AuroraAdmin.Iface realThrift) {
    // Capture all API method calls to validate response objects.
    Class<AuroraAdmin.Iface> thriftClass = AuroraAdmin.Iface.class;
    return (AuroraAdmin.Iface) Proxy.newProxyInstance(
        thriftClass.getClassLoader(),
        new Class<?>[] {thriftClass},
        (o, method, args) -> {
          Response response;
          try {
            response = (Response) method.invoke(realThrift, args);
          } catch (InvocationTargetException e) {
            Throwables.propagateIfPossible(e.getTargetException(), TException.class);
            throw e;
          }
          assertTrue(response.isSetResponseCode());
          assertNotNull(response.getDetails());
          return response;
        });
  }

  private static SanitizedConfiguration fromUnsanitized(IJobConfiguration job)
      throws TaskDescriptionException {

    return SanitizedConfiguration.fromUnsanitized(TaskTestUtil.CONFIGURATION_MANAGER, job);
  }

  @Test
  public void testCreateJob() throws Exception {
    IJobConfiguration job = IJobConfiguration.build(makeProdJob());
    SanitizedConfiguration sanitized = fromUnsanitized(job);
    jobUpdateController.assertNotUpdating(JOB_KEY);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expectNoCronJob();
    expectInstanceQuotaCheck(sanitized, ENOUGH_QUOTA);

    stateManager.insertPendingTasks(
        storageUtil.mutableStoreProvider,
        sanitized.getJobConfig().getTaskConfig(),
        sanitized.getInstanceIds());

    control.replay();

    assertOkResponse(thrift.createJob(makeProdJob()));
    assertEquals(1L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateJobFailsForCron() throws Exception {
    IJobConfiguration job = IJobConfiguration.build(makeProdJob().setCronSchedule(""));

    control.replay();

    assertEquals(invalidResponse(NO_CRON), thrift.createJob(job.newBuilder()));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateJobFailsConfigCheck() throws Exception {
    IJobConfiguration job = IJobConfiguration.build(makeJob(INVALID_TASK_CONFIG));
    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job.newBuilder()));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateJobFailsJobExists() throws Exception {
    IJobConfiguration job = IJobConfiguration.build(makeJob());
    jobUpdateController.assertNotUpdating(JOB_KEY);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active(), buildScheduledTask());

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job.newBuilder()));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateJobFailsCronJobExists() throws Exception {
    IJobConfiguration job = IJobConfiguration.build(makeJob());
    jobUpdateController.assertNotUpdating(JOB_KEY);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expectCronJob();

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job.newBuilder()));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateJobFailsInstanceCheck() throws Exception {
    IJobConfiguration job = IJobConfiguration.build(
        makeJob(defaultTask(true), THRESHOLDS.getMaxTasksPerJob() + 1));

    jobUpdateController.assertNotUpdating(JOB_KEY);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expectNoCronJob();
    expect(quotaManager.checkInstanceAddition(
        anyObject(ITaskConfig.class),
        anyInt(),
        eq(storageUtil.mutableStoreProvider))).andReturn(ENOUGH_QUOTA);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job.newBuilder()));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateJobFailsQuotaCheck() throws Exception {
    IJobConfiguration job = IJobConfiguration.build(makeProdJob());
    SanitizedConfiguration sanitized = fromUnsanitized(job);
    jobUpdateController.assertNotUpdating(JOB_KEY);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expectNoCronJob();
    expectInstanceQuotaCheck(sanitized, NOT_ENOUGH_QUOTA);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job.newBuilder()));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  private void assertMessageMatches(Response response, String string) {
    // TODO(wfarner): This test coverage could be much better.  Circle back to apply more thorough
    // response contents testing throughout.
    assertTrue(Iterables.any(response.getDetails(), detail -> detail.getMessage().equals(string)));
  }

  @Test
  public void testCreateEmptyJob() throws Exception {
    control.replay();

    JobConfiguration job =
        new JobConfiguration().setKey(JOB_KEY.newBuilder()).setOwner(IDENTITY);
    assertResponse(INVALID_REQUEST, thrift.createJob(job));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateJobFailsNoExecutorOrContainerConfig() throws Exception {
    JobConfiguration job = makeJob();
    job.getTaskConfig().unsetExecutorConfig();

    control.replay();

    Response response = thrift.createJob(job);
    assertResponse(INVALID_REQUEST, response);
    assertMessageMatches(response, ConfigurationManager.NO_EXECUTOR_OR_CONTAINER);
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateHomogeneousJobNoInstances() throws Exception {
    JobConfiguration job = makeJob();
    job.setInstanceCount(0);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateJobNegativeInstanceCount() throws Exception {
    JobConfiguration job = makeJob();
    job.setInstanceCount(-1);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateJobNoResources() throws Exception {
    control.replay();

    TaskConfig task = productionTask();
    task.unsetResources();
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task)));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateJobBadCpu() throws Exception {
    control.replay();

    TaskConfig task = productionTask()
            .setResources(ImmutableSet.of(
                    numCpus(0.0),
                    ramMb(1024),
                    diskMb(1024)));
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task)));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateJobBadRam() throws Exception {
    control.replay();

    TaskConfig task = productionTask()
            .setResources(ImmutableSet.of(
                    numCpus(1),
                    ramMb(-123),
                    diskMb(1024)));
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task)));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateJobBadDisk() throws Exception {
    control.replay();

    TaskConfig task = productionTask()
            .setResources(ImmutableSet.of(
                    numCpus(1),
                    ramMb(1024),
                    diskMb(0)));
    task.unsetResources();
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task)));
  }

  @Test
  public void testCreateJobGoodResources() throws Exception {

    TaskConfig task = productionTask()
            .setResources(ImmutableSet.of(
                    numCpus(1.0),
                    ramMb(1024),
                    diskMb(1024)));

    IJobConfiguration job = IJobConfiguration.build(makeJob(task));
    SanitizedConfiguration sanitized = fromUnsanitized(job);
    jobUpdateController.assertNotUpdating(JOB_KEY);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expectNoCronJob();
    expectInstanceQuotaCheck(sanitized, ENOUGH_QUOTA);

    stateManager.insertPendingTasks(
            storageUtil.mutableStoreProvider,
            sanitized.getJobConfig().getTaskConfig(),
            sanitized.getInstanceIds());

    control.replay();

    assertOkResponse(thrift.createJob(job.newBuilder()));
  }

  @Test
  public void testCreateJobPopulateDefaults() throws Exception {
    Set<Resource> resources = ImmutableSet.of(numCpus(1.0), ramMb(1024), diskMb(1024));
    TaskConfig task = new TaskConfig()
        .setContactEmail("testing@twitter.com")
        .setExecutorConfig(
            new ExecutorConfig(EXECUTOR_NAME, "config")) // Arbitrary opaque data.
        .setResources(resources)
        .setIsService(true)
        .setProduction(true)
        .setTier(TaskTestUtil.PROD_TIER_NAME)
        .setOwner(IDENTITY)
        .setContainer(Container.mesos(new MesosContainer()))
        .setJob(JOB_KEY.newBuilder());
    JobConfiguration job = makeJob(task);

    JobConfiguration sanitized = job.deepCopy();
    sanitized.getTaskConfig()
        .setJob(JOB_KEY.newBuilder())
        .setPriority(0)
        .setIsService(true)
        .setProduction(true)
        .setTaskLinks(ImmutableMap.of())
        .setConstraints(ImmutableSet.of())
        .setMaxTaskFailures(0)
        .setResources(resources);

    jobUpdateController.assertNotUpdating(JOB_KEY);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expectNoCronJob();
    expectInstanceQuotaCheck(ITaskConfig.build(sanitized.getTaskConfig()), ENOUGH_QUOTA);
    stateManager.insertPendingTasks(
        storageUtil.mutableStoreProvider,
        ITaskConfig.build(sanitized.getTaskConfig()),
        ImmutableSet.of(0));

    control.replay();

    assertOkResponse(thrift.createJob(job));
    assertEquals(1L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testCreateUnauthorizedDedicatedJob() throws Exception {
    control.replay();

    TaskConfig task = nonProductionTask();
    task.setConstraints(ImmutableSet.of(dedicatedConstraint(ImmutableSet.of("mesos"))));
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task)));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testLimitConstraintForDedicatedJob() throws Exception {
    control.replay();

    TaskConfig task = nonProductionTask();
    task.setConstraints(ImmutableSet.of(dedicatedConstraint(1)));
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task)));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  @Test
  public void testMultipleValueConstraintForDedicatedJob() throws Exception {
    control.replay();

    TaskConfig task = nonProductionTask();
    task.setConstraints(ImmutableSet.of(dedicatedConstraint(ImmutableSet.of("mesos", "test"))));
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task)));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  private IScheduledTask buildTaskForJobUpdate(int instanceId) {
    return buildTaskForJobUpdate(instanceId, "data");
  }

  private IScheduledTask buildTaskForJobUpdate(int instanceId, String executorData) {
    return IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setInstanceId(instanceId)
            .setTask(populatedTask()
                .setIsService(true)
                .setExecutorConfig(new ExecutorConfig().setName(EXECUTOR_NAME)
                    .setData(executorData)))));
  }

  private IScheduledTask buildScheduledTask() {
    return buildScheduledTask(JOB_NAME, TASK_ID);
  }

  private static IScheduledTask buildScheduledTask(String jobName, String taskId) {
    return IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setTaskId(taskId)
            .setInstanceId(0)
            .setTask(new TaskConfig()
                .setJob(JOB_KEY.newBuilder().setName(jobName))
                .setOwner(IDENTITY))));
  }

  private void expectTransitionsToKilling() {
    expectTransitionsToKilling(Optional.absent());
  }

  private void expectTransitionsToKilling(Optional<String> message) {
    expect(auditMessages.killedByRemoteUser(message)).andReturn(Optional.of("test"));
    expect(stateManager.changeState(
        storageUtil.mutableStoreProvider,
        TASK_ID,
        Optional.absent(),
        ScheduleStatus.KILLING,
        Optional.of("test"))).andReturn(StateChangeResult.SUCCESS);
  }

  @Test
  public void testJobScopedKillsActive() throws Exception {
    Query.Builder query = Query.unscoped().byJob(JOB_KEY).active();
    storageUtil.expectTaskFetch(query, buildScheduledTask());
    jobUpdateController.assertNotUpdating(JOB_KEY);
    expectTransitionsToKilling();

    control.replay();

    assertOkResponse(thrift.killTasks(JOB_KEY.newBuilder(), null, null));
    assertEquals(1L, statsProvider.getLongValue(KILL_TASKS));
  }

  @Test
  public void testKillTasksInstanceScoped() throws Exception {
    Query.Builder query = Query.instanceScoped(JOB_KEY, ImmutableSet.of(1)).active();
    storageUtil.expectTaskFetch(query, buildScheduledTask());
    jobUpdateController.assertNotUpdating(JOB_KEY);
    expectTransitionsToKilling();

    control.replay();

    assertOkResponse(thrift.killTasks(JOB_KEY.newBuilder(), ImmutableSet.of(1), null));
    assertEquals(1L, statsProvider.getLongValue(KILL_TASKS));
  }

  @Test
  public void testKillTasksWhileJobUpdating() throws Exception {
    jobUpdateController.assertNotUpdating(JOB_KEY);
    expectLastCall().andThrow(new JobUpdatingException("job is updating"));

    control.replay();

    assertResponse(JOB_UPDATING_ERROR, thrift.killTasks(JOB_KEY.newBuilder(), null, null));
    assertEquals(0L, statsProvider.getLongValue(KILL_TASKS));
  }

  @Test
  public void testKillNonExistentTasks() throws Exception {
    Query.Builder query = Query.unscoped().byJob(JOB_KEY).active();
    storageUtil.expectTaskFetch(query);
    jobUpdateController.assertNotUpdating(JOB_KEY);

    control.replay();

    Response response = thrift.killTasks(JOB_KEY.newBuilder(), null, null);
    assertOkResponse(response);
    assertMessageMatches(response, SchedulerThriftInterface.NO_TASKS_TO_KILL_MESSAGE);
    assertEquals(0L, statsProvider.getLongValue(KILL_TASKS));
  }

  @Test
  public void testKillTasksWithMessage() throws Exception {
    String message = "Test message";
    Query.Builder query = Query.unscoped().byJob(JOB_KEY).active();
    storageUtil.expectTaskFetch(query, buildScheduledTask());
    jobUpdateController.assertNotUpdating(JOB_KEY);
    expectTransitionsToKilling(Optional.of(message));

    control.replay();

    assertOkResponse(thrift.killTasks(JOB_KEY.newBuilder(), null, message));
  }

  @Test
  public void testPruneTasksRejectsActiveStates() throws Exception {
    control.replay();
    Response rsp = thrift.pruneTasks(new TaskQuery().setStatuses(Tasks.ACTIVE_STATES));
    assertResponse(ResponseCode.ERROR, rsp);
  }

  @Test
  public void testPruneTasksRejectsMixedStates() throws Exception {
    control.replay();
    Response rsp = thrift.pruneTasks(new TaskQuery().setStatuses(
        ImmutableSet.of(ScheduleStatus.FINISHED, ScheduleStatus.KILLING)));
    assertResponse(ResponseCode.ERROR, rsp);
  }

  @Test
  public void testPruneTasksAddsDefaultStatuses() throws Exception {
    storageUtil.expectTaskFetch(
        Query.arbitrary(new TaskQuery().setStatuses(Tasks.TERMINAL_STATES)),
        buildScheduledTask());
    stateManager.deleteTasks(
        storageUtil.mutableStoreProvider,
        ImmutableSet.of(buildScheduledTask().getAssignedTask().getTaskId()));
    control.replay();

    assertOkResponse(thrift.pruneTasks(new TaskQuery()));
  }

  @Test
  public void testPruneTasksRespectsScopedTerminalState() throws Exception {
    storageUtil.expectTaskFetch(
        Query.arbitrary(new TaskQuery().setStatuses(ImmutableSet.of(ScheduleStatus.FAILED))),
        buildScheduledTask());
    stateManager.deleteTasks(
        storageUtil.mutableStoreProvider,
        ImmutableSet.of(buildScheduledTask().getAssignedTask().getTaskId()));
    control.replay();

    assertOkResponse(thrift.pruneTasks(
        new TaskQuery().setStatuses(ImmutableSet.of(ScheduleStatus.FAILED))));
  }

  @Test
  public void testPruneTasksAppliesQueryLimit() throws Exception {
    TaskQuery query = new TaskQuery().setLimit(3);
    storageUtil.expectTaskFetch(
        Query.arbitrary(query.setStatuses(Tasks.TERMINAL_STATES)),
        buildScheduledTask("a/b/c", "task1"),
        buildScheduledTask("a/b/c", "task2"),
        buildScheduledTask("a/b/c", "task3"),
        buildScheduledTask("a/b/c", "task4"),
        buildScheduledTask("a/b/c", "task5"));
    stateManager.deleteTasks(
        storageUtil.mutableStoreProvider,
        ImmutableSet.of("task1", "task2", "task3"));
    control.replay();

    assertOkResponse(thrift.pruneTasks(query));
  }

  @Test
  public void testSetQuota() throws Exception {
    ResourceAggregate resourceAggregate = new ResourceAggregate()
        .setNumCpus(10)
        .setDiskMb(100)
        .setRamMb(200);
    quotaManager.saveQuota(
        ROLE,
        IResourceAggregate.build(resourceAggregate.deepCopy()
            .setResources(ImmutableSet.of(numCpus(10), ramMb(200), diskMb(100)))),
        storageUtil.mutableStoreProvider);

    control.replay();

    assertOkResponse(thrift.setQuota(ROLE, resourceAggregate));
  }

  @Test
  public void testSetQuotaFails() throws Exception {
    ResourceAggregate resourceAggregate = new ResourceAggregate()
        .setNumCpus(10)
        .setDiskMb(100)
        .setRamMb(200);
    quotaManager.saveQuota(
        ROLE,
        IResourceAggregate.build(resourceAggregate.deepCopy()
            .setResources(ImmutableSet.of(numCpus(10), ramMb(200), diskMb(100)))),
        storageUtil.mutableStoreProvider);

    expectLastCall().andThrow(new QuotaManager.QuotaException("fail"));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.setQuota(ROLE, resourceAggregate));
  }

  @Test
  public void testForceTaskState() throws Exception {
    ScheduleStatus status = ScheduleStatus.FAILED;

    expect(auditMessages.transitionedBy()).andReturn(Optional.of("test"));
    expect(stateManager.changeState(
        storageUtil.mutableStoreProvider,
        TASK_ID,
        Optional.absent(),
        ScheduleStatus.FAILED,
        Optional.of("test"))).andReturn(StateChangeResult.SUCCESS);

    control.replay();

    assertOkResponse(thrift.forceTaskState(TASK_ID, status));
  }

  @Test
  public void testBackupControls() throws Exception {
    backup.backupNow();

    Set<String> backups = ImmutableSet.of("a", "b");
    expect(recovery.listBackups()).andReturn(backups);

    String backupId = "backup";
    recovery.stage(backupId);

    Query.Builder query = Query.taskScoped("taskId");
    Set<IScheduledTask> queryResult = ImmutableSet.of(
        IScheduledTask.build(new ScheduledTask().setStatus(ScheduleStatus.RUNNING)));
    expect(recovery.query(query)).andReturn(queryResult);

    recovery.deleteTasks(query);

    recovery.commit();

    recovery.unload();

    control.replay();

    assertEquals(okEmptyResponse(), thrift.performBackup());

    assertEquals(
        okResponse(Result.listBackupsResult(new ListBackupsResult().setBackups(backups))),
        thrift.listBackups());

    assertEquals(okEmptyResponse(), thrift.stageRecovery(backupId));

    assertEquals(
        okResponse(Result.queryRecoveryResult(
            new QueryRecoveryResult().setTasks(IScheduledTask.toBuildersSet(queryResult)))),
        thrift.queryRecovery(query.get().newBuilder()));

    assertEquals(okEmptyResponse(), thrift.deleteRecoveryTasks(query.get().newBuilder()));

    assertEquals(okEmptyResponse(), thrift.commitRecovery());

    assertEquals(okEmptyResponse(), thrift.unloadRecovery());
  }

  @Test
  public void testRecoveryException() throws Exception {
    Throwable recoveryException = new RecoveryException("Injected");

    String backupId = "backup";
    recovery.stage(backupId);
    expectLastCall().andThrow(recoveryException);

    control.replay();

    try {
      thrift.stageRecovery(backupId);
      fail("No recovery exception thrown.");
    } catch (RecoveryException e) {
      assertEquals(recoveryException.getMessage(), e.getMessage());
    }
  }

  @Test
  public void testRestartShards() throws Exception {
    Set<Integer> shards = ImmutableSet.of(0);

    jobUpdateController.assertNotUpdating(JOB_KEY);
    storageUtil.expectTaskFetch(
        Query.instanceScoped(JOB_KEY, shards).active(),
        buildScheduledTask());

    expect(auditMessages.restartedByRemoteUser())
        .andReturn(Optional.of("test"));
    expect(stateManager.changeState(
        storageUtil.mutableStoreProvider,
        TASK_ID,
        Optional.absent(),
        ScheduleStatus.RESTARTING,
        Optional.of("test"))).andReturn(StateChangeResult.SUCCESS);

    control.replay();

    assertOkResponse(
        thrift.restartShards(JOB_KEY.newBuilder(), shards));
    assertEquals(1L, statsProvider.getLongValue(RESTART_SHARDS));
  }

  @Test
  public void testRestartShardsWhileJobUpdating() throws Exception {
    Set<Integer> shards = ImmutableSet.of(1, 6);

    jobUpdateController.assertNotUpdating(JOB_KEY);
    expectLastCall().andThrow(new JobUpdatingException("job is updating"));

    control.replay();

    assertResponse(
        JOB_UPDATING_ERROR,
        thrift.restartShards(JOB_KEY.newBuilder(), shards));
    assertEquals(0L, statsProvider.getLongValue(KILL_TASKS));
  }

  @Test
  public void testRestartShardsNotFoundTasksFailure() throws Exception {
    Set<Integer> shards = ImmutableSet.of(1, 6);

    jobUpdateController.assertNotUpdating(JOB_KEY);
    storageUtil.expectTaskFetch(Query.instanceScoped(JOB_KEY, shards).active());

    control.replay();

    assertResponse(
        INVALID_REQUEST,
        thrift.restartShards(JOB_KEY.newBuilder(), shards));
    assertEquals(0L, statsProvider.getLongValue(KILL_TASKS));
  }

  @Test
  public void testReplaceCronTemplate() throws Exception {
    jobUpdateController.assertNotUpdating(JOB_KEY);
    SanitizedConfiguration sanitized = fromUnsanitized(IJobConfiguration.build(CRON_JOB));

    expectCronQuotaCheck(sanitized.getJobConfig(), ENOUGH_QUOTA);
    cronJobManager.updateJob(anyObject(SanitizedCronJob.class));
    control.replay();

    assertOkResponse(thrift.replaceCronTemplate(CRON_JOB));
    assertEquals(1L, statsProvider.getLongValue(CREATE_OR_UPDATE_CRON));
  }

  @Test
  public void testReplaceCronTemplateDoesNotExist() throws Exception {
    jobUpdateController.assertNotUpdating(JOB_KEY);
    SanitizedConfiguration sanitized = fromUnsanitized(IJobConfiguration.build(CRON_JOB));

    expectCronQuotaCheck(sanitized.getJobConfig(), ENOUGH_QUOTA);
    cronJobManager.updateJob(anyObject(SanitizedCronJob.class));
    expectLastCall().andThrow(new CronException("Nope"));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.replaceCronTemplate(CRON_JOB));
    assertEquals(0L, statsProvider.getLongValue(CREATE_OR_UPDATE_CRON));
  }

  @Test
  public void testStartCronJob() throws Exception {
    cronJobManager.startJobNow(JOB_KEY);
    control.replay();
    assertResponse(OK, thrift.startCronJob(JOB_KEY.newBuilder()));
  }

  @Test
  public void testStartCronJobFailsInCronManager() throws Exception {
    cronJobManager.startJobNow(JOB_KEY);
    expectLastCall().andThrow(new CronException("failed"));
    control.replay();
    assertResponse(INVALID_REQUEST, thrift.startCronJob(JOB_KEY.newBuilder()));
  }

  @Test
  public void testScheduleCronCreatesJob() throws Exception {
    jobUpdateController.assertNotUpdating(JOB_KEY);
    SanitizedConfiguration sanitized = fromUnsanitized(IJobConfiguration.build(CRON_JOB));

    expectCronQuotaCheck(sanitized.getJobConfig(), ENOUGH_QUOTA);

    expectNoCronJob().times(2);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    cronJobManager.createJob(SanitizedCronJob.from(sanitized));
    control.replay();
    assertResponse(OK, thrift.scheduleCronJob(CRON_JOB));
  }

  @Test
  public void testScheduleCronFailsCreationDueToExistingNonCron() throws Exception {
    jobUpdateController.assertNotUpdating(JOB_KEY);
    SanitizedConfiguration sanitized = fromUnsanitized(IJobConfiguration.build(CRON_JOB));

    expectCronQuotaCheck(sanitized.getJobConfig(), ENOUGH_QUOTA);

    expectNoCronJob();
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active(), buildScheduledTask());
    control.replay();
    assertEquals(
        invalidResponse(jobAlreadyExistsMessage(JOB_KEY)),
        thrift.scheduleCronJob(CRON_JOB));
  }

  @Test
  public void testScheduleCronUpdatesJob() throws Exception {
    jobUpdateController.assertNotUpdating(JOB_KEY);
    SanitizedConfiguration sanitized = fromUnsanitized(IJobConfiguration.build(CRON_JOB));

    expectCronQuotaCheck(sanitized.getJobConfig(), ENOUGH_QUOTA);

    expectCronJob();
    cronJobManager.updateJob(SanitizedCronJob.from(sanitized));
    control.replay();

    assertResponse(OK, thrift.scheduleCronJob(CRON_JOB));
  }

  @Test
  public void testScheduleCronJobFailedTaskConfigValidation() throws Exception {
    control.replay();
    IJobConfiguration job = IJobConfiguration.build(makeJob(INVALID_TASK_CONFIG));
    assertResponse(INVALID_REQUEST, thrift.scheduleCronJob(job.newBuilder()));
  }

  @Test
  public void testScheduleCronJobFailsWithNoCronSchedule() throws Exception {
    control.replay();

    assertEquals(
        invalidResponse(noCronScheduleMessage(JOB_KEY)),
        thrift.scheduleCronJob(makeJob()));
  }

  @Test
  public void testScheduleCronFailsQuotaCheck() throws Exception {
    jobUpdateController.assertNotUpdating(JOB_KEY);
    SanitizedConfiguration sanitized = fromUnsanitized(IJobConfiguration.build(CRON_JOB));

    expectCronQuotaCheck(sanitized.getJobConfig(), NOT_ENOUGH_QUOTA);

    control.replay();
    assertResponse(INVALID_REQUEST, thrift.scheduleCronJob(CRON_JOB));
  }

  @Test
  public void testDescheduleCronJob() throws Exception {
    jobUpdateController.assertNotUpdating(JOB_KEY);
    expect(cronJobManager.deleteJob(JOB_KEY)).andReturn(true);

    control.replay();

    assertResponse(OK, thrift.descheduleCronJob(CRON_JOB.getKey()));
  }

  @Test
  public void testDescheduleCronJobWhileJobUpdating() throws Exception {
    jobUpdateController.assertNotUpdating(JOB_KEY);
    expectLastCall().andThrow(new JobUpdatingException("job is updating"));
    control.replay();
    assertResponse(JOB_UPDATING_ERROR, thrift.descheduleCronJob(CRON_JOB.getKey()));
  }

  @Test
  public void testDescheduleNotACron() throws Exception {
    jobUpdateController.assertNotUpdating(JOB_KEY);
    expect(cronJobManager.deleteJob(JOB_KEY)).andReturn(false);
    control.replay();

    assertEquals(
        okEmptyResponse(notScheduledCronMessage(JOB_KEY)),
        thrift.descheduleCronJob(JOB_KEY.newBuilder()));
  }

  @Test
  public void testUnauthorizedDedicatedJob() throws Exception {
    control.replay();

    TaskConfig task = nonProductionTask();
    task.setConstraints(ImmutableSet.of(dedicatedConstraint(ImmutableSet.of("mesos"))));
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task)));
    assertEquals(0L, statsProvider.getLongValue(CREATE_JOB));
  }

  private static Set<IHostStatus> status(String host, MaintenanceMode mode) {
    return ImmutableSet.of(IHostStatus.build(new HostStatus(host, mode)));
  }

  @Test
  public void testHostMaintenance() throws Exception {
    Set<String> hostnames = ImmutableSet.of("a");
    Set<IHostStatus> none = status("a", NONE);
    Set<IHostStatus> scheduled = status("a", SCHEDULED);
    Set<IHostStatus> draining = status("a", DRAINING);
    Set<IHostStatus> drained = status("a", DRAINING);
    expect(maintenance.getStatus(hostnames)).andReturn(none);
    expect(maintenance.startMaintenance(hostnames)).andReturn(scheduled);
    expect(maintenance.drain(hostnames)).andReturn(draining);
    expect(maintenance.getStatus(hostnames)).andReturn(draining);
    expect(maintenance.getStatus(hostnames)).andReturn(drained);
    expect(maintenance.endMaintenance(hostnames)).andReturn(none);

    control.replay();

    Hosts hosts = new Hosts(hostnames);

    assertEquals(
        IHostStatus.toBuildersSet(none),
        thrift.maintenanceStatus(hosts).getResult().getMaintenanceStatusResult()
            .getStatuses());
    assertEquals(1L, statsProvider.getLongValue(MAINTENANCE_STATUS));
    assertEquals(
        IHostStatus.toBuildersSet(scheduled),
        thrift.startMaintenance(hosts).getResult().getStartMaintenanceResult()
            .getStatuses());
    assertEquals(1L, statsProvider.getLongValue(START_MAINTENANCE));
    assertEquals(
        IHostStatus.toBuildersSet(draining),
        thrift.drainHosts(hosts).getResult().getDrainHostsResult().getStatuses());
    assertEquals(1L, statsProvider.getLongValue(DRAIN_HOSTS));
    assertEquals(
        IHostStatus.toBuildersSet(draining),
        thrift.maintenanceStatus(hosts).getResult().getMaintenanceStatusResult()
            .getStatuses());
    assertEquals(2L, statsProvider.getLongValue(MAINTENANCE_STATUS));
    assertEquals(
        IHostStatus.toBuildersSet(drained),
        thrift.maintenanceStatus(hosts).getResult().getMaintenanceStatusResult()
            .getStatuses());
    assertEquals(3L, statsProvider.getLongValue(MAINTENANCE_STATUS));
    assertEquals(
        IHostStatus.toBuildersSet(none),
        thrift.endMaintenance(hosts).getResult().getEndMaintenanceResult().getStatuses());
    assertEquals(1L, statsProvider.getLongValue(END_MAINTENANCE));
  }

  private static Response okEmptyResponse() {
    return response(OK, Optional.absent());
  }

  private static Response okEmptyResponse(String message) {
    return Responses.empty()
        .setResponseCode(OK)
        .setDetails(ImmutableList.of(new ResponseDetail(message)));
  }

  private static Response invalidResponse(String message) {
    return Responses.empty()
        .setResponseCode(INVALID_REQUEST)
        .setDetails(ImmutableList.of(new ResponseDetail(message)));
  }

  @Test
  public void testSnapshot() throws Exception {
    snapshotStore.snapshot();
    expectLastCall();

    snapshotStore.snapshot();
    expectLastCall().andThrow(new StorageException("mock error!"));

    control.replay();

    assertOkResponse(thrift.snapshot());

    try {
      thrift.snapshot();
      fail("No StorageException thrown.");
    } catch (StorageException e) {
      // Expected.
    }
  }

  @Test
  public void testExplicitTaskReconciliationWithNoBatchSize() throws Exception {
    ExplicitReconciliationSettings settings = new ExplicitReconciliationSettings();

    taskReconciler.triggerExplicitReconciliation(Optional.absent());
    expectLastCall();

    control.replay();

    assertOkResponse(thrift.triggerExplicitTaskReconciliation(settings));
  }

  @Test
  public void testExplicitTaskReconciliationWithValidBatchSize() throws Exception {
    ExplicitReconciliationSettings settings = new ExplicitReconciliationSettings();
    settings.setBatchSize(10);

    taskReconciler.triggerExplicitReconciliation(Optional.of(settings.getBatchSize()));
    expectLastCall();

    control.replay();
    assertOkResponse(thrift.triggerExplicitTaskReconciliation(settings));
  }

  @Test
  public void testExplicitTaskReconciliationWithNegativeBatchSize() throws Exception {
    ExplicitReconciliationSettings settings = new ExplicitReconciliationSettings();
    settings.setBatchSize(-1000);

    control.replay();
    assertResponse(INVALID_REQUEST, thrift.triggerExplicitTaskReconciliation(settings));
  }

  @Test
  public void testExplicitTaskReconciliationWithZeroBatchSize() throws Exception {
    ExplicitReconciliationSettings settings = new ExplicitReconciliationSettings();
    settings.setBatchSize(0);

    control.replay();
    assertResponse(INVALID_REQUEST, thrift.triggerExplicitTaskReconciliation(settings));
  }

  @Test
  public void testImplicitTaskReconciliation() throws Exception {
    taskReconciler.triggerImplicitReconciliation();
    expectLastCall();

    control.replay();

    assertOkResponse(thrift.triggerImplicitTaskReconciliation());
  }

  @Test
  public void testAddInstancesWithInstanceKey() throws Exception {
    expectNoCronJob();
    jobUpdateController.assertNotUpdating(JOB_KEY);
    IScheduledTask activeTask = buildScheduledTask();
    ITaskConfig task = activeTask.getAssignedTask().getTask();
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active(), activeTask);
    expect(quotaManager.checkInstanceAddition(
        task,
        2,
        storageUtil.mutableStoreProvider)).andReturn(ENOUGH_QUOTA);
    stateManager.insertPendingTasks(
        storageUtil.mutableStoreProvider,
        task,
        ImmutableSet.of(1, 2));

    control.replay();

    assertOkResponse(thrift.addInstances(INSTANCE_KEY, 2));
    assertEquals(2L, statsProvider.getLongValue(ADD_INSTANCES));
  }

  @Test
  public void testAddInstancesWithInstanceKeyFailsWithNoInstance() throws Exception {
    expectNoCronJob();
    jobUpdateController.assertNotUpdating(JOB_KEY);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());

    control.replay();

    assertEquals(
        invalidResponse(SchedulerThriftInterface.INVALID_INSTANCE_ID),
        thrift.addInstances(INSTANCE_KEY, 2));
    assertEquals(0L, statsProvider.getLongValue(ADD_INSTANCES));
  }

  @Test
  public void testAddInstancesWithInstanceKeyFailsInvalidCount() throws Exception {
    control.replay();

    assertEquals(
        invalidResponse(SchedulerThriftInterface.INVALID_INSTANCE_COUNT),
        thrift.addInstances(INSTANCE_KEY, 0));
    assertEquals(0L, statsProvider.getLongValue(ADD_INSTANCES));
  }

  @Test
  public void testAddInstancesFailsCronJob() throws Exception {
    expectCronJob();

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.addInstances(INSTANCE_KEY, 1));
  }

  @Test(expected = StorageException.class)
  public void testAddInstancesFailsWithNonTransient() throws Exception {
    expect(storageUtil.jobStore.fetchJob(JOB_KEY)).andThrow(new StorageException("no retry"));

    control.replay();

    thrift.addInstances(INSTANCE_KEY, 1);
  }

  @Test
  public void testAddInstancesWhileJobUpdating() throws Exception {
    expectNoCronJob();
    jobUpdateController.assertNotUpdating(JOB_KEY);
    expectLastCall().andThrow(new JobUpdatingException("job is updating"));

    control.replay();

    assertResponse(JOB_UPDATING_ERROR, thrift.addInstances(INSTANCE_KEY, 1));
    assertEquals(0L, statsProvider.getLongValue(ADD_INSTANCES));
  }

  @Test
  public void testAddInstancesFailsQuotaCheck() throws Exception {
    IScheduledTask activeTask = buildScheduledTask();
    ITaskConfig task = activeTask.getAssignedTask().getTask();
    expectNoCronJob();
    jobUpdateController.assertNotUpdating(JOB_KEY);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active(), activeTask);
    expectInstanceQuotaCheck(task, NOT_ENOUGH_QUOTA);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.addInstances(INSTANCE_KEY, 1));
    assertEquals(0L, statsProvider.getLongValue(ADD_INSTANCES));
  }

  @Test
  public void testAddInstancesInstanceCollisionFailure() throws Exception {
    IScheduledTask activeTask = buildScheduledTask();
    ITaskConfig task = activeTask.getAssignedTask().getTask();
    expectNoCronJob();
    jobUpdateController.assertNotUpdating(JOB_KEY);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active(), activeTask);
    expectInstanceQuotaCheck(task, ENOUGH_QUOTA);
    stateManager.insertPendingTasks(
        storageUtil.mutableStoreProvider,
        task,
        ImmutableSet.of(1));
    expectLastCall().andThrow(new IllegalArgumentException("instance collision"));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.addInstances(INSTANCE_KEY, 1));
    assertEquals(0L, statsProvider.getLongValue(ADD_INSTANCES));
  }

  @Test
  public void testStartUpdate() throws Exception {
    expectGetRemoteUser();
    expectNoCronJob();

    ITaskConfig newTask = buildTaskForJobUpdate(0).getAssignedTask().getTask();

    IScheduledTask oldTask1 = buildTaskForJobUpdate(0, "old");
    IScheduledTask oldTask2 = buildTaskForJobUpdate(1, "old");
    IScheduledTask oldTask3 = buildTaskForJobUpdate(2, "old2");
    IScheduledTask oldTask4 = buildTaskForJobUpdate(3, "old2");
    IScheduledTask oldTask5 = buildTaskForJobUpdate(4, "old");
    IScheduledTask oldTask6 = buildTaskForJobUpdate(5, "old");
    IScheduledTask oldTask7 = buildTaskForJobUpdate(6, "old");

    IJobUpdate update = buildJobUpdate(6, newTask, ImmutableMap.of(
        oldTask1.getAssignedTask().getTask(), ImmutableSet.of(new Range(0, 1), new Range(4, 6)),
        oldTask3.getAssignedTask().getTask(), ImmutableSet.of(new Range(2, 3))
    ));

    expect(quotaManager.checkJobUpdate(
        update,
        storageUtil.mutableStoreProvider)).andReturn(ENOUGH_QUOTA);

    expect(uuidGenerator.createNew()).andReturn(UU_ID);
    storageUtil.expectTaskFetch(
        Query.unscoped().byJob(JOB_KEY).active(),
        oldTask1,
        oldTask2,
        oldTask3,
        oldTask4,
        oldTask5,
        oldTask6,
        oldTask7);

    jobUpdateController.start(update, AUDIT);

    control.replay();

    Response response =
        assertOkResponse(thrift.startJobUpdate(buildJobUpdateRequest(update), AUDIT_MESSAGE));
    assertEquals(
        new StartJobUpdateResult(UPDATE_KEY.newBuilder()).setUpdateSummary(
        update.getSummary().newBuilder()), response.getResult().getStartJobUpdateResult());
    assertEquals(6L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  private void expectJobUpdateQuotaCheck(QuotaCheckResult result) {
    expect(quotaManager.checkJobUpdate(
        anyObject(IJobUpdate.class),
        eq(storageUtil.mutableStoreProvider))).andReturn(result);
  }

  @Test
  public void testStartUpdateEmptyDesired() throws Exception {
    expectGetRemoteUser();
    expectNoCronJob();

    ITaskConfig newTask = buildTaskForJobUpdate(0).getAssignedTask().getTask();

    IScheduledTask oldTask1 = buildTaskForJobUpdate(0);
    IScheduledTask oldTask2 = buildTaskForJobUpdate(1);

    // Set instance count to 1 to generate empty desired state in diff.
    IJobUpdate update = buildJobUpdate(
        1,
        newTask,
        ImmutableMap.of(oldTask1.getAssignedTask().getTask(), ImmutableSet.of(new Range(0, 1))));

    expectJobUpdateQuotaCheck(ENOUGH_QUOTA);

    // Set diff-adjusted IJobUpdate expectations.
    JobUpdate expected = update.newBuilder();
    expected.getInstructions().setInitialState(ImmutableSet.of(
        new InstanceTaskConfig(newTask.newBuilder(), ImmutableSet.of(new Range(1, 1)))));
    expected.getInstructions().unsetDesiredState();

    expect(uuidGenerator.createNew()).andReturn(UU_ID);
    storageUtil.expectTaskFetch(
        Query.unscoped().byJob(JOB_KEY).active(),
        oldTask1,
        oldTask2);

    jobUpdateController.start(IJobUpdate.build(expected), AUDIT);

    control.replay();

    Response response = assertOkResponse(
        thrift.startJobUpdate(buildJobUpdateRequest(update), AUDIT_MESSAGE));
    assertEquals(
        new StartJobUpdateResult(UPDATE_KEY.newBuilder()).setUpdateSummary(expected.getSummary()),
        response.getResult().getStartJobUpdateResult());
    assertEquals(1L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test(expected = NullPointerException.class)
  public void testStartUpdateFailsNullRequest() throws Exception {
    control.replay();
    thrift.startJobUpdate(null, AUDIT_MESSAGE);
  }

  @Test(expected = NullPointerException.class)
  public void testStartUpdateFailsNullTaskConfig() throws Exception {
    control.replay();
    thrift.startJobUpdate(
        new JobUpdateRequest(null, 5, buildJobUpdateSettings()),
        AUDIT_MESSAGE);
  }

  @Test
  public void testStartUpdateFailsInvalidGroupSize() throws Exception {
    control.replay();

    JobUpdateRequest updateRequest = buildServiceJobUpdateRequest();
    updateRequest.getSettings().setUpdateGroupSize(0);

    assertEquals(
        invalidResponse(SchedulerThriftInterface.INVALID_GROUP_SIZE),
        thrift.startJobUpdate(updateRequest, AUDIT_MESSAGE));
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartUpdateFailsInvalidMaxInstanceFailures() throws Exception {
    control.replay();

    JobUpdateRequest updateRequest = buildServiceJobUpdateRequest();
    updateRequest.getSettings().setMaxPerInstanceFailures(-1);

    assertEquals(
        invalidResponse(SchedulerThriftInterface.INVALID_MAX_INSTANCE_FAILURES),
        thrift.startJobUpdate(updateRequest, AUDIT_MESSAGE));
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartUpdateFailsTooManyPerInstanceFailures() throws Exception {
    control.replay();

    JobUpdateRequest updateRequest = buildServiceJobUpdateRequest();
    updateRequest.getSettings()
        .setMaxPerInstanceFailures(THRESHOLDS.getMaxUpdateInstanceFailures() + 10);

    assertEquals(
        invalidResponse(SchedulerThriftInterface.TOO_MANY_POTENTIAL_FAILED_INSTANCES),
        thrift.startJobUpdate(updateRequest, AUDIT_MESSAGE));
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartUpdateFailsInvalidMaxFailedInstances() throws Exception {
    control.replay();

    JobUpdateRequest updateRequest = buildServiceJobUpdateRequest();
    updateRequest.getSettings().setMaxFailedInstances(-1);

    assertEquals(
        invalidResponse(SchedulerThriftInterface.INVALID_MAX_FAILED_INSTANCES),
        thrift.startJobUpdate(updateRequest, AUDIT_MESSAGE));
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartUpdateFailsInvalidMinWaitInRunning() throws Exception {
    control.replay();

    JobUpdateRequest updateRequest = buildServiceJobUpdateRequest();
    updateRequest.getSettings().setMinWaitInInstanceRunningMs(-1);

    assertEquals(
        invalidResponse(SchedulerThriftInterface.INVALID_MIN_WAIT_TO_RUNNING),
        thrift.startJobUpdate(updateRequest, AUDIT_MESSAGE));
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartUpdateFailsNonServiceTask() throws Exception {
    control.replay();
    JobUpdateRequest request = buildJobUpdateRequest(populatedTask().setIsService(false));
    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(request, AUDIT_MESSAGE));
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartUpdateFailsInvalidPulseTimeout() throws Exception {
    control.replay();

    JobUpdateRequest updateRequest = buildServiceJobUpdateRequest();
    updateRequest.getSettings().setBlockIfNoPulsesAfterMs(-1);

    assertEquals(
        invalidResponse(SchedulerThriftInterface.INVALID_PULSE_TIMEOUT),
        thrift.startJobUpdate(updateRequest, AUDIT_MESSAGE));
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartUpdateFailsForCronJob() throws Exception {
    JobUpdateRequest request = buildServiceJobUpdateRequest(populatedTask());
    expectCronJob();

    control.replay();
    assertEquals(invalidResponse(NO_CRON), thrift.startJobUpdate(request, AUDIT_MESSAGE));
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartUpdateFailsConfigValidation() throws Exception {
    JobUpdateRequest request = buildJobUpdateRequest(INVALID_TASK_CONFIG.setIsService(true));

    control.replay();
    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(request, AUDIT_MESSAGE));
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartNoopUpdate() throws Exception {
    expectNoCronJob();
    expect(uuidGenerator.createNew()).andReturn(UU_ID);
    ITaskConfig newTask = buildTaskForJobUpdate(0).getAssignedTask().getTask();

    IScheduledTask oldTask = buildTaskForJobUpdate(0);
    storageUtil.expectTaskFetch(Query.unscoped().byJob(JOB_KEY).active(), oldTask);

    IJobUpdate update = buildJobUpdate(
        1,
        newTask,
        ImmutableMap.of(oldTask.getAssignedTask().getTask(), ImmutableSet.of(new Range(0, 0))));

    control.replay();
    JobUpdateRequest request = buildJobUpdateRequest(update);
    Response response = thrift.startJobUpdate(request, AUDIT_MESSAGE);
    assertResponse(OK, response);
    assertEquals(
        NOOP_JOB_UPDATE_MESSAGE,
        Iterables.getOnlyElement(response.getDetails()).getMessage());
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartUpdateInvalidScope() throws Exception {
    expectNoCronJob();
    expect(uuidGenerator.createNew()).andReturn(UU_ID);

    IScheduledTask oldTask = buildTaskForJobUpdate(0);
    storageUtil.expectTaskFetch(Query.unscoped().byJob(JOB_KEY).active(), oldTask);

    ITaskConfig newTask = buildTaskForJobUpdate(0).getAssignedTask().getTask();
    JobUpdate builder = buildJobUpdate(
        1,
        newTask,
        ImmutableMap.of(oldTask.getAssignedTask().getTask(), ImmutableSet.of(new Range(0, 0))))
        .newBuilder();
    builder.getInstructions().getSettings()
        .setUpdateOnlyTheseInstances(ImmutableSet.of(new Range(100, 100)));

    control.replay();
    JobUpdateRequest request = buildJobUpdateRequest(IJobUpdate.build(builder));
    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(request, AUDIT_MESSAGE));
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartScopeIncludesNoop() throws Exception {
    // Test for regression of AURORA-1332: a scoped update should be allowed when unchanged
    // instances are included in the scope.

    expectGetRemoteUser();
    expectNoCronJob();
    expect(uuidGenerator.createNew()).andReturn(UU_ID);

    ScheduledTask taskBuilder = buildTaskForJobUpdate(0).newBuilder();
    taskBuilder.getAssignedTask().getTask()
            .setResources(ImmutableSet.of(
                    numCpus(100),
                    ramMb(1024),
                    diskMb(1024)));
    IScheduledTask newTask = IScheduledTask.build(taskBuilder);

    IScheduledTask oldTask1 = buildTaskForJobUpdate(1);
    IScheduledTask oldTask2 = buildTaskForJobUpdate(2);
    storageUtil.expectTaskFetch(
        Query.unscoped().byJob(JOB_KEY).active(),
        newTask, oldTask1, oldTask2);
    expectJobUpdateQuotaCheck(ENOUGH_QUOTA);
    jobUpdateController.start(EasyMock.<IJobUpdate>anyObject(), eq(AUDIT));

    ITaskConfig newConfig = newTask.getAssignedTask().getTask();
    JobUpdate builder = buildJobUpdate(
        3,
        newConfig,
        ImmutableMap.of(
            newConfig, ImmutableSet.of(new Range(0, 0)),
            oldTask1.getAssignedTask().getTask(), ImmutableSet.of(new Range(1, 2))))
        .newBuilder();
    builder.getInstructions().getSettings()
        .setUpdateOnlyTheseInstances(ImmutableSet.of(new Range(0, 2)));

    control.replay();
    JobUpdateRequest request = buildJobUpdateRequest(IJobUpdate.build(builder));
    assertResponse(OK, thrift.startJobUpdate(request, AUDIT_MESSAGE));
    assertEquals(3L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartUpdateFailsInstanceCountCheck() throws Exception {
    JobUpdateRequest request = buildServiceJobUpdateRequest(populatedTask());
    request.setInstanceCount(THRESHOLDS.getMaxTasksPerJob() + 1);
    expectGetRemoteUser();
    expectNoCronJob();
    expect(uuidGenerator.createNew()).andReturn(UU_ID);
    storageUtil.expectTaskFetch(Query.unscoped().byJob(JOB_KEY).active());
    expectJobUpdateQuotaCheck(ENOUGH_QUOTA);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(request, AUDIT_MESSAGE));
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartUpdateFailsQuotaCheck() throws Exception {
    JobUpdateRequest request = buildServiceJobUpdateRequest(populatedTask());
    expectGetRemoteUser();
    expectNoCronJob();
    expect(uuidGenerator.createNew()).andReturn(UU_ID);

    IScheduledTask oldTask = buildTaskForJobUpdate(0);
    storageUtil.expectTaskFetch(Query.unscoped().byJob(JOB_KEY).active(), oldTask);

    expectJobUpdateQuotaCheck(NOT_ENOUGH_QUOTA);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(request, AUDIT_MESSAGE));
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartUpdateFailsInController() throws Exception {
    expectGetRemoteUser();
    expectNoCronJob();

    IScheduledTask oldTask = buildTaskForJobUpdate(0, "old");
    ITaskConfig newTask = buildTaskForJobUpdate(0, "new").getAssignedTask().getTask();

    IJobUpdate update = buildJobUpdate(
        1,
        newTask,
        ImmutableMap.of(oldTask.getAssignedTask().getTask(), ImmutableSet.of(new Range(0, 0))));

    expect(uuidGenerator.createNew()).andReturn(UU_ID);
    expectJobUpdateQuotaCheck(ENOUGH_QUOTA);

    storageUtil.expectTaskFetch(Query.unscoped().byJob(JOB_KEY).active(), oldTask);
    jobUpdateController.start(update, AUDIT);
    expectLastCall().andThrow(new UpdateStateException("failed"));

    control.replay();

    assertResponse(
        INVALID_REQUEST,
        thrift.startJobUpdate(buildJobUpdateRequest(update), AUDIT_MESSAGE));
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testStartUpdateFailsInControllerWhenUpdateInProgress() throws Exception {
    expectGetRemoteUser();
    expectNoCronJob();

    IScheduledTask oldTask = buildTaskForJobUpdate(0, "old");
    ITaskConfig newTask = buildTaskForJobUpdate(0, "new").getAssignedTask().getTask();

    IJobUpdate update = buildJobUpdate(
        1,
        newTask,
        ImmutableMap.of(oldTask.getAssignedTask().getTask(), ImmutableSet.of(new Range(0, 0))));

    expect(uuidGenerator.createNew()).andReturn(UU_ID);
    expectJobUpdateQuotaCheck(ENOUGH_QUOTA);

    storageUtil.expectTaskFetch(Query.unscoped().byJob(JOB_KEY).active(), oldTask);
    jobUpdateController.start(update, AUDIT);
    expectLastCall().andThrow(new UpdateInProgressException("failed", update.getSummary()));

    control.replay();

    Response response = thrift.startJobUpdate(buildJobUpdateRequest(update), AUDIT_MESSAGE);

    assertResponse(INVALID_REQUEST, response);
    assertEquals(
        new StartJobUpdateResult(UPDATE_KEY.newBuilder()).setUpdateSummary(
            update.getSummary().newBuilder()), response.getResult().getStartJobUpdateResult());
    assertEquals(0L, statsProvider.getLongValue(START_JOB_UPDATE));
  }

  @Test
  public void testPauseJobUpdateByCoordinator() throws Exception {
    expectGetRemoteUser();
    jobUpdateController.pause(UPDATE_KEY, AUDIT);

    control.replay();

    assertResponse(OK, thrift.pauseJobUpdate(UPDATE_KEY.newBuilder(), AUDIT_MESSAGE));
  }

  @Test
  public void testPauseJobUpdateByUser() throws Exception {
    expectGetRemoteUser();
    jobUpdateController.pause(UPDATE_KEY, AUDIT);

    control.replay();

    assertResponse(OK, thrift.pauseJobUpdate(UPDATE_KEY.newBuilder(), AUDIT_MESSAGE));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPauseMessageTooLong() throws Exception {
    expectGetRemoteUser();

    control.replay();

    assertResponse(
        OK,
        thrift.pauseJobUpdate(
            UPDATE_KEY.newBuilder(),
            Strings.repeat("*", AuditData.MAX_MESSAGE_LENGTH + 1)));
  }

  @Test
  public void testPauseJobUpdateFailsInController() throws Exception {
    expectGetRemoteUser();
    jobUpdateController.pause(UPDATE_KEY, AUDIT);
    expectLastCall().andThrow(new UpdateStateException("failed"));

    control.replay();

    assertResponse(
        INVALID_REQUEST,
        thrift.pauseJobUpdate(UPDATE_KEY.newBuilder(), AUDIT_MESSAGE));
  }

  @Test
  public void testResumeJobUpdate() throws Exception {
    expectGetRemoteUser();
    jobUpdateController.resume(UPDATE_KEY, AUDIT);

    control.replay();

    assertResponse(OK, thrift.resumeJobUpdate(UPDATE_KEY.newBuilder(), AUDIT_MESSAGE));
  }

  @Test
  public void testResumeJobUpdateFailsInController() throws Exception {
    expectGetRemoteUser();
    jobUpdateController.resume(UPDATE_KEY, AUDIT);
    expectLastCall().andThrow(new UpdateStateException("failed"));

    control.replay();

    assertResponse(
        INVALID_REQUEST,
        thrift.resumeJobUpdate(UPDATE_KEY.newBuilder(), AUDIT_MESSAGE));
  }

  @Test
  public void testAbortJobUpdateByCoordinator() throws Exception {
    expectGetRemoteUser();
    jobUpdateController.abort(UPDATE_KEY, AUDIT);

    control.replay();

    assertResponse(OK, thrift.abortJobUpdate(UPDATE_KEY.newBuilder(), AUDIT_MESSAGE));
  }

  @Test
  public void testAbortJobUpdateByUser() throws Exception {
    expectGetRemoteUser();
    jobUpdateController.abort(UPDATE_KEY, AUDIT);

    control.replay();

    assertResponse(OK, thrift.abortJobUpdate(UPDATE_KEY.newBuilder(), AUDIT_MESSAGE));
  }

  @Test
  public void testAbortJobUpdateFailsInController() throws Exception {
    expectGetRemoteUser();
    jobUpdateController.abort(UPDATE_KEY, AUDIT);
    expectLastCall().andThrow(new UpdateStateException("failed"));

    control.replay();

    assertResponse(
        INVALID_REQUEST,
        thrift.abortJobUpdate(UPDATE_KEY.newBuilder(), AUDIT_MESSAGE));
  }

  @Test
  public void testPulseJobUpdatePulsedAsCoordinator() throws Exception {
    expect(jobUpdateController.pulse(UPDATE_KEY)).andReturn(JobUpdatePulseStatus.OK);

    control.replay();

    assertEquals(
        okResponse(Result.pulseJobUpdateResult(
            new PulseJobUpdateResult(JobUpdatePulseStatus.OK))),
        thrift.pulseJobUpdate(UPDATE_KEY.newBuilder()));
  }

  @Test
  public void testPulseJobUpdatePulsedAsUser() throws Exception {
    expect(jobUpdateController.pulse(UPDATE_KEY)).andReturn(JobUpdatePulseStatus.OK);

    control.replay();

    assertEquals(
        okResponse(Result.pulseJobUpdateResult(new PulseJobUpdateResult(JobUpdatePulseStatus.OK))),
        thrift.pulseJobUpdate(UPDATE_KEY.newBuilder()));
  }

  @Test
  public void testPulseJobUpdateFails() throws Exception {
    expect(jobUpdateController.pulse(UPDATE_KEY)).andThrow(new UpdateStateException("failure"));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.pulseJobUpdate(UPDATE_KEY.newBuilder()));
  }

  @Test
  public void testGetJobUpdateSummaries() throws Exception {
    Response updateSummary = Responses.empty()
        .setResponseCode(OK)
        .setDetails(ImmutableList.of(new ResponseDetail("summary")));

    expect(readOnlyScheduler.getJobUpdateSummaries(
        anyObject(JobUpdateQuery.class))).andReturn(updateSummary);
    control.replay();

    assertEquals(updateSummary, thrift.getJobUpdateSummaries(new JobUpdateQuery()));
  }

  private IExpectationSetters<String> expectGetRemoteUser() {
    return expect(auditMessages.getRemoteUserName()).andReturn(USER);
  }

  private static TaskConfig populatedTask() {
    return defaultTask(true).setConstraints(Sets.newHashSet(
        new Constraint("host", TaskConstraint.limit(new LimitConstraint(1)))));
  }

  private static Constraint dedicatedConstraint(int value) {
    return new Constraint(DEDICATED_ATTRIBUTE, TaskConstraint.limit(new LimitConstraint(value)));
  }

  private static Constraint dedicatedConstraint(Set<String> values) {
    return new Constraint(DEDICATED_ATTRIBUTE,
        TaskConstraint.value(new ValueConstraint(false, values)));
  }

  private static JobUpdateRequest buildServiceJobUpdateRequest() {
    return buildServiceJobUpdateRequest(defaultTask(true));
  }

  private static JobUpdateRequest buildServiceJobUpdateRequest(TaskConfig config) {
    return buildJobUpdateRequest(config.setIsService(true));
  }

  private static JobUpdateRequest buildJobUpdateRequest(TaskConfig config) {
    return new JobUpdateRequest()
        .setInstanceCount(6)
        .setSettings(buildJobUpdateSettings())
        .setTaskConfig(config);
  }

  private static JobUpdateSettings buildJobUpdateSettings() {
    return new JobUpdateSettings()
        .setUpdateGroupSize(10)
        .setMaxFailedInstances(2)
        .setMaxPerInstanceFailures(1)
        .setMinWaitInInstanceRunningMs(15000)
        .setRollbackOnFailure(true);
  }

  private static Integer rangesToInstanceCount(Set<IRange> ranges) {
    int instanceCount = 0;
    for (IRange range : ranges) {
      instanceCount += range.getLast() - range.getFirst() + 1;
    }

    return instanceCount;
  }

  private static JobUpdateRequest buildJobUpdateRequest(IJobUpdate update) {
    return new JobUpdateRequest()
        .setInstanceCount(rangesToInstanceCount(
            update.getInstructions().getDesiredState().getInstances()))
        .setSettings(update.getInstructions().getSettings().newBuilder())
        .setTaskConfig(update.getInstructions().getDesiredState().getTask().newBuilder())
        .setMetadata(IMetadata.toBuildersSet(update.getSummary().getMetadata()));
  }

  private static IJobUpdate buildJobUpdate(
      int instanceCount,
      ITaskConfig newConfig,
      ImmutableMap<ITaskConfig, ImmutableSet<Range>> oldConfigMap) {

    ImmutableSet.Builder<InstanceTaskConfig> builder = ImmutableSet.builder();
    for (Map.Entry<ITaskConfig, ImmutableSet<Range>> entry : oldConfigMap.entrySet()) {
      builder.add(new InstanceTaskConfig(entry.getKey().newBuilder(), entry.getValue()));
    }

    return IJobUpdate.build(new JobUpdate()
        .setSummary(new JobUpdateSummary()
            .setKey(UPDATE_KEY.newBuilder())
            .setUser(IDENTITY.getUser())
            .setMetadata(METADATA))
        .setInstructions(new JobUpdateInstructions()
            .setSettings(buildJobUpdateSettings())
            .setDesiredState(new InstanceTaskConfig()
                .setTask(newConfig.newBuilder())
                .setInstances(ImmutableSet.of(new Range(0, instanceCount - 1))))
            .setInitialState(builder.build())));
  }

  private IExpectationSetters<?> expectCronJob() {
    return expect(storageUtil.jobStore.fetchJob(JOB_KEY))
        .andReturn(Optional.of(IJobConfiguration.build(CRON_JOB)));
  }

  private IExpectationSetters<?> expectNoCronJob() {
    return expect(storageUtil.jobStore.fetchJob(JOB_KEY))
        .andReturn(Optional.absent());
  }

  private IExpectationSetters<?> expectInstanceQuotaCheck(
      SanitizedConfiguration sanitized,
      QuotaCheckResult result) {

    return expectInstanceQuotaCheck(sanitized.getJobConfig().getTaskConfig(), result);
  }

  private IExpectationSetters<?> expectInstanceQuotaCheck(
      ITaskConfig config,
      QuotaCheckResult result) {

    return expect(quotaManager.checkInstanceAddition(
        config,
        1,
        storageUtil.mutableStoreProvider)).andReturn(result);
  }

  private IExpectationSetters<?> expectCronQuotaCheck(
      IJobConfiguration config,
      QuotaCheckResult result) {

    return expect(quotaManager.checkCronUpdate(config, storageUtil.mutableStoreProvider))
        .andReturn(result);
  }
}

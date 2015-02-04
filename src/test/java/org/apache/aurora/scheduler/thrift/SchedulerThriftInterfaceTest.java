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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.auth.CapabilityValidator;
import org.apache.aurora.auth.CapabilityValidator.AuditCheck;
import org.apache.aurora.auth.CapabilityValidator.Capability;
import org.apache.aurora.auth.SessionValidator.AuthFailedException;
import org.apache.aurora.gen.AddInstancesConfig;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.ConfigGroup;
import org.apache.aurora.gen.ConfigRewrite;
import org.apache.aurora.gen.ConfigSummary;
import org.apache.aurora.gen.ConfigSummaryResult;
import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.HostStatus;
import org.apache.aurora.gen.Hosts;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.InstanceConfigRewrite;
import org.apache.aurora.gen.InstanceKey;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobConfigRewrite;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobStats;
import org.apache.aurora.gen.JobSummary;
import org.apache.aurora.gen.JobSummaryResult;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateRequest;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.LimitConstraint;
import org.apache.aurora.gen.ListBackupsResult;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.MesosContainer;
import org.apache.aurora.gen.PendingReason;
import org.apache.aurora.gen.QueryRecoveryResult;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ResponseCode;
import org.apache.aurora.gen.ResponseDetail;
import org.apache.aurora.gen.Result;
import org.apache.aurora.gen.RewriteConfigsRequest;
import org.apache.aurora.gen.RoleSummary;
import org.apache.aurora.gen.RoleSummaryResult;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.ServerInfo;
import org.apache.aurora.gen.SessionKey;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.gen.ValueConstraint;
import org.apache.aurora.scheduler.TaskIdGenerator;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Query.Builder;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.configuration.SanitizedConfiguration;
import org.apache.aurora.scheduler.cron.CronException;
import org.apache.aurora.scheduler.cron.CronJobManager;
import org.apache.aurora.scheduler.cron.CronPredictor;
import org.apache.aurora.scheduler.cron.CrontabEntry;
import org.apache.aurora.scheduler.cron.SanitizedCronJob;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.metadata.NearestFit;
import org.apache.aurora.scheduler.quota.QuotaCheckResult;
import org.apache.aurora.scheduler.quota.QuotaInfo;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.state.LockManager;
import org.apache.aurora.scheduler.state.LockManager.LockException;
import org.apache.aurora.scheduler.state.MaintenanceController;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.state.UUIDGenerator;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.NonVolatileStorage;
import org.apache.aurora.scheduler.storage.backup.Recovery;
import org.apache.aurora.scheduler.storage.backup.StorageBackup;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.ILockKey;
import org.apache.aurora.scheduler.storage.entities.IRange;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.IServerInfo;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.EnableUpdater;
import org.apache.aurora.scheduler.thrift.aop.AopModule;
import org.apache.aurora.scheduler.updater.JobUpdateController;
import org.apache.aurora.scheduler.updater.UpdateStateException;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.auth.CapabilityValidator.Capability.MACHINE_MAINTAINER;
import static org.apache.aurora.auth.CapabilityValidator.Capability.PROVISIONER;
import static org.apache.aurora.auth.CapabilityValidator.Capability.ROOT;
import static org.apache.aurora.auth.SessionValidator.SessionContext;
import static org.apache.aurora.gen.LockValidation.CHECKED;
import static org.apache.aurora.gen.LockValidation.UNCHECKED;
import static org.apache.aurora.gen.MaintenanceMode.DRAINING;
import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.apache.aurora.gen.MaintenanceMode.SCHEDULED;
import static org.apache.aurora.gen.ResponseCode.AUTH_FAILED;
import static org.apache.aurora.gen.ResponseCode.ERROR;
import static org.apache.aurora.gen.ResponseCode.ERROR_TRANSIENT;
import static org.apache.aurora.gen.ResponseCode.INVALID_REQUEST;
import static org.apache.aurora.gen.ResponseCode.LOCK_ERROR;
import static org.apache.aurora.gen.ResponseCode.OK;
import static org.apache.aurora.gen.ResponseCode.WARNING;
import static org.apache.aurora.gen.apiConstants.THRIFT_API_VERSION;
import static org.apache.aurora.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;
import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.INSUFFICIENT_QUOTA;
import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.SUFFICIENT_QUOTA;
import static org.apache.aurora.scheduler.storage.backup.Recovery.RecoveryException;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.MAX_TASKS_PER_JOB;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.MAX_TASK_ID_LENGTH;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.NOOP_JOB_UPDATE_MESSAGE;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.NO_CRON;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.jobAlreadyExistsMessage;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.killedByMessage;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.noCronScheduleMessage;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.notScheduledCronMessage;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.restartedByMessage;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.transitionMessage;
import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SchedulerThriftInterfaceTest extends EasyMockTest {

  private static final String ROLE = "bar_role";
  private static final String USER = "foo_user";
  private static final String JOB_NAME = "job_foo";
  private static final Identity ROLE_IDENTITY = new Identity(ROLE, USER);
  private static final SessionKey SESSION = new SessionKey();
  private static final IJobKey JOB_KEY = JobKeys.from(ROLE, "devel", JOB_NAME);
  private static final ILockKey LOCK_KEY = ILockKey.build(LockKey.job(JOB_KEY.newBuilder()));
  private static final ILock LOCK =
      ILock.build(new Lock().setKey(LOCK_KEY.newBuilder()).setToken("token"));
  private static final JobConfiguration CRON_JOB = makeJob().setCronSchedule("* * * * *");
  private static final Lock DEFAULT_LOCK = null;
  private static final String TASK_ID = "task_id";
  private static final String UPDATE_ID = "82d6d790-3212-11e3-aa6e-0800200c9a74";
  private static final UUID UU_ID = UUID.fromString(UPDATE_ID);

  private static final IResourceAggregate QUOTA =
      IResourceAggregate.build(new ResourceAggregate(10.0, 1024, 2048));

  private static final IResourceAggregate CONSUMED =
      IResourceAggregate.build(new ResourceAggregate(0.0, 0, 0));

  private static final QuotaCheckResult ENOUGH_QUOTA = new QuotaCheckResult(SUFFICIENT_QUOTA);
  private static final QuotaCheckResult NOT_ENOUGH_QUOTA = new QuotaCheckResult(INSUFFICIENT_QUOTA);

  private static final ServerInfo SERVER_INFO =
      new ServerInfo()
          .setClusterName("test")
          .setThriftAPIVersion(THRIFT_API_VERSION)
          .setStatsUrlPrefix("fake_url");
  private static final String CRON_SCHEDULE = "0 * * * *";

  private StorageTestUtil storageUtil;
  private LockManager lockManager;
  private CapabilityValidator userValidator;
  private SessionContext context;
  private StorageBackup backup;
  private Recovery recovery;
  private MaintenanceController maintenance;
  private AuroraAdmin.Iface thrift;
  private CronJobManager cronJobManager;
  private CronPredictor cronPredictor;
  private QuotaManager quotaManager;
  private NearestFit nearestFit;
  private StateManager stateManager;
  private TaskIdGenerator taskIdGenerator;
  private UUIDGenerator uuidGenerator;
  private JobUpdateController jobUpdateController;

  @Before
  public void setUp() throws Exception {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    lockManager = createMock(LockManager.class);
    userValidator = createMock(CapabilityValidator.class);
    context = createMock(SessionContext.class);
    setUpValidationExpectations();
    backup = createMock(StorageBackup.class);
    recovery = createMock(Recovery.class);
    maintenance = createMock(MaintenanceController.class);
    cronJobManager = createMock(CronJobManager.class);
    cronPredictor = createMock(CronPredictor.class);
    quotaManager = createMock(QuotaManager.class);
    nearestFit = createMock(NearestFit.class);
    stateManager = createMock(StateManager.class);
    taskIdGenerator = createMock(TaskIdGenerator.class);
    uuidGenerator = createMock(UUIDGenerator.class);
    jobUpdateController = createMock(JobUpdateController.class);

    // Use guice and install AuthModule to apply AOP-style auth layer.
    Module testModule = new AbstractModule() {
      @Override
      protected void configure() {
        bind(NonVolatileStorage.class).toInstance(storageUtil.storage);
        bind(LockManager.class).toInstance(lockManager);
        bind(CapabilityValidator.class).toInstance(userValidator);
        bind(StorageBackup.class).toInstance(backup);
        bind(Recovery.class).toInstance(recovery);
        bind(MaintenanceController.class).toInstance(maintenance);
        bind(CronJobManager.class).toInstance(cronJobManager);
        bind(QuotaManager.class).toInstance(quotaManager);
        bind(AuroraAdmin.Iface.class).to(SchedulerThriftInterface.class);
        bind(IServerInfo.class).toInstance(IServerInfo.build(SERVER_INFO));
        bind(CronPredictor.class).toInstance(cronPredictor);
        bind(NearestFit.class).toInstance(nearestFit);
        bind(StateManager.class).toInstance(stateManager);
        bind(TaskIdGenerator.class).toInstance(taskIdGenerator);
        bind(UUIDGenerator.class).toInstance(uuidGenerator);
        bind(JobUpdateController.class).toInstance(jobUpdateController);
        bind(Boolean.class).annotatedWith(EnableUpdater.class).toInstance(true);
      }
    };
    Injector injector = Guice.createInjector(testModule, new AopModule());
    thrift = getResponseProxy(injector.getInstance(AuroraAdmin.Iface.class));
  }

  private static AuroraAdmin.Iface getResponseProxy(final AuroraAdmin.Iface realThrift) {
    // Capture all API method calls to validate response objects.
    Class<AuroraAdmin.Iface> thriftClass = AuroraAdmin.Iface.class;
    return (AuroraAdmin.Iface) Proxy.newProxyInstance(
        thriftClass.getClassLoader(),
        new Class<?>[] {thriftClass},
        new InvocationHandler() {
          @Override
          public Object invoke(Object o, Method method, Object[] args) throws Throwable {
            Response response = (Response) method.invoke(realThrift, args);
            assertTrue(response.isSetResponseCode());
            assertNotNull(response.getDetails());
            return response;
          }
        });
  }

  private void setUpValidationExpectations() throws Exception {
    expect(userValidator.toString(SESSION)).andReturn(USER).anyTimes();
    expect(context.getIdentity()).andReturn(USER).anyTimes();
  }

  private static Response assertOkResponse(Response response) {
    return assertResponse(OK, response);
  }

  private static Response assertResponse(ResponseCode expected, Response response) {
    assertEquals(expected, response.getResponseCode());
    return response;
  }

  @Test
  public void testPopulateJobConfig() throws Exception {
    IJobConfiguration job = IJobConfiguration.build(makeJob());
    SanitizedConfiguration sanitized = SanitizedConfiguration.fromUnsanitized(job);
    control.replay();

    Response response = assertOkResponse(thrift.populateJobConfig(job.newBuilder()));
    assertEquals(
        ImmutableSet.of(sanitized.getJobConfig().getTaskConfig().newBuilder()),
        response.getResult().getPopulateJobResult().getPopulatedDEPRECATED());

    assertEquals(
        sanitized.getJobConfig().getTaskConfig().newBuilder(),
        response.getResult().getPopulateJobResult().getTaskConfig());
  }

  @Test
  public void testPopulateJobConfigFails() throws Exception {
    IJobConfiguration job = IJobConfiguration.build(makeJob(null));
    control.replay();

    assertResponse(ResponseCode.INVALID_REQUEST, thrift.populateJobConfig(job.newBuilder()));
  }

  @Test
  public void testCreateJobNoLock() throws Exception {
    // Validate key is populated during sanitizing.
    JobConfiguration jobConfig = makeProdJob();
    jobConfig.getTaskConfig().unsetJob();

    IJobConfiguration job = IJobConfiguration.build(makeProdJob());
    SanitizedConfiguration sanitized = SanitizedConfiguration.fromUnsanitized(job);
    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(false);
    expect(taskIdGenerator.generate(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(TASK_ID);
    expect(quotaManager.checkInstanceAddition(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(ENOUGH_QUOTA);

    stateManager.insertPendingTasks(
        storageUtil.mutableStoreProvider,
        sanitized.getJobConfig().getTaskConfig(),
        sanitized.getInstanceIds());

    control.replay();

    assertOkResponse(thrift.createJob(jobConfig, DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testCreateJobWithLock() throws Exception {
    IJobConfiguration job = IJobConfiguration.build(makeProdJob());
    SanitizedConfiguration sanitized = SanitizedConfiguration.fromUnsanitized(job);
    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(LOCK));
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(false);
    expect(taskIdGenerator.generate(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(TASK_ID);
    expect(quotaManager.checkInstanceAddition(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(ENOUGH_QUOTA);

    stateManager.insertPendingTasks(
        storageUtil.mutableStoreProvider,
        sanitized.getJobConfig().getTaskConfig(),
        sanitized.getInstanceIds());

    control.replay();

    assertOkResponse(thrift.createJob(job.newBuilder(), LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testCreateJobFailsForCron() throws Exception {
    IJobConfiguration job = IJobConfiguration.build(makeProdJob().setCronSchedule(""));
    expectAuth(ROLE, true);

    control.replay();

    assertEquals(
        invalidResponse(NO_CRON),
        thrift.createJob(job.newBuilder(), LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testCreateJobFailsAuth() throws Exception {
    IJobConfiguration job = IJobConfiguration.build(makeJob());
    expectAuth(ROLE, false);
    control.replay();

    assertResponse(
        AUTH_FAILED,
        thrift.createJob(job.newBuilder(), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testCreateJobFailsConfigCheck() throws Exception {
    IJobConfiguration job = IJobConfiguration.build(makeJob(null));
    expectAuth(ROLE, true);
    control.replay();

    assertResponse(
        INVALID_REQUEST,
        thrift.createJob(job.newBuilder(), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testCreateJobFailsLockCheck() throws Exception {
    IJobConfiguration job = IJobConfiguration.build(makeJob());
    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(LOCK));
    expectLastCall().andThrow(new LockException("Invalid lock"));

    control.replay();

    assertResponse(LOCK_ERROR, thrift.createJob(job.newBuilder(), LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testCreateJobFailsJobExists() throws Exception {
    IJobConfiguration job = IJobConfiguration.build(makeJob());
    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active(), buildScheduledTask());

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job.newBuilder(), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testCreateJobFailsCronJobExists() throws Exception {
    IJobConfiguration job = IJobConfiguration.build(makeJob());
    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(true);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job.newBuilder(), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testCreateJobFailsInstanceCheck() throws Exception {
    IJobConfiguration job = IJobConfiguration.build(
        makeJob(defaultTask(true), MAX_TASKS_PER_JOB.get() + 1));

    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(false);
    expect(quotaManager.checkInstanceAddition(anyObject(ITaskConfig.class), anyInt()))
        .andReturn(ENOUGH_QUOTA);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job.newBuilder(), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testCreateJobFailsTaskIdLength() throws Exception {
    IJobConfiguration job = IJobConfiguration.build(makeJob());
    SanitizedConfiguration sanitized = SanitizedConfiguration.fromUnsanitized(job);
    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(false);
    expect(quotaManager.checkInstanceAddition(anyObject(ITaskConfig.class), anyInt()))
        .andReturn(ENOUGH_QUOTA);
    expect(taskIdGenerator.generate(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(Strings.repeat("a", MAX_TASK_ID_LENGTH + 1));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job.newBuilder(), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testCreateJobFailsQuotaCheck() throws Exception {
    IJobConfiguration job = IJobConfiguration.build(makeProdJob());
    SanitizedConfiguration sanitized = SanitizedConfiguration.fromUnsanitized(job);
    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(false);
    expect(taskIdGenerator.generate(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(TASK_ID);
    expect(quotaManager.checkInstanceAddition(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(NOT_ENOUGH_QUOTA);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job.newBuilder(), DEFAULT_LOCK, SESSION));
  }

  private void assertMessageMatches(Response response, final String string) {
    // TODO(wfarner): This test coverage could be much better.  Circle back to apply more thorough
    // response contents testing throughout.
    assertTrue(Iterables.any(response.getDetails(), new Predicate<ResponseDetail>() {
      @Override
      public boolean apply(ResponseDetail detail) {
        return detail.getMessage().equals(string);
      }
    }));
  }

  @Test
  public void testCreateEmptyJob() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    JobConfiguration job =
        new JobConfiguration().setKey(JOB_KEY.newBuilder()).setOwner(ROLE_IDENTITY);
    assertResponse(INVALID_REQUEST, thrift.createJob(job, DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testCreateJobFailsNoExecutorConfig() throws Exception {
    JobConfiguration job = makeJob();
    job.getTaskConfig().unsetExecutorConfig();
    expectAuth(ROLE, true);

    control.replay();

    Response response = thrift.createJob(job, LOCK.newBuilder(), SESSION);
    assertResponse(INVALID_REQUEST, response);
    // TODO(wfarner): Don't rely on a magic string here, reference a constant from the source.
    assertMessageMatches(response, "Configuration may not be null");
  }

  @Test
  public void testCreateHomogeneousJobNoInstances() throws Exception {
    JobConfiguration job = makeJob();
    job.unsetInstanceCount();
    expectAuth(ROLE, true);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job, DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testCreateJobNegativeInstanceCount() throws Exception {
    JobConfiguration job = makeJob();
    job.setInstanceCount(0 - 1);
    expectAuth(ROLE, true);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job, DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testCreateJobNoResources() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    TaskConfig task = productionTask();
    task.unsetNumCpus();
    task.unsetRamMb();
    task.unsetDiskMb();
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testCreateJobBadCpu() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    TaskConfig task = productionTask().setNumCpus(0.0);
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testCreateJobBadRam() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    TaskConfig task = productionTask().setRamMb(-123);
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testCreateJobBadDisk() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    TaskConfig task = productionTask().setDiskMb(0);
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testCreateJobPopulateDefaults() throws Exception {
    TaskConfig task = new TaskConfig()
        .setContactEmail("testing@twitter.com")
        .setExecutorConfig(new ExecutorConfig("aurora", "config"))  // Arbitrary opaque data.
        .setNumCpus(1.0)
        .setRamMb(1024)
        .setDiskMb(1024)
        .setIsService(true)
        .setProduction(true)
        .setOwner(ROLE_IDENTITY)
        .setEnvironment("devel")
        .setContainer(Container.mesos(new MesosContainer()))
        .setJobName(JOB_NAME);
    JobConfiguration job = makeJob(task);

    expectAuth(ROLE, true);

    JobConfiguration sanitized = job.deepCopy();
    sanitized.getTaskConfig()
        .setJob(JOB_KEY.newBuilder())
        .setNumCpus(1.0)
        .setPriority(0)
        .setRamMb(1024)
        .setDiskMb(1024)
        .setIsService(true)
        .setProduction(true)
        .setRequestedPorts(ImmutableSet.<String>of())
        .setTaskLinks(ImmutableMap.<String, String>of())
        .setConstraints(ImmutableSet.of(
            ConfigurationManager.hostLimitConstraint(1),
            ConfigurationManager.rackLimitConstraint(1)))
        .setMaxTaskFailures(1)
        .setEnvironment("devel");

    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(false);
    expect(taskIdGenerator.generate(ITaskConfig.build(sanitized.getTaskConfig()), 1))
        .andReturn(TASK_ID);
    expect(quotaManager.checkInstanceAddition(ITaskConfig.build(sanitized.getTaskConfig()), 1))
        .andReturn(ENOUGH_QUOTA);
    stateManager.insertPendingTasks(
        storageUtil.mutableStoreProvider,
        ITaskConfig.build(sanitized.getTaskConfig()),
        ImmutableSet.of(0));

    control.replay();

    assertOkResponse(thrift.createJob(job, DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testCreateUnauthorizedDedicatedJob() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    TaskConfig task = nonProductionTask();
    task.addToConstraints(dedicatedConstraint(ImmutableSet.of("mesos")));
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testLimitConstraintForDedicatedJob() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    TaskConfig task = nonProductionTask();
    task.addToConstraints(dedicatedConstraint(1));
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testMultipleValueConstraintForDedicatedJob() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    TaskConfig task = nonProductionTask();
    task.addToConstraints(dedicatedConstraint(ImmutableSet.of("mesos", "test")));
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task), DEFAULT_LOCK, SESSION));
  }

  private IScheduledTask buildTaskForJobUpdate(int instanceId) {
    return buildTaskForJobUpdate(instanceId, "data");
  }

  private IScheduledTask buildTaskForJobUpdate(int instanceId, String executorData) {
    return IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setInstanceId(instanceId)
            .setTask(ConfigurationManager.applyDefaultsIfUnset(populatedTask()
                .setRamMb(5)
                .setIsService(true)
                .setExecutorConfig(new ExecutorConfig().setData(executorData))))));
  }

  private IScheduledTask buildScheduledTask() {
    return buildScheduledTask(JOB_NAME, TASK_ID);
  }

  private static IScheduledTask buildScheduledTask(String jobName, String taskId) {
    return IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setTaskId(taskId)
            .setTask(new TaskConfig()
                .setJob(JOB_KEY.newBuilder().setName(jobName))
                .setOwner(ROLE_IDENTITY)
                .setEnvironment("devel")
                .setJobName(jobName))));
  }

  private void expectTransitionsToKilling() {
    expect(stateManager.changeState(
        storageUtil.mutableStoreProvider,
        TASK_ID,
        Optional.<ScheduleStatus>absent(),
        ScheduleStatus.KILLING,
        killedByMessage(USER))).andReturn(true);
  }

  @Test
  public void testUserKillTasks() throws Exception {
    Query.Builder query = Query.unscoped().byJob(JOB_KEY).active();
    expectAuth(ROOT, false);
    expectAuth(ROLE, true);
    storageUtil.expectTaskFetch(query, buildScheduledTask());
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(LOCK));
    expectTransitionsToKilling();

    control.replay();

    assertOkResponse(thrift.killTasks(query.get(), LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testAdminKillTasks() throws Exception {
    Query.Builder query = Query.unscoped().byJob(JOB_KEY).active();
    expectAuth(ROOT, true);
    storageUtil.expectTaskFetch(query, buildScheduledTask());
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    expectTransitionsToKilling();

    control.replay();

    assertOkResponse(thrift.killTasks(query.get(), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testKillByJobName() throws Exception {
    TaskQuery query = new TaskQuery().setJobName("job");
    expectAuth(ROOT, true);
    storageUtil.expectTaskFetch(Query.arbitrary(query).active(), buildScheduledTask());
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    expectTransitionsToKilling();

    control.replay();

    assertEquals(okEmptyResponse(), thrift.killTasks(query, DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testKillQueryActive() throws Exception {
    Query.Builder query = Query.unscoped().byJob(JOB_KEY);
    expectAuth(ROOT, true);
    storageUtil.expectTaskFetch(query.active(), buildScheduledTask());
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    expectTransitionsToKilling();

    control.replay();

    assertOkResponse(thrift.killTasks(query.get(), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testKillTasksLockCheckFailed() throws Exception {
    Query.Builder query = Query.unscoped().byJob(JOB_KEY).active();
    IScheduledTask task2 = buildScheduledTask("job_bar", TASK_ID);
    ILockKey key2 = ILockKey.build(LockKey.job(
        JobKeys.from(ROLE, "devel", "job_bar").newBuilder()));
    expectAuth(ROOT, false);
    expectAuth(ROLE, true);
    storageUtil.expectTaskFetch(query, buildScheduledTask(), task2);
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(LOCK));
    lockManager.validateIfLocked(key2, Optional.of(LOCK));
    expectLastCall().andThrow(new LockException("Failed lock check."));

    control.replay();

    assertResponse(LOCK_ERROR, thrift.killTasks(query.get(), LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testKillByTaskId() throws Exception {
    // A non-admin user may kill their own tasks when specified by task IDs.
    Query.Builder query = Query.taskScoped("taskid");
    expectAuth(ROOT, false);
    expectAuth(ImmutableSet.of(ROLE), true);
    // This query happens twice - once for authentication (without consistency) and once again
    // to perform the state change (within a write transaction).
    storageUtil.expectTaskFetch(query.active(), buildScheduledTask()).times(2);
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    expectTransitionsToKilling();

    control.replay();

    assertOkResponse(thrift.killTasks(query.get(), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testKillByStatus() throws Exception {
    // A non-admin user may not kill arbitrary tasks.
    Query.Builder query = Query.statusScoped(ScheduleStatus.RUNNING);
    expectAuth(ROOT, false);

    control.replay();

    assertResponse(AUTH_FAILED, thrift.killTasks(query.get(), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testKillWithRoleSpecs() throws Exception {
    // The query performed here is somewhat nonsensical, since we would not have any tasks owned by
    // multiple roles.  However, that behavior is defined in the storage system.
    Query.Builder query = Query.arbitrary(new TaskQuery()
        .setRole("a")
        .setJobKeys(ImmutableSet.of(JobKeys.from("b", "devel", "job").newBuilder())));

    expectAuth(ROOT, false);
    expectAuth(ImmutableSet.of("a", "b"), true);
    storageUtil.expectTaskFetch(query.active(), buildScheduledTask());
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    expectTransitionsToKilling();

    control.replay();

    assertOkResponse(thrift.killTasks(query.get(), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testKillTasksAuthFailure() throws Exception {
    Query.Builder query = Query.unscoped().byJob(JOB_KEY).active();
    expectAuth(ROOT, false);
    expectAuth(ROLE, false);

    control.replay();

    assertResponse(AUTH_FAILED, thrift.killTasks(query.get(), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testKillTasksInvalidJobName() throws Exception {
    TaskQuery query = new TaskQuery()
        .setOwner(ROLE_IDENTITY)
        .setJobName("");

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.killTasks(query, DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testKillNonExistentTasks() throws Exception {
    Query.Builder query = Query.unscoped().byJob(JOB_KEY).active();
    expectAuth(ROOT, true);
    storageUtil.expectTaskFetch(query);

    control.replay();

    Response response = thrift.killTasks(query.get(), DEFAULT_LOCK, SESSION);
    assertOkResponse(response);
    assertMessageMatches(response, SchedulerThriftInterface.NO_TASKS_TO_KILL_MESSAGE);
  }

  @Test
  public void testKillAuthenticatesQueryRole() throws Exception {
    expectAuth(ROOT, false);
    expectAuth(ImmutableSet.of("foo"), true);

    Query.Builder query = Query.roleScoped("foo").active();

    storageUtil.expectTaskFetch(query, buildScheduledTask());
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    expectTransitionsToKilling();

    control.replay();

    assertOkResponse(thrift.killTasks(query.get(), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testKillCronAuthenticatesQueryJobKeyRole() throws Exception {
    expectAuth(ROOT, false);
    IJobKey key = JobKeys.from("role", "env", "job");

    Query.Builder query = Query.arbitrary(new TaskQuery().setJobKeys(
        ImmutableSet.of(key.newBuilder())));

    storageUtil.expectTaskFetch(query.active());
    expectAuth(ImmutableSet.of("role"), true);

    control.replay();
    assertOkResponse(thrift.killTasks(query.get(), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testSetQuota() throws Exception {
    ResourceAggregate resourceAggregate = new ResourceAggregate()
        .setNumCpus(10)
        .setDiskMb(100)
        .setRamMb(200);
    expectAuth(ROOT, true);
    quotaManager.saveQuota(ROLE, IResourceAggregate.build(resourceAggregate));

    control.replay();

    assertOkResponse(thrift.setQuota(ROLE, resourceAggregate, SESSION));
  }

  @Test
  public void testSetQuotaFails() throws Exception {
    ResourceAggregate resourceAggregate = new ResourceAggregate()
        .setNumCpus(10)
        .setDiskMb(100)
        .setRamMb(200);
    expectAuth(ROOT, true);
    quotaManager.saveQuota(ROLE, IResourceAggregate.build(resourceAggregate));
    expectLastCall().andThrow(new QuotaManager.QuotaException("fail"));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.setQuota(ROLE, resourceAggregate, SESSION));
  }

  @Test
  public void testProvisionerSetQuota() throws Exception {
    ResourceAggregate resourceAggregate = new ResourceAggregate()
        .setNumCpus(10)
        .setDiskMb(100)
        .setRamMb(200);
    expectAuth(ROOT, false);
    expectAuth(PROVISIONER, true);
    quotaManager.saveQuota(ROLE, IResourceAggregate.build(resourceAggregate));

    control.replay();

    assertOkResponse(thrift.setQuota(ROLE, resourceAggregate, SESSION));
  }

  @Test
  public void testMachineMaintainerAccess() throws Exception {
    Hosts hosts = new Hosts()
        .setHostNames(ImmutableSet.of("host1"));
    Set<HostStatus> statuses = ImmutableSet.of();

    expectAuth(ROOT, false);
    expectAuth(MACHINE_MAINTAINER, true);
    expect(maintenance.startMaintenance(hosts.getHostNames())).andReturn(statuses);

    expectAuth(ROOT, false);
    expectAuth(MACHINE_MAINTAINER, true);
    expect(maintenance.drain(hosts.getHostNames())).andReturn(statuses);

    expectAuth(ROOT, false);
    expectAuth(MACHINE_MAINTAINER, true);
    expect(maintenance.getStatus(hosts.getHostNames())).andReturn(statuses);

    expectAuth(ROOT, false);
    expectAuth(MACHINE_MAINTAINER, true);
    expect(maintenance.endMaintenance(hosts.getHostNames())).andReturn(statuses);

    control.replay();

    assertOkResponse(thrift.startMaintenance(hosts, SESSION));
    assertOkResponse(thrift.drainHosts(hosts, SESSION));
    assertOkResponse(thrift.maintenanceStatus(hosts, SESSION));
    assertOkResponse(thrift.endMaintenance(hosts, SESSION));
  }

  @Test
  public void testMachineMaintenanceAccessDenied() throws Exception {
    Hosts hosts = new Hosts().setHostNames(ImmutableSet.of("host1"));

    expectAuth(ROOT, false).times(4);
    expectAuth(MACHINE_MAINTAINER, false).times(4);

    control.replay();

    assertResponse(AUTH_FAILED, thrift.startMaintenance(hosts, SESSION));
    assertResponse(AUTH_FAILED, thrift.drainHosts(hosts, SESSION));
    assertResponse(AUTH_FAILED, thrift.maintenanceStatus(hosts, SESSION));
    assertResponse(AUTH_FAILED, thrift.endMaintenance(hosts, SESSION));
  }

  @Test
  public void testSetQuotaAuthFailure() throws Exception {
    ResourceAggregate resourceAggregate = new ResourceAggregate()
        .setNumCpus(10)
        .setDiskMb(100)
        .setRamMb(200);
    expectAuth(ROOT, false);
    expectAuth(PROVISIONER, false);

    control.replay();

    assertResponse(AUTH_FAILED, thrift.setQuota(ROLE, resourceAggregate, SESSION));
  }

  @Test
  public void testForceTaskState() throws Exception {
    ScheduleStatus status = ScheduleStatus.FAILED;

    expect(stateManager.changeState(
        storageUtil.mutableStoreProvider,
        TASK_ID,
        Optional.<ScheduleStatus>absent(),
        ScheduleStatus.FAILED,
        Optional.of(transitionMessage(USER).get()))).andReturn(true);

    // Expect auth is first called by an interceptor and then by SchedulerThriftInterface to extract
    // the SessionContext.
    // Note: This will change after AOP-style session validation passes in a SessionContext.
    expectAuth(ROOT, true).times(2);

    // Auth for the second call.  AOP auth fails, but second auth attempt required to get
    // SessionContext fails.  This is pretty contrived, but not technically impossible.
    expectAuth(ROOT, true);
    expectAuth(ROOT, false);

    control.replay();

    assertOkResponse(thrift.forceTaskState(TASK_ID, status, SESSION));
    assertEquals(
        response(AUTH_FAILED, Optional.<Result>absent(), AUTH_DENIED_MESSAGE),
        thrift.forceTaskState(TASK_ID, status, SESSION));
  }

  @Test
  public void testBackupControls() throws Exception {
    expectAuth(ROOT, true);
    backup.backupNow();

    expectAuth(ROOT, true);
    Set<String> backups = ImmutableSet.of("a", "b");
    expect(recovery.listBackups()).andReturn(backups);

    expectAuth(ROOT, true);
    String backupId = "backup";
    recovery.stage(backupId);

    expectAuth(ROOT, true);
    Query.Builder query = Query.taskScoped("taskId");
    Set<IScheduledTask> queryResult = ImmutableSet.of(
        IScheduledTask.build(new ScheduledTask().setStatus(ScheduleStatus.RUNNING)));
    expect(recovery.query(query)).andReturn(queryResult);

    expectAuth(ROOT, true);
    recovery.deleteTasks(query);

    expectAuth(ROOT, true);
    recovery.commit();

    expectAuth(ROOT, true);
    recovery.unload();

    control.replay();

    assertEquals(okEmptyResponse(), thrift.performBackup(SESSION));

    assertEquals(
        okResponse(Result.listBackupsResult(new ListBackupsResult().setBackups(backups))),
        thrift.listBackups(SESSION));

    assertEquals(okEmptyResponse(), thrift.stageRecovery(backupId, SESSION));

    assertEquals(
        okResponse(Result.queryRecoveryResult(
            new QueryRecoveryResult().setTasks(IScheduledTask.toBuildersSet(queryResult)))),
        thrift.queryRecovery(query.get(), SESSION));

    assertEquals(okEmptyResponse(), thrift.deleteRecoveryTasks(query.get(), SESSION));

    assertEquals(okEmptyResponse(), thrift.commitRecovery(SESSION));

    assertEquals(okEmptyResponse(), thrift.unloadRecovery(SESSION));
  }

  @Test
  public void testRecoveryException() throws Exception {
    Throwable recoveryException = new RecoveryException("Injected");

    expectAuth(ROOT, true);
    String backupId = "backup";
    recovery.stage(backupId);
    expectLastCall().andThrow(recoveryException);

    control.replay();

    assertEquals(
        errorResponse(recoveryException.getMessage()),
        thrift.stageRecovery(backupId, SESSION));
  }

  @Test
  public void testForceTaskStateAuthFailure() throws Exception {
    expectAuth(ROOT, false);

    control.replay();

    assertResponse(AUTH_FAILED, thrift.forceTaskState("task", ScheduleStatus.FAILED, SESSION));
  }

  @Test
  public void testRestartShards() throws Exception {
    Set<Integer> shards = ImmutableSet.of(0);

    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(LOCK));
    storageUtil.expectTaskFetch(
        Query.instanceScoped(JOB_KEY, shards).active(),
        buildScheduledTask());

    expect(stateManager.changeState(
        storageUtil.mutableStoreProvider,
        TASK_ID,
        Optional.<ScheduleStatus>absent(),
        ScheduleStatus.RESTARTING,
        restartedByMessage(USER))).andReturn(true);

    control.replay();

    assertOkResponse(
        thrift.restartShards(JOB_KEY.newBuilder(), shards, LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testRestartShardsAuthFailure() throws Exception {
    expectAuth(ROLE, false);

    control.replay();

    assertResponse(
        AUTH_FAILED,
        thrift.restartShards(JOB_KEY.newBuilder(), ImmutableSet.of(0), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testRestartShardsLockCheckFails() throws Exception {
    Set<Integer> shards = ImmutableSet.of(1, 6);

    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(LOCK));
    expectLastCall().andThrow(new LockException("test"));

    control.replay();

    assertResponse(
        LOCK_ERROR,
        thrift.restartShards(JOB_KEY.newBuilder(), shards, LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testRestartShardsNotFoundTasksFailure() throws Exception {
    Set<Integer> shards = ImmutableSet.of(1, 6);

    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(LOCK));
    storageUtil.expectTaskFetch(Query.instanceScoped(JOB_KEY, shards).active());

    control.replay();

    assertResponse(
        INVALID_REQUEST,
        thrift.restartShards(JOB_KEY.newBuilder(), shards, LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testReplaceCronTemplate() throws Exception {
    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    SanitizedConfiguration sanitized =
        SanitizedConfiguration.fromUnsanitized(IJobConfiguration.build(CRON_JOB));

    expect(taskIdGenerator.generate(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(TASK_ID);
    expect(quotaManager.checkInstanceAddition(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(ENOUGH_QUOTA);

    cronJobManager.updateJob(anyObject(SanitizedCronJob.class));
    control.replay();

    // Validate key is populated during sanitizing.
    JobConfiguration jobConfig = CRON_JOB;
    jobConfig.getTaskConfig().unsetJob();
    assertOkResponse(thrift.replaceCronTemplate(jobConfig, DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testReplaceCronTemplateFailedAuth() throws Exception {
    expectAuth(ROLE, false);

    control.replay();

    assertResponse(AUTH_FAILED, thrift.replaceCronTemplate(CRON_JOB, DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testReplaceCronTemplateFailedLockValidation() throws Exception {
    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    expectLastCall().andThrow(new LockException("Failed lock."));
    control.replay();

    assertResponse(LOCK_ERROR, thrift.replaceCronTemplate(CRON_JOB, DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testReplaceCronTemplateDoesNotExist() throws Exception {
    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    SanitizedConfiguration sanitized =
        SanitizedConfiguration.fromUnsanitized(IJobConfiguration.build(CRON_JOB));

    expect(taskIdGenerator.generate(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(TASK_ID);
    expect(quotaManager.checkInstanceAddition(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(ENOUGH_QUOTA);

    cronJobManager.updateJob(anyObject(SanitizedCronJob.class));
    expectLastCall().andThrow(new CronException("Nope"));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.replaceCronTemplate(CRON_JOB, DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testStartCronJob() throws Exception {
    expectAuth(ROLE, true);
    cronJobManager.startJobNow(JOB_KEY);
    control.replay();
    assertResponse(OK, thrift.startCronJob(JOB_KEY.newBuilder(), SESSION));
  }

  @Test
  public void testStartCronJobFailsAuth() throws Exception {
    expectAuth(ROLE, false);
    control.replay();
    assertResponse(AUTH_FAILED, thrift.startCronJob(JOB_KEY.newBuilder(), SESSION));
  }

  @Test
  public void testStartCronJobFailsInCronManager() throws Exception {
    expectAuth(ROLE, true);
    cronJobManager.startJobNow(JOB_KEY);
    expectLastCall().andThrow(new CronException("failed"));
    control.replay();
    assertResponse(INVALID_REQUEST, thrift.startCronJob(JOB_KEY.newBuilder(), SESSION));
  }

  @Test
  public void testScheduleCronCreatesJob() throws Exception {
    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    SanitizedConfiguration sanitized =
        SanitizedConfiguration.fromUnsanitized(IJobConfiguration.build(CRON_JOB));

    expect(taskIdGenerator.generate(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(TASK_ID);
    expect(quotaManager.checkInstanceAddition(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(ENOUGH_QUOTA);

    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(false).times(2);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    cronJobManager.createJob(SanitizedCronJob.from(sanitized));
    control.replay();
    assertResponse(OK, thrift.scheduleCronJob(CRON_JOB, DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testScheduleCronFailsCreationDueToExistingNonCron() throws Exception {
    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    SanitizedConfiguration sanitized =
        SanitizedConfiguration.fromUnsanitized(IJobConfiguration.build(CRON_JOB));

    expect(taskIdGenerator.generate(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(TASK_ID);
    expect(quotaManager.checkInstanceAddition(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(ENOUGH_QUOTA);

    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(false);
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active(), buildScheduledTask());
    control.replay();
    assertEquals(
        invalidResponse(jobAlreadyExistsMessage(JOB_KEY)),
        thrift.scheduleCronJob(CRON_JOB, DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testScheduleCronUpdatesJob() throws Exception {
    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    SanitizedConfiguration sanitized =
        SanitizedConfiguration.fromUnsanitized(IJobConfiguration.build(CRON_JOB));

    expect(taskIdGenerator.generate(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(TASK_ID);
    expect(quotaManager.checkInstanceAddition(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(ENOUGH_QUOTA);

    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(true);
    cronJobManager.updateJob(SanitizedCronJob.from(sanitized));
    control.replay();

    // Validate key is populated during sanitizing.
    JobConfiguration jobConfig = CRON_JOB;
    jobConfig.getTaskConfig().unsetJob();
    assertResponse(OK, thrift.scheduleCronJob(jobConfig, DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testUpdateScheduledCronJobFailedAuth() throws Exception {
    expectAuth(ROLE, false);
    control.replay();
    assertResponse(AUTH_FAILED, thrift.scheduleCronJob(CRON_JOB, DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testScheduleCronJobFailedTaskConfigValidation() throws Exception {
    expectAuth(ROLE, true);
    control.replay();
    IJobConfiguration job = IJobConfiguration.build(makeJob(null));
    assertResponse(
        INVALID_REQUEST,
        thrift.scheduleCronJob(job.newBuilder(), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testScheduleCronJobFailsLockValidation() throws Exception {
    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(LOCK));
    expectLastCall().andThrow(new LockException("Failed lock"));
    control.replay();
    assertResponse(LOCK_ERROR, thrift.scheduleCronJob(CRON_JOB, LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testScheduleCronJobFailsWithNoCronSchedule() throws Exception {
    expectAuth(ROLE, true);
    control.replay();

    assertEquals(
        invalidResponse(noCronScheduleMessage(JOB_KEY)),
        thrift.scheduleCronJob(makeJob(), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testScheduleCronFailsQuotaCheck() throws Exception {
    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    SanitizedConfiguration sanitized =
        SanitizedConfiguration.fromUnsanitized(IJobConfiguration.build(CRON_JOB));

    expect(taskIdGenerator.generate(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(TASK_ID);
    expect(quotaManager.checkInstanceAddition(sanitized.getJobConfig().getTaskConfig(), 1))
        .andReturn(NOT_ENOUGH_QUOTA);

    control.replay();
    assertResponse(INVALID_REQUEST, thrift.scheduleCronJob(CRON_JOB, DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testDescheduleCronJob() throws Exception {
    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    expect(cronJobManager.deleteJob(JOB_KEY)).andReturn(true);
    control.replay();
    assertResponse(OK, thrift.descheduleCronJob(CRON_JOB.getKey(), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testDescheduleCronJobFailsAuth() throws Exception {
    expectAuth(ROLE, false);
    control.replay();
    assertResponse(AUTH_FAILED,
        thrift.descheduleCronJob(CRON_JOB.getKey(), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testDescheduleCronJobFailsLockValidation() throws Exception {
    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    expectLastCall().andThrow(new LockException("Failed lock"));
    control.replay();
    assertResponse(LOCK_ERROR, thrift.descheduleCronJob(CRON_JOB.getKey(), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testDescheduleNotACron() throws Exception {
    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    expect(cronJobManager.deleteJob(JOB_KEY)).andReturn(false);
    control.replay();

    assertEquals(
        invalidResponse(notScheduledCronMessage(JOB_KEY)),
        thrift.descheduleCronJob(JOB_KEY.newBuilder(), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testRewriteShardTaskMissing() throws Exception {
    InstanceKey instance = new InstanceKey(JobKeys.from("foo", "bar", "baz").newBuilder(), 0);

    expectAuth(ROOT, true);
    storageUtil.expectTaskFetch(
        Query.instanceScoped(IJobKey.build(instance.getJobKey()), instance.getInstanceId())
            .active());

    control.replay();

    RewriteConfigsRequest request = new RewriteConfigsRequest(
        ImmutableList.of(ConfigRewrite.instanceRewrite(
            new InstanceConfigRewrite(instance, productionTask(), productionTask()))));
    assertResponse(WARNING, thrift.rewriteConfigs(request, SESSION));
  }

  @Test
  public void testRewriteNoCommands() throws Exception {
    expectAuth(ROOT, true);

    control.replay();

    RewriteConfigsRequest request = new RewriteConfigsRequest(ImmutableList.<ConfigRewrite>of());
    assertResponse(INVALID_REQUEST, thrift.rewriteConfigs(request, SESSION));
  }

  @Test
  public void testRewriteInvalidJob() throws Exception {
    expectAuth(ROOT, true);

    control.replay();

    IJobConfiguration job = IJobConfiguration.build(makeJob());
    RewriteConfigsRequest request = new RewriteConfigsRequest(
        ImmutableList.of(ConfigRewrite.jobRewrite(
            new JobConfigRewrite(job.newBuilder(), job.newBuilder().setTaskConfig(null)))));
    assertResponse(ERROR, thrift.rewriteConfigs(request, SESSION));
  }

  @Test
  public void testRewriteChangeJobKey() throws Exception {
    expectAuth(ROOT, true);

    control.replay();

    IJobConfiguration job = IJobConfiguration.build(makeJob());
    JobKey rewrittenJobKey = JobKeys.from("a", "b", "c").newBuilder();
    Identity rewrittenIdentity = new Identity(rewrittenJobKey.getRole(), "steve");
    RewriteConfigsRequest request = new RewriteConfigsRequest(
        ImmutableList.of(ConfigRewrite.jobRewrite(new JobConfigRewrite(
            job.newBuilder(),
            job.newBuilder()
                .setTaskConfig(job.getTaskConfig().newBuilder().setJob(rewrittenJobKey)
                    .setOwner(rewrittenIdentity))
                .setOwner(rewrittenIdentity)
                .setKey(rewrittenJobKey)))));
    assertResponse(WARNING, thrift.rewriteConfigs(request, SESSION));
  }

  @Test
  public void testRewriteShardCasMismatch() throws Exception {
    TaskConfig storedConfig = productionTask();
    TaskConfig modifiedConfig =
        storedConfig.deepCopy().setExecutorConfig(new ExecutorConfig("aurora", "rewritten"));
    IScheduledTask storedTask = IScheduledTask.build(
        new ScheduledTask().setAssignedTask(new AssignedTask().setTask(storedConfig)));
    InstanceKey instance = new InstanceKey(
        JobKeys.from(
            storedConfig.getOwner().getRole(),
            storedConfig.getEnvironment(),
            storedConfig.getJobName()).newBuilder(),
        0);

    expectAuth(ROOT, true);
    storageUtil.expectTaskFetch(
        Query.instanceScoped(IInstanceKey.build(instance)).active(), storedTask);

    control.replay();

    RewriteConfigsRequest request = new RewriteConfigsRequest(
        ImmutableList.of(ConfigRewrite.instanceRewrite(
            new InstanceConfigRewrite(instance, modifiedConfig, modifiedConfig))));
    assertResponse(WARNING, thrift.rewriteConfigs(request, SESSION));
  }

  @Test
  public void testRewriteInstance() throws Exception {
    TaskConfig storedConfig = productionTask();
    ITaskConfig modifiedConfig = ITaskConfig.build(
        storedConfig.deepCopy().setExecutorConfig(new ExecutorConfig("aurora", "rewritten")));
    String taskId = "task_id";
    IScheduledTask storedTask = IScheduledTask.build(new ScheduledTask().setAssignedTask(
        new AssignedTask()
            .setTaskId(taskId)
            .setTask(storedConfig)));
    InstanceKey instanceKey = new InstanceKey(
        JobKeys.from(
            storedConfig.getOwner().getRole(),
            storedConfig.getEnvironment(),
            storedConfig.getJobName()).newBuilder(),
        0);

    expectAuth(ROOT, true);
    storageUtil.expectTaskFetch(
        Query.instanceScoped(IInstanceKey.build(instanceKey)).active(), storedTask);
    expect(storageUtil.taskStore.unsafeModifyInPlace(
        taskId,
        ITaskConfig.build(ConfigurationManager.applyDefaultsIfUnset(modifiedConfig.newBuilder()))))
        .andReturn(true);

    control.replay();

    RewriteConfigsRequest request = new RewriteConfigsRequest(
        ImmutableList.of(ConfigRewrite.instanceRewrite(
            new InstanceConfigRewrite(instanceKey, storedConfig, modifiedConfig.newBuilder()))));
    assertOkResponse(thrift.rewriteConfigs(request, SESSION));
  }

  @Test
  public void testRewriteInstanceUnchanged() throws Exception {
    TaskConfig config = productionTask();
    String taskId = "task_id";
    IScheduledTask task = IScheduledTask.build(new ScheduledTask().setAssignedTask(
        new AssignedTask()
            .setTaskId(taskId)
            .setTask(config)));
    InstanceKey instanceKey = new InstanceKey(
        JobKeys.from(
            config.getOwner().getRole(),
            config.getEnvironment(),
            config.getJobName()).newBuilder(),
        0);

    expectAuth(ROOT, true);
    storageUtil.expectTaskFetch(
        Query.instanceScoped(IInstanceKey.build(instanceKey)).active(), task);
    expect(storageUtil.taskStore.unsafeModifyInPlace(
        taskId,
        ITaskConfig.build(ConfigurationManager.applyDefaultsIfUnset(config.deepCopy()))))
        .andReturn(false);

    control.replay();

    RewriteConfigsRequest request = new RewriteConfigsRequest(
        ImmutableList.of(ConfigRewrite.instanceRewrite(
            new InstanceConfigRewrite(instanceKey, config, config))));
    assertResponse(WARNING, thrift.rewriteConfigs(request, SESSION));
  }

  @Test
  public void testRewriteJobCasMismatch() throws Exception {
    JobConfiguration oldJob = makeJob(productionTask());
    JobConfiguration newJob = oldJob.deepCopy();
    newJob.getTaskConfig().setExecutorConfig(new ExecutorConfig("aurora", "rewritten"));
    String manager = "manager_key";
    expectAuth(ROOT, true);
    expect(storageUtil.jobStore.fetchManagerIds()).andReturn(ImmutableSet.of(manager));
    expect(storageUtil.jobStore.fetchJob(manager, IJobKey.build(oldJob.getKey())))
        .andReturn(Optional.of(IJobConfiguration.build(oldJob)));

    control.replay();

    RewriteConfigsRequest request = new RewriteConfigsRequest(
        ImmutableList.of(ConfigRewrite.jobRewrite(
            new JobConfigRewrite(newJob, newJob))));
    assertResponse(WARNING, thrift.rewriteConfigs(request, SESSION));
  }

  @Test
  public void testRewriteJobNotFound() throws Exception {
    JobConfiguration oldJob = makeJob(productionTask());
    JobConfiguration newJob = oldJob.deepCopy();
    newJob.getTaskConfig().setExecutorConfig(new ExecutorConfig("aurora", "rewritten"));
    String manager = "manager_key";
    expectAuth(ROOT, true);
    expect(storageUtil.jobStore.fetchManagerIds()).andReturn(ImmutableSet.of(manager));
    expect(storageUtil.jobStore.fetchJob(manager, IJobKey.build(oldJob.getKey())))
        .andReturn(Optional.<IJobConfiguration>absent());

    control.replay();

    RewriteConfigsRequest request = new RewriteConfigsRequest(
        ImmutableList.of(ConfigRewrite.jobRewrite(
            new JobConfigRewrite(oldJob, newJob))));
    assertResponse(WARNING, thrift.rewriteConfigs(request, SESSION));
  }

  @Test
  public void testRewriteJobMultipleMatches() throws Exception {
    JobConfiguration oldJob = makeJob(productionTask());
    JobConfiguration newJob = oldJob.deepCopy();
    newJob.getTaskConfig().setExecutorConfig(new ExecutorConfig("aurora", "rewritten"));
    String manager1 = "manager1";
    String manager2 = "manager2";
    String manager3 = "manager3";
    expectAuth(ROOT, true);
    expect(storageUtil.jobStore.fetchManagerIds())
        .andReturn(ImmutableSet.of(manager1, manager2, manager3));
    expect(storageUtil.jobStore.fetchJob(manager1, IJobKey.build(oldJob.getKey())))
        .andReturn(Optional.of(IJobConfiguration.build(oldJob)));
    expect(storageUtil.jobStore.fetchJob(manager2, IJobKey.build(oldJob.getKey())))
        .andReturn(Optional.of(IJobConfiguration.build(makeJob())));
    expect(storageUtil.jobStore.fetchJob(manager3, IJobKey.build(oldJob.getKey())))
        .andReturn(Optional.of(IJobConfiguration.build(
            makeJob().setKey(JobKeys.from("1", "2", "3").newBuilder()))));

    control.replay();

    RewriteConfigsRequest request = new RewriteConfigsRequest(
        ImmutableList.of(ConfigRewrite.jobRewrite(
            new JobConfigRewrite(oldJob, newJob))));
    assertResponse(WARNING, thrift.rewriteConfigs(request, SESSION));
  }

  @Test
  public void testRewriteJob() throws Exception {
    JobConfiguration oldJob = makeJob(productionTask());
    JobConfiguration newJob = oldJob.deepCopy();
    newJob.getTaskConfig().setExecutorConfig(new ExecutorConfig("aurora", "rewritten"));
    String manager = "manager_key";
    expectAuth(ROOT, true);
    expect(storageUtil.jobStore.fetchManagerIds()).andReturn(ImmutableSet.of(manager));
    expect(storageUtil.jobStore.fetchJob(manager, IJobKey.build(oldJob.getKey())))
        .andReturn(Optional.of(IJobConfiguration.build(oldJob)));
    storageUtil.jobStore.saveAcceptedJob(
        manager,
        ConfigurationManager.validateAndPopulate(IJobConfiguration.build(newJob)));

    control.replay();

    // Validate key is populated during sanitizing.
    JobConfiguration requestConfig = oldJob.deepCopy();
    requestConfig.getTaskConfig().unsetJob();

    RewriteConfigsRequest request = new RewriteConfigsRequest(
        ImmutableList.of(ConfigRewrite.jobRewrite(
            new JobConfigRewrite(oldJob, newJob))));
    assertOkResponse(thrift.rewriteConfigs(request, SESSION));
  }

  @Test
  public void testUnauthorizedDedicatedJob() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    TaskConfig task = nonProductionTask();
    task.addToConstraints(dedicatedConstraint(ImmutableSet.of("mesos")));
    assertResponse(INVALID_REQUEST, thrift.createJob(makeJob(task), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testHostMaintenance() throws Exception {
    expectAuth(ROOT, true).times(6);
    Set<String> hostnames = ImmutableSet.of("a");
    Set<HostStatus> none = ImmutableSet.of(new HostStatus("a", NONE));
    Set<HostStatus> scheduled = ImmutableSet.of(new HostStatus("a", SCHEDULED));
    Set<HostStatus> draining = ImmutableSet.of(new HostStatus("a", DRAINING));
    Set<HostStatus> drained = ImmutableSet.of(new HostStatus("a", DRAINING));
    expect(maintenance.getStatus(hostnames)).andReturn(none);
    expect(maintenance.startMaintenance(hostnames)).andReturn(scheduled);
    expect(maintenance.drain(hostnames)).andReturn(draining);
    expect(maintenance.getStatus(hostnames)).andReturn(draining);
    expect(maintenance.getStatus(hostnames)).andReturn(drained);
    expect(maintenance.endMaintenance(hostnames)).andReturn(none);

    control.replay();

    Hosts hosts = new Hosts(hostnames);

    assertEquals(
        none,
        thrift.maintenanceStatus(hosts, SESSION).getResult().getMaintenanceStatusResult()
            .getStatuses());
    assertEquals(
        scheduled,
        thrift.startMaintenance(hosts, SESSION).getResult().getStartMaintenanceResult()
            .getStatuses());
    assertEquals(
        draining,
        thrift.drainHosts(hosts, SESSION).getResult().getDrainHostsResult().getStatuses());
    assertEquals(
        draining,
        thrift.maintenanceStatus(hosts, SESSION).getResult().getMaintenanceStatusResult()
            .getStatuses());
    assertEquals(
        drained,
        thrift.maintenanceStatus(hosts, SESSION).getResult().getMaintenanceStatusResult()
            .getStatuses());
    assertEquals(
        none,
        thrift.endMaintenance(hosts, SESSION).getResult().getEndMaintenanceResult().getStatuses());
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
    Query.Builder query = Query.roleScoped(ROLE);

    Set<JobSummary> ownedImmedieteJobSummaryOnly = ImmutableSet.of(
        new JobSummary().setJob(ownedImmediateJob).setStats(new JobStats().setActiveTaskCount(1)));

    expect(cronPredictor.predictNextRun(CrontabEntry.parse(CRON_SCHEDULE)))
        .andReturn(new Date(nextCronRunMs))
        .anyTimes();

    expect(cronJobManager.getJobs()).andReturn(IJobConfiguration.setFromBuilders(ownedCronJobOnly));
    storageUtil.expectTaskFetch(query);

    expect(cronJobManager.getJobs()).andReturn(IJobConfiguration.setFromBuilders(bothCronJobs));
    storageUtil.expectTaskFetch(query);

    expect(cronJobManager.getJobs())
        .andReturn(IJobConfiguration.setFromBuilders(unownedCronJobOnly));
    storageUtil.expectTaskFetch(query, ownedImmediateTask);

    expect(cronJobManager.getJobs()).andReturn(ImmutableSet.<IJobConfiguration>of());
    storageUtil.expectTaskFetch(query);

    // Handle the case where a cron job has a running task (same JobKey present in both stores).
    expect(cronJobManager.getJobs())
        .andReturn(ImmutableList.of(IJobConfiguration.build(ownedCronJob)));
    storageUtil.expectTaskFetch(query, ownedCronJobScheduledTask);

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

    assertEquals(jobSummaryResponse(ImmutableSet.<JobSummary>of()), thrift.getJobSummary(ROLE));

    assertEquals(jobSummaryResponse(ownedCronJobSummaryWithRunningTask),
        thrift.getJobSummary(ROLE));
  }

  private Response jobSummaryResponse(Set<JobSummary> jobSummaries) {
    return okResponse(Result.jobSummaryResult(new JobSummaryResult().setSummaries(jobSummaries)));
  }

  private static final Function<String, ResponseDetail> MESSAGE_TO_DETAIL =
      new Function<String, ResponseDetail>() {
        @Override
        public ResponseDetail apply(String message) {
          return new ResponseDetail().setMessage(message);
        }
      };

  private Response response(ResponseCode code, Optional<Result> result, String... messages) {
    Response response = Util.emptyResponse()
        .setResponseCode(code)
        .setServerInfo(SERVER_INFO)
        .setResult(result.orNull());
    if (messages.length > 0) {
      response.setDetails(FluentIterable.from(Arrays.asList(messages)).transform(MESSAGE_TO_DETAIL)
          .toList());
    }

    return response;
  }

  private Response okResponse(Result result) {
    return response(OK, Optional.of(result));
  }

  private Response okEmptyResponse() {
    return response(OK, Optional.<Result>absent());
  }

  private Response errorResponse(String message) {
    return response(ERROR, Optional.<Result>absent())
        .setDetails(ImmutableList.of(new ResponseDetail().setMessage(message)));
  }

  private Response invalidResponse(String message) {
    return Util.emptyResponse()
        .setResponseCode(INVALID_REQUEST)
        .setServerInfo(SERVER_INFO)
        .setDetails(ImmutableList.of(new ResponseDetail(message)));
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

    expect(cronJobManager.getJobs()).andReturn(IJobConfiguration.setFromBuilders(ownedCronJobOnly));
    storageUtil.expectTaskFetch(query);

    expect(cronJobManager.getJobs()).andReturn(IJobConfiguration.setFromBuilders(bothCronJobs));
    storageUtil.expectTaskFetch(query);

    expect(cronJobManager.getJobs())
        .andReturn(IJobConfiguration.setFromBuilders(unownedCronJobOnly));
    storageUtil.expectTaskFetch(query, ownedImmediateTask);

    expect(cronJobManager.getJobs()).andReturn(ImmutableSet.<IJobConfiguration>of());
    storageUtil.expectTaskFetch(query);

    // Handle the case where a cron job has a running task (same JobKey present in both stores).
    expect(cronJobManager.getJobs())
        .andReturn(ImmutableList.of(IJobConfiguration.build(ownedCronJob)));
    storageUtil.expectTaskFetch(query, ownedCronJobScheduledTask);

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
    expect(cronJobManager.getJobs()).andReturn(IJobConfiguration.setFromBuilders(crons));
    storageUtil.expectTaskFetch(Query.unscoped().active(), immediateTask);

    control.replay();

    Set<JobConfiguration> allJobs =
        ImmutableSet.<JobConfiguration>builder().addAll(crons).add(immediateJob).build();
    assertEquals(allJobs, thrift.getJobs(null).getResult().getGetJobsResult().getConfigs());
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
  public void testGetPendingReason() throws Exception {
    Builder query = Query.unscoped().byJob(JOB_KEY);
    Builder filterQuery = Query.unscoped().byJob(JOB_KEY).byStatus(ScheduleStatus.PENDING);
    String taskId = "task_id_test";
    ImmutableSet<Veto> result = ImmutableSet.of(
        Veto.constraintMismatch("first"),
        Veto.constraintMismatch("second"));

    IScheduledTask pendingTask = IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setTask(defaultTask(true))
            .setTaskId(taskId))
        .setStatus(ScheduleStatus.PENDING));

    storageUtil.expectTaskFetch(filterQuery, pendingTask);
    expect(nearestFit.getNearestFit(taskId)).andReturn(result);

    control.replay();

    Set<PendingReason> expected = ImmutableSet.of(new PendingReason()
        .setTaskId(taskId)
        .setReason("Constraint not satisfied: first,Constraint not satisfied: second"));

    Response response = assertOkResponse(thrift.getPendingReason(query.get()));
    assertEquals(expected, response.getResult().getGetPendingReasonResult().getReasons());
  }

  @Test
  public void testGetPendingReasonFailsStatusSet() throws Exception {
    Builder query = Query.unscoped().byStatus(ScheduleStatus.ASSIGNED);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.getPendingReason(query.get()));
  }

  @Test
  public void testGetPendingReasonFailsSlavesSet() throws Exception {
    Builder query = Query.unscoped().bySlave("host1");

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.getPendingReason(query.get()));
  }

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
  public void testEmptyConfigSummary() throws Exception {
    IJobKey key = JobKeys.from("test", "test", "test");

    storageUtil.expectTaskFetch(Query.jobScoped(key).active(), ImmutableSet.<IScheduledTask>of());

    ConfigSummary summary = new ConfigSummary()
        .setKey(key.newBuilder())
        .setGroups(Sets.<ConfigGroup>newHashSet());

    ConfigSummaryResult expected = new ConfigSummaryResult().setSummary(summary);

    control.replay();

    Response response = assertOkResponse(thrift.getConfigSummary(key.newBuilder()));
    assertEquals(expected, response.getResult().getConfigSummaryResult());
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

    storageUtil.expectTaskFetch(Query.unscoped(), task1, task2, task3, task4);

    expect(cronJobManager.getJobs()).andReturn(IJobConfiguration.setFromBuilders(crons));

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
  public void testSnapshot() throws Exception {
    expectAuth(ROOT, false);

    expectAuth(ROOT, true);
    storageUtil.storage.snapshot();
    expectLastCall();

    expectAuth(ROOT, true);
    storageUtil.storage.snapshot();
    expectLastCall().andThrow(new Storage.StorageException("mock error!"));

    control.replay();

    assertResponse(AUTH_FAILED, thrift.snapshot(SESSION));
    assertOkResponse(thrift.snapshot(SESSION));
    assertResponse(ERROR, thrift.snapshot(SESSION));
  }

  private static AddInstancesConfig createInstanceConfig(TaskConfig task) {
    return new AddInstancesConfig()
        .setTaskConfig(task)
        .setInstanceIds(ImmutableSet.of(0))
        .setKey(JOB_KEY.newBuilder());
  }

  @Test
  public void testAddInstances() throws Exception {
    ITaskConfig populatedTask = ITaskConfig.build(populatedTask());
    expectAuth(ROLE, true);
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(false);
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(LOCK));
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expect(taskIdGenerator.generate(populatedTask, 1))
        .andReturn(TASK_ID);
    expect(quotaManager.checkInstanceAddition(populatedTask, 1)).andReturn(ENOUGH_QUOTA);
    stateManager.insertPendingTasks(
        storageUtil.mutableStoreProvider,
        populatedTask,
        ImmutableSet.of(0));

    control.replay();

    // Validate key is populated during sanitizing.
    AddInstancesConfig config = createInstanceConfig(populatedTask.newBuilder());
    config.getTaskConfig().unsetJob();
    assertOkResponse(thrift.addInstances(config, LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testAddInstancesWithNullLock() throws Exception {
    ITaskConfig populatedTask = ITaskConfig.build(populatedTask());
    AddInstancesConfig config = createInstanceConfig(populatedTask.newBuilder());
    expectAuth(ROLE, true);
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(false);
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expect(taskIdGenerator.generate(populatedTask, 1))
        .andReturn(TASK_ID);
    expect(quotaManager.checkInstanceAddition(populatedTask, 1)).andReturn(ENOUGH_QUOTA);
    stateManager.insertPendingTasks(
        storageUtil.mutableStoreProvider,
        populatedTask,
        ImmutableSet.of(0));

    control.replay();

    assertOkResponse(thrift.addInstances(config, DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testAddInstancesFailsAuth() throws Exception {
    AddInstancesConfig config = createInstanceConfig(defaultTask(true));
    expectAuth(ROLE, false);

    control.replay();

    assertResponse(AUTH_FAILED, thrift.addInstances(config, LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testAddInstancesFailsConfigCheck() throws Exception {
    AddInstancesConfig config = createInstanceConfig(defaultTask(true).setJobName(null));
    expectAuth(ROLE, true);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.addInstances(config, LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testAddInstancesFailsCronJob() throws Exception {
    AddInstancesConfig config = createInstanceConfig(defaultTask(true));
    expectAuth(ROLE, true);
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(true);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.addInstances(config, LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testAddInstancesFailsWithTransient() throws Exception {
    AddInstancesConfig config = createInstanceConfig(defaultTask(true));
    expectAuth(ROLE, true);
    expect(cronJobManager.hasJob(JOB_KEY)).andThrow(new Storage.TransientStorageException("retry"));

    control.replay();

    assertResponse(ERROR_TRANSIENT, thrift.addInstances(config, LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testAddInstancesFailsWithNonTransient() throws Exception {
    AddInstancesConfig config = createInstanceConfig(defaultTask(true));
    expectAuth(ROLE, true);
    expect(cronJobManager.hasJob(JOB_KEY)).andThrow(new Storage.StorageException("no retry"));

    control.replay();

    assertResponse(ERROR, thrift.addInstances(config, LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testAddInstancesLockCheckFails() throws Exception {
    AddInstancesConfig config = createInstanceConfig(defaultTask(true));
    expectAuth(ROLE, true);
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(false);
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(LOCK));
    expectLastCall().andThrow(new LockException("Failed lock check."));

    control.replay();

    assertResponse(LOCK_ERROR, thrift.addInstances(config, LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testAddInstancesFailsTaskIdLength() throws Exception {
    ITaskConfig populatedTask = ITaskConfig.build(populatedTask());
    AddInstancesConfig config = createInstanceConfig(populatedTask.newBuilder());
    expectAuth(ROLE, true);
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(false);
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(LOCK));
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expect(quotaManager.checkInstanceAddition(anyObject(ITaskConfig.class), anyInt()))
        .andReturn(ENOUGH_QUOTA);
    expect(taskIdGenerator.generate(populatedTask, 1))
        .andReturn(Strings.repeat("a", MAX_TASK_ID_LENGTH + 1));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.addInstances(config, LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testAddInstancesFailsQuotaCheck() throws Exception {
    ITaskConfig populatedTask = ITaskConfig.build(populatedTask());
    AddInstancesConfig config = createInstanceConfig(populatedTask.newBuilder());
    expectAuth(ROLE, true);
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(false);
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(LOCK));
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expect(taskIdGenerator.generate(populatedTask, 1))
        .andReturn(TASK_ID);
    expect(quotaManager.checkInstanceAddition(populatedTask, 1)).andReturn(NOT_ENOUGH_QUOTA);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.addInstances(config, LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testAddInstancesInstanceCollisionFailure() throws Exception {
    ITaskConfig populatedTask = ITaskConfig.build(populatedTask());
    AddInstancesConfig config = createInstanceConfig(populatedTask.newBuilder());
    expectAuth(ROLE, true);
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(false);
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(LOCK));
    storageUtil.expectTaskFetch(Query.jobScoped(JOB_KEY).active());
    expect(taskIdGenerator.generate(populatedTask, 1))
        .andReturn(TASK_ID);
    expect(quotaManager.checkInstanceAddition(populatedTask, 1)).andReturn(ENOUGH_QUOTA);
    stateManager.insertPendingTasks(
        storageUtil.mutableStoreProvider,
        populatedTask,
        ImmutableSet.of(0));
    expectLastCall().andThrow(new IllegalArgumentException("instance collision"));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.addInstances(config, LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testAcquireLock() throws Exception {
    expectAuth(ROLE, true);
    expect(lockManager.acquireLock(LOCK_KEY, USER)).andReturn(LOCK);

    control.replay();

    Response response = thrift.acquireLock(LOCK_KEY.newBuilder(), SESSION);
    assertEquals(LOCK.newBuilder(), response.getResult().getAcquireLockResult().getLock());
  }

  @Test
  public void testAcquireLockInvalidKey() throws Exception {
    control.replay();

    assertResponse(ERROR, thrift.acquireLock(LockKey.job(new JobKey()), SESSION));
  }

  @Test
  public void testAcquireLockAuthFailed() throws Exception {
    expectAuth(ROLE, false);
    control.replay();

    assertResponse(AUTH_FAILED, thrift.acquireLock(LOCK_KEY.newBuilder(), SESSION));
  }

  @Test
  public void testAcquireLockFailed() throws Exception {
    expectAuth(ROLE, true);
    expect(lockManager.acquireLock(LOCK_KEY, USER))
        .andThrow(new LockException("Failed"));

    control.replay();

    assertResponse(LOCK_ERROR, thrift.acquireLock(LOCK_KEY.newBuilder(), SESSION));
  }

  @Test
  public void testReleaseLock() throws Exception {
    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(LOCK));
    lockManager.releaseLock(LOCK);

    control.replay();

    assertOkResponse(thrift.releaseLock(LOCK.newBuilder(), CHECKED, SESSION));
  }

  @Test
  public void testReleaseLockInvalidKey() throws Exception {
    control.replay();

    assertResponse(
        ERROR,
        thrift.releaseLock(new Lock().setKey(LockKey.job(new JobKey())), CHECKED, SESSION));
  }

  @Test
  public void testReleaseLockAuthFailed() throws Exception {
    expectAuth(ROLE, false);
    control.replay();

    assertResponse(AUTH_FAILED, thrift.releaseLock(LOCK.newBuilder(), CHECKED, SESSION));
  }

  @Test
  public void testReleaseLockFailed() throws Exception {
    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(LOCK));
    expectLastCall().andThrow(new LockException("Failed"));

    control.replay();

    assertResponse(LOCK_ERROR, thrift.releaseLock(LOCK.newBuilder(), CHECKED, SESSION));
  }

  @Test
  public void testReleaseLockUnchecked() throws Exception {
    expectAuth(ROLE, true);
    lockManager.releaseLock(LOCK);

    control.replay();

    assertEquals(okEmptyResponse(), thrift.releaseLock(LOCK.newBuilder(), UNCHECKED, SESSION));
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
    expect(quotaManager.getQuotaInfo(ROLE)).andReturn(infoMock);
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
    String id = "id";
    JobUpdateDetails details = createJobUpdateDetails();
    expect(storageUtil.jobUpdateStore.fetchJobUpdateDetails(id))
        .andReturn(Optional.of(IJobUpdateDetails.build(details)));

    control.replay();

    Response response = assertOkResponse(thrift.getJobUpdateDetails(id));
    assertEquals(
        details,
        response.getResult().getGetJobUpdateDetailsResult().getDetails());
  }

  @Test
  public void testGetJobUpdateDetailsInvalidId() throws Exception {
    String id = "id";
    expect(storageUtil.jobUpdateStore.fetchJobUpdateDetails(id))
        .andReturn(Optional.<IJobUpdateDetails>absent());

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.getJobUpdateDetails(id));
  }

  private static List<JobUpdateSummary> createJobUpdateSummaries(int count) {
    ImmutableList.Builder<JobUpdateSummary> builder = ImmutableList.builder();
    for (int i = 0; i < count; i++) {
      builder.add(new JobUpdateSummary()
          .setUpdateId("id" + i)
          .setJobKey(JOB_KEY.newBuilder())
          .setUser(USER));
    }
    return builder.build();
  }

  private static JobUpdateDetails createJobUpdateDetails() {
    return new JobUpdateDetails()
        .setUpdate(new JobUpdate().setSummary(createJobUpdateSummaries(1).get(0)));
  }

  @Test
  public void testStartUpdate() throws Exception {
    expectAuth(ROLE, true);
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(false);

    ITaskConfig newTask = buildTaskForJobUpdate(0).getAssignedTask().getTask();
    expect(taskIdGenerator.generate(newTask, 6)).andReturn(TASK_ID);

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

    expect(quotaManager.checkJobUpdate(update)).andReturn(ENOUGH_QUOTA);
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

    jobUpdateController.start(update, USER);

    control.replay();

    // Validate key is populated during sanitizing.
    JobUpdateRequest request = buildJobUpdateRequest(update);
    request.getTaskConfig().unsetJob();

    Response response = assertOkResponse(thrift.startJobUpdate(request, SESSION));
    assertEquals(UPDATE_ID, response.getResult().getStartJobUpdateResult().getUpdateId());
  }

  @Test
  public void testStartUpdateEmptyDesired() throws Exception {
    expectAuth(ROLE, true);
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(false);

    ITaskConfig newTask = buildTaskForJobUpdate(0).getAssignedTask().getTask();
    expect(taskIdGenerator.generate(newTask, 1)).andReturn(TASK_ID);

    IScheduledTask oldTask1 = buildTaskForJobUpdate(0);
    IScheduledTask oldTask2 = buildTaskForJobUpdate(1);

    // Set instance count to 1 to generate empty desired state in diff.
    IJobUpdate update = buildJobUpdate(
        1,
        newTask,
        ImmutableMap.of(oldTask1.getAssignedTask().getTask(), ImmutableSet.of(new Range(0, 1))));

    expect(quotaManager.checkJobUpdate(anyObject(IJobUpdate.class))).andReturn(ENOUGH_QUOTA);

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

    jobUpdateController.start(IJobUpdate.build(expected), USER);

    control.replay();

    Response response =
        assertOkResponse(thrift.startJobUpdate(buildJobUpdateRequest(update), SESSION));
    assertEquals(UPDATE_ID, response.getResult().getStartJobUpdateResult().getUpdateId());
  }

  @Test
  public void testStartUpdateFailsNullRequest() throws Exception {
    control.replay();
    assertResponse(ERROR, thrift.startJobUpdate(null, SESSION));
  }

  @Test
  public void testStartUpdateFailsNullTaskConfig() throws Exception {
    control.replay();
    assertResponse(ERROR, thrift.startJobUpdate(new JobUpdateRequest(
        null,
        5,
        buildJobUpdateSettings()), SESSION));
  }

  @Test
  public void testStartUpdateFailsInvalidJobKey() throws Exception {
    control.replay();
    assertResponse(ERROR, thrift.startJobUpdate(new JobUpdateRequest(
        new TaskConfig()
            .setJobName("&")
            .setEnvironment("devel")
            .setOwner(new Identity(ROLE, null)),
        5,
        buildJobUpdateSettings()), SESSION));
  }

  @Test
  public void testStartUpdateFailsInvalidGroupSize() throws Exception {
    control.replay();
    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(new JobUpdateRequest(
        defaultTask(true),
        5,
        buildJobUpdateSettings().setUpdateGroupSize(0)), SESSION));
  }

  @Test
  public void testStartUpdateFailsInvalidMaxInstanceFailures() throws Exception {
    control.replay();
    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(new JobUpdateRequest(
        defaultTask(true),
        5,
        buildJobUpdateSettings().setMaxPerInstanceFailures(-1)), SESSION));
  }

  @Test
  public void testStartUpdateFailsInvalidMaxFailedInstances() throws Exception {
    control.replay();
    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(new JobUpdateRequest(
        defaultTask(true),
        5,
        buildJobUpdateSettings().setMaxFailedInstances(-1)), SESSION));
  }

  @Test
  public void testStartUpdateFailsInvalidMaxWaitToRunning() throws Exception {
    control.replay();
    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(new JobUpdateRequest(
        defaultTask(true),
        5,
        buildJobUpdateSettings().setMaxWaitToInstanceRunningMs(-1)), SESSION));
  }

  @Test
  public void testStartUpdateFailsInvalidMinWaitInRunning() throws Exception {
    control.replay();
    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(new JobUpdateRequest(
        defaultTask(true),
        5,
        buildJobUpdateSettings().setMinWaitInInstanceRunningMs(-1)), SESSION));
  }

  @Test
  public void testStartUpdateFailsNonServiceTask() throws Exception {
    control.replay();
    JobUpdateRequest request = buildJobUpdateRequest(populatedTask().setIsService(false));
    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(request, SESSION));
  }

  @Test
  public void testStartUpdateFailsAuth() throws Exception {
    JobUpdateRequest request = buildJobUpdateRequest(populatedTask().setIsService(true));
    expectAuth(ROLE, false);

    control.replay();
    assertResponse(AUTH_FAILED, thrift.startJobUpdate(request, SESSION));
  }

  @Test
  public void testStartUpdateFailsForCronJob() throws Exception {
    JobUpdateRequest request = buildJobUpdateRequest(populatedTask().setIsService(true));
    expectAuth(ROLE, true);
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(true);

    control.replay();
    assertEquals(invalidResponse(NO_CRON), thrift.startJobUpdate(request, SESSION));
  }

  @Test
  public void testStartUpdateFailsConfigValidation() throws Exception {
    JobUpdateRequest request =
        buildJobUpdateRequest(populatedTask().setIsService(true).setNumCpus(-1));
    expectAuth(ROLE, true);

    control.replay();
    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(request, SESSION));
  }

  @Test
  public void testStartNoopUpdate() throws Exception {
    expectAuth(ROLE, true);
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(false);
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
    Response response = thrift.startJobUpdate(request, SESSION);
    assertResponse(OK, response);
    assertEquals(
        NOOP_JOB_UPDATE_MESSAGE,
        Iterables.getOnlyElement(response.getDetails()).getMessage());
  }

  @Test
  public void testStartUpdateInvalidScope() throws Exception {
    expectAuth(ROLE, true);
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(false);
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
    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(request, SESSION));
  }

  @Test
  public void testStartUpdateFailsInstanceCountCheck() throws Exception {
    JobUpdateRequest request = buildJobUpdateRequest(populatedTask().setIsService(true));
    request.setInstanceCount(4001);
    expectAuth(ROLE, true);
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(false);
    expect(uuidGenerator.createNew()).andReturn(UU_ID);
    storageUtil.expectTaskFetch(Query.unscoped().byJob(JOB_KEY).active());
    expect(quotaManager.checkJobUpdate(anyObject(IJobUpdate.class))).andReturn(ENOUGH_QUOTA);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(request, SESSION));
  }

  @Test
  public void testStartUpdateFailsTaskIdLength() throws Exception {
    JobUpdateRequest request = buildJobUpdateRequest(populatedTask().setIsService(true));
    expectAuth(ROLE, true);
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(false);
    expect(uuidGenerator.createNew()).andReturn(UU_ID);
    storageUtil.expectTaskFetch(Query.unscoped().byJob(JOB_KEY).active());
    expect(quotaManager.checkJobUpdate(anyObject(IJobUpdate.class))).andReturn(ENOUGH_QUOTA);
    expect(taskIdGenerator.generate(ITaskConfig.build(request.getTaskConfig()), 6))
        .andReturn(Strings.repeat("a", MAX_TASK_ID_LENGTH + 1));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(request, SESSION));
  }

  @Test
  public void testStartUpdateFailsQuotaCheck() throws Exception {
    JobUpdateRequest request = buildJobUpdateRequest(populatedTask().setIsService(true));
    expectAuth(ROLE, true);
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(false);
    expect(uuidGenerator.createNew()).andReturn(UU_ID);

    IScheduledTask oldTask = buildTaskForJobUpdate(0);
    storageUtil.expectTaskFetch(Query.unscoped().byJob(JOB_KEY).active(), oldTask);
    expect(taskIdGenerator.generate(ITaskConfig.build(request.getTaskConfig()), 6))
        .andReturn(TASK_ID);

    ITaskConfig config = ITaskConfig.build(request.getTaskConfig());
    IJobUpdate update = buildJobUpdate(6, config, ImmutableMap.of(
        oldTask.getAssignedTask().getTask(), ImmutableSet.of(new Range(0, 0))));

    expect(quotaManager.checkJobUpdate(update)).andReturn(NOT_ENOUGH_QUOTA);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(request, SESSION));
  }

  @Test
  public void testStartUpdateFailsInController() throws Exception {
    expectAuth(ROLE, true);
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(false);

    IScheduledTask oldTask = buildTaskForJobUpdate(0, "old");
    ITaskConfig newTask = buildTaskForJobUpdate(0, "new").getAssignedTask().getTask();

    IJobUpdate update = buildJobUpdate(
        1,
        newTask,
        ImmutableMap.of(oldTask.getAssignedTask().getTask(), ImmutableSet.of(new Range(0, 0))));

    expect(uuidGenerator.createNew()).andReturn(UU_ID);
    expect(taskIdGenerator.generate(newTask, 1)).andReturn(TASK_ID);
    expect(quotaManager.checkJobUpdate(update)).andReturn(ENOUGH_QUOTA);
    storageUtil.expectTaskFetch(Query.unscoped().byJob(JOB_KEY).active(), oldTask);
    jobUpdateController.start(update, USER);
    expectLastCall().andThrow(new UpdateStateException("failed"));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.startJobUpdate(buildJobUpdateRequest(update), SESSION));
  }

  @Test
  public void testStartUpdateFailsInProd() throws Exception {
    Module testModule = new AbstractModule() {
      @Override
      protected void configure() {
        bind(NonVolatileStorage.class).toInstance(storageUtil.storage);
        bind(LockManager.class).toInstance(createMock(LockManager.class));
        bind(CapabilityValidator.class).toInstance(createMock(CapabilityValidator.class));
        bind(StorageBackup.class).toInstance(createMock(StorageBackup.class));
        bind(Recovery.class).toInstance(createMock(Recovery.class));
        bind(MaintenanceController.class).toInstance(createMock(MaintenanceController.class));
        bind(CronJobManager.class).toInstance(createMock(CronJobManager.class));
        bind(QuotaManager.class).toInstance(createMock(QuotaManager.class));
        bind(AuroraAdmin.Iface.class).to(SchedulerThriftInterface.class);
        bind(IServerInfo.class).toInstance(IServerInfo.build(SERVER_INFO));
        bind(CronPredictor.class).toInstance(createMock(CronPredictor.class));
        bind(NearestFit.class).toInstance(createMock(NearestFit.class));
        bind(StateManager.class).toInstance(createMock(StateManager.class));
        bind(TaskIdGenerator.class).toInstance(createMock(TaskIdGenerator.class));
        bind(UUIDGenerator.class).toInstance(createMock(UUIDGenerator.class));
        bind(JobUpdateController.class).toInstance(createMock(JobUpdateController.class));
        bind(Boolean.class)
            .annotatedWith(EnableUpdater.class)
            .toInstance(ThriftModule.ENABLE_BETA_UPDATER.get());
      }
    };
    Injector injector = Guice.createInjector(testModule, new AopModule());
    AuroraAdmin.Iface thriftInstance =
        getResponseProxy(injector.getInstance(AuroraAdmin.Iface.class));

    control.replay();
    assertResponse(INVALID_REQUEST, thriftInstance.startJobUpdate(null, null));
  }

  @Test
  public void testPauseJobUpdate() throws Exception {
    expectAuth(ROLE, true);
    jobUpdateController.pause(JOB_KEY, USER);

    control.replay();

    assertResponse(OK, thrift.pauseJobUpdate(JOB_KEY.newBuilder(), SESSION));
  }

  @Test
  public void testPauseJobUpdateFailsAuth() throws Exception {
    expectAuth(ROLE, false);

    control.replay();

    assertResponse(AUTH_FAILED, thrift.pauseJobUpdate(JOB_KEY.newBuilder(), SESSION));
  }

  @Test
  public void testPauseJobUpdateFailsInController() throws Exception {
    expectAuth(ROLE, true);
    jobUpdateController.pause(JOB_KEY, USER);
    expectLastCall().andThrow(new UpdateStateException("failed"));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.pauseJobUpdate(JOB_KEY.newBuilder(), SESSION));
  }

  @Test
  public void testResumeJobUpdate() throws Exception {
    expectAuth(ROLE, true);
    jobUpdateController.resume(JOB_KEY, USER);

    control.replay();

    assertResponse(OK, thrift.resumeJobUpdate(JOB_KEY.newBuilder(), SESSION));
  }

  @Test
  public void testResumeJobUpdateFailsAuth() throws Exception {
    expectAuth(ROLE, false);

    control.replay();

    assertResponse(AUTH_FAILED, thrift.resumeJobUpdate(JOB_KEY.newBuilder(), SESSION));
  }

  @Test
  public void testResumeJobUpdateFailsInController() throws Exception {
    expectAuth(ROLE, true);
    jobUpdateController.resume(JOB_KEY, USER);
    expectLastCall().andThrow(new UpdateStateException("failed"));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.resumeJobUpdate(JOB_KEY.newBuilder(), SESSION));
  }

  @Test
  public void testAbortJobUpdate() throws Exception {
    expectAuth(ROLE, true);
    jobUpdateController.abort(JOB_KEY, USER);

    control.replay();

    assertResponse(OK, thrift.abortJobUpdate(JOB_KEY.newBuilder(), SESSION));
  }

  @Test
  public void testAbortJobUpdateFailsAuth() throws Exception {
    expectAuth(ROLE, false);

    control.replay();

    assertResponse(AUTH_FAILED, thrift.abortJobUpdate(JOB_KEY.newBuilder(), SESSION));
  }

  @Test
  public void testAbortJobUpdateFailsInController() throws Exception {
    expectAuth(ROLE, true);
    jobUpdateController.abort(JOB_KEY, USER);
    expectLastCall().andThrow(new UpdateStateException("failed"));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.abortJobUpdate(JOB_KEY.newBuilder(), SESSION));
  }

  @Test
  public void testPulseJobUpdate() throws Exception {
    control.replay();

    assertEquals(
        errorResponse(SchedulerThriftInterface.NOT_IMPLEMENTED_MESSAGE),
        thrift.pulseJobUpdate("update", SESSION));
  }

  private static JobConfiguration makeProdJob() {
    return makeJob(productionTask(), 1);
  }

  private static JobConfiguration makeJob() {
    return makeJob(nonProductionTask(), 1);
  }

  private static JobConfiguration makeJob(TaskConfig task) {
    return makeJob(task, 1);
  }

  private static Iterable<IScheduledTask> makeDefaultScheduledTasks(int n) {
    return makeDefaultScheduledTasks(n, defaultTask(true));
  }

  private static Iterable<IScheduledTask> makeDefaultScheduledTasks(int n, TaskConfig config) {
    List<IScheduledTask> tasks = Lists.newArrayList();
    for (int i = 0; i < n; i++) {
      tasks.add(IScheduledTask.build(new ScheduledTask()
          .setAssignedTask(new AssignedTask().setTask(config).setInstanceId(i))));
    }

    return tasks;
  }

  private static JobConfiguration makeJob(TaskConfig task, int shardCount) {
    return new JobConfiguration()
        .setOwner(ROLE_IDENTITY)
        .setInstanceCount(shardCount)
        .setTaskConfig(task)
        .setKey(JOB_KEY.newBuilder());
  }

  private static final String AUTH_DENIED_MESSAGE = "Denied!";

  private IExpectationSetters<?> expectAuth(Set<String> roles, boolean allowed)
      throws AuthFailedException {

    if (!allowed) {
      return expect(userValidator.checkAuthenticated(SESSION, roles))
          .andThrow(new AuthFailedException(AUTH_DENIED_MESSAGE));
    } else {
      return expect(userValidator.checkAuthenticated(SESSION, roles))
          .andReturn(context);
    }
  }

  private IExpectationSetters<?> expectAuth(String role, boolean allowed)
      throws AuthFailedException {

    return expectAuth(ImmutableSet.of(role), allowed);
  }

  private IExpectationSetters<?> expectAuth(Capability capability, boolean allowed)
      throws AuthFailedException {

    if (!allowed) {
      return expect(userValidator.checkAuthorized(
          eq(SESSION),
          eq(capability),
          anyObject(AuditCheck.class))
      ).andThrow(new AuthFailedException(AUTH_DENIED_MESSAGE));
    } else {
      return expect(userValidator.checkAuthorized(
          eq(SESSION),
          eq(capability),
          anyObject(AuditCheck.class))).andReturn(context);
    }
  }

  private static TaskConfig defaultTask(boolean production) {
    return new TaskConfig()
        .setJob(JOB_KEY.newBuilder())
        .setOwner(new Identity(ROLE, USER))
        .setEnvironment("devel")
        .setJobName(JOB_NAME)
        .setContactEmail("testing@twitter.com")
        .setExecutorConfig(new ExecutorConfig("aurora", "data"))
        .setNumCpus(1)
        .setRamMb(1024)
        .setDiskMb(1024)
        .setProduction(production)
        .setRequestedPorts(ImmutableSet.<String>of())
        .setTaskLinks(ImmutableMap.<String, String>of())
        .setMaxTaskFailures(1)
        .setContainer(Container.mesos(new MesosContainer()));
  }

  private static TaskConfig populatedTask() {
    return defaultTask(true).setConstraints(Sets.newHashSet(
        new Constraint("host", TaskConstraint.limit(new LimitConstraint(1)))));
  }

  private static TaskConfig productionTask() {
    return defaultTask(true);
  }

  private static TaskConfig nonProductionTask() {
    return defaultTask(false);
  }

  private static Constraint dedicatedConstraint(int value) {
    return new Constraint(DEDICATED_ATTRIBUTE, TaskConstraint.limit(new LimitConstraint(value)));
  }

  private static Constraint dedicatedConstraint(Set<String> values) {
    return new Constraint(DEDICATED_ATTRIBUTE,
        TaskConstraint.value(new ValueConstraint(false, values)));
  }

  private static JobUpdateRequest buildJobUpdateRequest(TaskConfig config) {
    return new JobUpdateRequest()
        .setInstanceCount(6)
        .setSettings(buildJobUpdateSettings())
        .setTaskConfig(ConfigurationManager.applyDefaultsIfUnset(config));
  }

  private static JobUpdateSettings buildJobUpdateSettings() {
    return new JobUpdateSettings()
        .setUpdateGroupSize(10)
        .setMaxFailedInstances(2)
        .setMaxPerInstanceFailures(1)
        .setMaxWaitToInstanceRunningMs(30000)
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
        .setTaskConfig(update.getInstructions().getDesiredState().getTask().newBuilder());
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
            .setJobKey(JOB_KEY.newBuilder())
            .setUpdateId(UPDATE_ID)
            .setUser(ROLE_IDENTITY.getUser()))
        .setInstructions(new JobUpdateInstructions()
            .setSettings(buildJobUpdateSettings())
            .setDesiredState(new InstanceTaskConfig()
                .setTask(newConfig.newBuilder())
                .setInstances(ImmutableSet.of(new Range(0, instanceCount - 1))))
            .setInitialState(builder.build())));
  }
}

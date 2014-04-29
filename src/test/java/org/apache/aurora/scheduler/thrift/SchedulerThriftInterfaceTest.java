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
package org.apache.aurora.scheduler.thrift;

import java.util.Date;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.Clock;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.auth.CapabilityValidator;
import org.apache.aurora.auth.CapabilityValidator.AuditCheck;
import org.apache.aurora.auth.CapabilityValidator.Capability;
import org.apache.aurora.auth.SessionValidator.AuthFailedException;
import org.apache.aurora.gen.APIVersion;
import org.apache.aurora.gen.AddInstancesConfig;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.ConfigRewrite;
import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.HostStatus;
import org.apache.aurora.gen.Hosts;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.InstanceConfigRewrite;
import org.apache.aurora.gen.InstanceKey;
import org.apache.aurora.gen.JobConfigRewrite;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobStats;
import org.apache.aurora.gen.JobSummary;
import org.apache.aurora.gen.JobSummaryResult;
import org.apache.aurora.gen.LimitConstraint;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ResponseCode;
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
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.ScheduleException;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.configuration.SanitizedConfiguration;
import org.apache.aurora.scheduler.cron.CronPredictor;
import org.apache.aurora.scheduler.quota.QuotaInfo;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.state.CronJobManager;
import org.apache.aurora.scheduler.state.LockManager;
import org.apache.aurora.scheduler.state.LockManager.LockException;
import org.apache.aurora.scheduler.state.MaintenanceController;
import org.apache.aurora.scheduler.state.SchedulerCore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.NonVolatileStorage;
import org.apache.aurora.scheduler.storage.backup.Recovery;
import org.apache.aurora.scheduler.storage.backup.StorageBackup;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.ILockKey;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.IServerInfo;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.thrift.aop.AopModule;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.auth.CapabilityValidator.Capability.MACHINE_MAINTAINER;
import static org.apache.aurora.auth.CapabilityValidator.Capability.PROVISIONER;
import static org.apache.aurora.auth.CapabilityValidator.Capability.ROOT;
import static org.apache.aurora.auth.SessionValidator.SessionContext;
import static org.apache.aurora.gen.LockValidation.CHECKED;
import static org.apache.aurora.gen.MaintenanceMode.DRAINING;
import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.apache.aurora.gen.MaintenanceMode.SCHEDULED;
import static org.apache.aurora.gen.ResponseCode.AUTH_FAILED;
import static org.apache.aurora.gen.ResponseCode.ERROR;
import static org.apache.aurora.gen.ResponseCode.INVALID_REQUEST;
import static org.apache.aurora.gen.ResponseCode.LOCK_ERROR;
import static org.apache.aurora.gen.ResponseCode.OK;
import static org.apache.aurora.gen.ResponseCode.WARNING;
import static org.apache.aurora.gen.apiConstants.DEFAULT_ENVIRONMENT;
import static org.apache.aurora.gen.apiConstants.THRIFT_API_VERSION;
import static org.apache.aurora.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;
import static org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.transitionMessage;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SchedulerThriftInterfaceTest extends EasyMockTest {

  private static final String ROLE = "bar_role";
  private static final String USER = "foo_user";
  private static final String JOB_NAME = "job_foo";
  private static final Identity ROLE_IDENTITY = new Identity(ROLE, USER);
  private static final SessionKey SESSION = new SessionKey();
  private static final IJobKey JOB_KEY = JobKeys.from(ROLE, DEFAULT_ENVIRONMENT, JOB_NAME);
  private static final ILockKey LOCK_KEY = ILockKey.build(LockKey.job(JOB_KEY.newBuilder()));
  private static final ILock LOCK = ILock.build(new Lock().setKey(LOCK_KEY.newBuilder()));
  private static final JobConfiguration CRON_JOB = makeJob().setCronSchedule("test");
  private static final Lock DEFAULT_LOCK = null;

  private static final IResourceAggregate QUOTA =
      IResourceAggregate.build(new ResourceAggregate(10.0, 1024, 2048));

  private static final IResourceAggregate CONSUMED =
      IResourceAggregate.build(new ResourceAggregate(0.0, 0, 0));
  private static final ServerInfo SERVER_INFO =
      new ServerInfo()
          .setClusterName("test")
          .setThriftAPIVersion(THRIFT_API_VERSION)
          .setStatsUrlPrefix("fake_url");
  private static final APIVersion API_VERSION = new APIVersion().setMajor(THRIFT_API_VERSION);
  private static final String CRON_SCHEDULE = "0 * * * *";

  private StorageTestUtil storageUtil;
  private SchedulerCore scheduler;
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

  @Before
  public void setUp() throws Exception {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    scheduler = createMock(SchedulerCore.class);
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

    // Use guice and install AuthModule to apply AOP-style auth layer.
    Module testModule = new AbstractModule() {
      @Override
      protected void configure() {
        bind(Clock.class).toInstance(new FakeClock());
        bind(NonVolatileStorage.class).toInstance(storageUtil.storage);
        bind(SchedulerCore.class).toInstance(scheduler);
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
      }
    };
    Injector injector = Guice.createInjector(testModule, new AopModule());
    thrift = injector.getInstance(AuroraAdmin.Iface.class);
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
    control.replay();

    assertOkResponse(thrift.populateJobConfig(job.newBuilder()));
  }

  @Test
  public void testCreateJobNoLock() throws Exception {
    IJobConfiguration job = IJobConfiguration.build(makeJob());
    expectAuth(ROLE, true);
    scheduler.createJob(SanitizedConfiguration.fromUnsanitized(job));
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());

    control.replay();

    assertOkResponse(thrift.createJob(job.newBuilder(), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testCreateJobWithLock() throws Exception {
    IJobConfiguration job = IJobConfiguration.build(makeJob());
    expectAuth(ROLE, true);
    scheduler.createJob(SanitizedConfiguration.fromUnsanitized(job));
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(LOCK));

    control.replay();

    assertOkResponse(thrift.createJob(job.newBuilder(), LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testCreateJobWithLockFails() throws Exception {
    IJobConfiguration job = IJobConfiguration.build(makeJob());
    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(LOCK));
    expectLastCall().andThrow(new LockException("Invalid lock"));

    control.replay();

    assertResponse(LOCK_ERROR, thrift.createJob(job.newBuilder(), LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testCreateJobFails() throws Exception {
    IJobConfiguration job = IJobConfiguration.build(makeJob());
    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(LOCK));
    scheduler.createJob(SanitizedConfiguration.fromUnsanitized(job));
    expectLastCall().andThrow(new ScheduleException("fail"));
    control.replay();

    assertResponse(
        INVALID_REQUEST,
        thrift.createJob(job.newBuilder(), LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testCreateJobFailsNoExecutorConfig() throws Exception {
    JobConfiguration job = makeJob();
    job.getTaskConfig().unsetExecutorConfig();
    expectAuth(ROLE, true);
    control.replay();

    Response response = thrift.createJob(job, LOCK.newBuilder(), SESSION);
    assertResponse(INVALID_REQUEST, response);
    assertTrue(response.getMessage().contains("Configuration"));
  }

  @Test
  public void testCreateHomogeneousJobNoShards() throws Exception {
    JobConfiguration job = makeJob();
    job.setInstanceCount(0);
    job.unsetInstanceCount();
    expectAuth(ROLE, true);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.createJob(job, DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testCreateHomogeneousJob() throws Exception {
    JobConfiguration job = makeJob();
    job.setInstanceCount(2);
    expectAuth(ROLE, true);
    SanitizedConfiguration sanitized =
        SanitizedConfiguration.fromUnsanitized(IJobConfiguration.build(job));
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    assertEquals(2, sanitized.getTaskConfigs().size());
    scheduler.createJob(sanitized);
    control.replay();

    assertOkResponse(thrift.createJob(job, DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testCreateJobAuthFailure() throws Exception {
    expectAuth(ROLE, false);

    control.replay();

    assertResponse(AUTH_FAILED, thrift.createJob(makeJob(), DEFAULT_LOCK, SESSION));
  }

  private static IScheduledTask buildScheduledTask(String jobName) {
    return IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setTask(new TaskConfig()
                .setOwner(ROLE_IDENTITY)
                .setEnvironment(DEFAULT_ENVIRONMENT)
                .setJobName(jobName))));
  }

  @Test
  public void testKillTasksImmediate() throws Exception {
    Query.Builder query = Query.unscoped().byJob(JOB_KEY).active();
    expectAuth(ROOT, false);
    expectAuth(ROLE, true);
    storageUtil.expectTaskFetch(query, buildScheduledTask(JOB_NAME)).times(2);
    scheduler.killTasks(query, USER);
    storageUtil.expectTaskFetch(query);
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(LOCK));

    control.replay();

    assertOkResponse(thrift.killTasks(query.get(), LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testKillTasksDelayed() throws Exception {
    Query.Builder query = Query.unscoped().byJob(JOB_KEY).active();
    IScheduledTask task = buildScheduledTask(JOB_NAME);
    expectAuth(ROOT, false);
    expectAuth(ROLE, true);
    scheduler.killTasks(query, USER);
    storageUtil.expectTaskFetch(query, task).times(2);
    storageUtil.expectTaskFetch(query);
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());

    control.replay();

    assertOkResponse(thrift.killTasks(query.get(), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testKillTasksLockCheckFailed() throws Exception {
    Query.Builder query = Query.unscoped().byJob(JOB_KEY).active();
    IScheduledTask task2 = buildScheduledTask("job_bar");
    ILockKey key2 = ILockKey.build(LockKey.job(
        JobKeys.from(ROLE, DEFAULT_ENVIRONMENT, "job_bar").newBuilder()));
    expectAuth(ROOT, false);
    expectAuth(ROLE, true);
    storageUtil.expectTaskFetch(query, buildScheduledTask(JOB_NAME), task2);
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(LOCK));
    lockManager.validateIfLocked(key2, Optional.of(LOCK));
    expectLastCall().andThrow(new LockException("Failed lock check."));

    control.replay();

    assertResponse(LOCK_ERROR, thrift.killTasks(query.get(), LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testKillTasksAuthFailure() throws Exception {
    Query.Builder query = Query.unscoped().byJob(JOB_KEY).active();
    expectAuth(ROOT, false);
    expectAuth(ROLE, false);
    storageUtil.expectTaskFetch(query, buildScheduledTask(JOB_NAME));

    control.replay();

    assertResponse(AUTH_FAILED, thrift.killTasks(query.get(), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testAdminKillTasks() throws Exception {
    Query.Builder query = Query.unscoped().byJob(JOB_KEY).active();

    expectAuth(ROOT, true);
    scheduler.killTasks(query, USER);
    storageUtil.expectTaskFetch(query).times(2);

    control.replay();

    assertOkResponse(thrift.killTasks(query.get(), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testKillTasksInvalidJobname() throws Exception {
    TaskQuery query = new TaskQuery()
        .setOwner(ROLE_IDENTITY)
        .setJobName("");

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.killTasks(query, DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testKillNonExistentTasks() throws Exception {
    Query.Builder query = Query.unscoped().byJob(JOB_KEY);

    expectAuth(ROOT, true);

    scheduler.killTasks(query, USER);
    expectLastCall().andThrow(new ScheduleException("No jobs matching query"));
    storageUtil.expectTaskFetch(query);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.killTasks(query.get(), DEFAULT_LOCK, SESSION));
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
    Hosts hosts = new Hosts()
        .setHostNames(ImmutableSet.of("host1"));
    Set<HostStatus> statuses = ImmutableSet.of();

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
    String taskId = "task_id_foo";
    ScheduleStatus status = ScheduleStatus.FAILED;

    scheduler.setTaskStatus(taskId, status, transitionMessage(USER));
    // Expect auth is first called by an interceptor and then by SchedulerThriftInterface to extract
    // the SessionContext.
    // Note: This will change after AOP-style session validation passes in a SessionContext.
    expectAuth(ROOT, true).times(2);

    control.replay();

    assertOkResponse(thrift.forceTaskState(taskId, status, SESSION));
  }

  @Test
  public void testForceTaskStateAuthFailure() throws Exception {
    expectAuth(ROOT, false);

    control.replay();

    assertResponse(AUTH_FAILED, thrift.forceTaskState("task", ScheduleStatus.FAILED, SESSION));
  }

  @Test
  public void testRestartShards() throws Exception {
    Set<Integer> shards = ImmutableSet.of(1, 6);

    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(LOCK));
    scheduler.restartShards(JOB_KEY, shards, USER);

    control.replay();

    assertOkResponse(
        thrift.restartShards(JOB_KEY.newBuilder(), shards, LOCK.newBuilder(), SESSION));
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
  public void testRestartShardsFails() throws Exception {
    Set<Integer> shards = ImmutableSet.of(1, 6);

    String message = "Injected.";
    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    scheduler.restartShards(JOB_KEY, shards, USER);
    expectLastCall().andThrow(new ScheduleException(message));

    control.replay();

    Response resp = thrift.restartShards(JOB_KEY.newBuilder(), shards, DEFAULT_LOCK, SESSION);
    assertResponse(INVALID_REQUEST, resp);
    assertEquals(message, resp.getMessage());
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
        .setEnvironment(DEFAULT_ENVIRONMENT)
        .setJobName(JOB_NAME);
    JobConfiguration job = makeJob(task);

    expectAuth(ROLE, true);

    JobConfiguration sanitized = job.deepCopy();
    sanitized.getTaskConfig()
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
        .setEnvironment(DEFAULT_ENVIRONMENT);

    scheduler.createJob(new SanitizedConfiguration(IJobConfiguration.build(sanitized)));
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());

    control.replay();

    assertOkResponse(thrift.createJob(job, DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testReplaceCronTemplate() throws Exception {
    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    cronJobManager.updateJob(anyObject(SanitizedConfiguration.class));
    control.replay();

    assertOkResponse(thrift.replaceCronTemplate(CRON_JOB, DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testReplaceCronTemplateFailedAuth() throws Exception {
    expectAuth(ROLE, false);

    control.replay();

    assertResponse(AUTH_FAILED, thrift.replaceCronTemplate(CRON_JOB, DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testCreateCronJobFailedLockCheck() throws Exception {
    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(LOCK));
    expectLastCall().andThrow(new LockException("Lock check failed."));
    control.replay();

    assertResponse(LOCK_ERROR, thrift.replaceCronTemplate(CRON_JOB, LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testReplaceCronTemplateDoesNotExist() throws Exception {
    expectAuth(ROLE, true);
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    cronJobManager.updateJob(
        SanitizedConfiguration.fromUnsanitized(IJobConfiguration.build(CRON_JOB)));
    expectLastCall().andThrow(new ScheduleException("Nope"));
    control.replay();

    assertResponse(INVALID_REQUEST, thrift.replaceCronTemplate(CRON_JOB, DEFAULT_LOCK, SESSION));
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
    storageUtil.expectTaskFetch(Query.instanceScoped(instance).active(), storedTask);

    control.replay();

    RewriteConfigsRequest request = new RewriteConfigsRequest(
        ImmutableList.of(ConfigRewrite.instanceRewrite(
            new InstanceConfigRewrite(instance, modifiedConfig, modifiedConfig))));
    assertResponse(WARNING, thrift.rewriteConfigs(request, SESSION));
  }

  @Test
  public void testRewriteShard() throws Exception {
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
    storageUtil.expectTaskFetch(Query.instanceScoped(instanceKey).active(), storedTask);
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
  public void testRewriteJobCasMismatch() throws Exception {
    JobConfiguration oldJob = makeJob(productionTask());
    JobConfiguration newJob = oldJob.deepCopy();
    newJob.getTaskConfig().setExecutorConfig(new ExecutorConfig("aurora", "rewritten"));
    String manager = "manager_key";
    expectAuth(ROOT, true);
    expect(storageUtil.jobStore.fetchManagerIds()).andReturn(ImmutableSet.of(manager));
    expect(storageUtil.jobStore.fetchJobs(manager))
        .andReturn(ImmutableList.of(IJobConfiguration.build(oldJob)));

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
    expect(storageUtil.jobStore.fetchJobs(manager))
        .andReturn(ImmutableList.<IJobConfiguration>of());

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
    String manager = "manager_key";
    expectAuth(ROOT, true);
    expect(storageUtil.jobStore.fetchManagerIds()).andReturn(ImmutableSet.of(manager));
    expect(storageUtil.jobStore.fetchJobs(manager))
        .andReturn(IJobConfiguration.listFromBuilders(ImmutableList.of(oldJob, makeJob())));

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
    expect(storageUtil.jobStore.fetchJobs(manager))
        .andReturn(ImmutableList.of(IJobConfiguration.build(oldJob)));
    storageUtil.jobStore.saveAcceptedJob(
        manager,
        ConfigurationManager.validateAndPopulate(IJobConfiguration.build(newJob)));

    control.replay();

    RewriteConfigsRequest request = new RewriteConfigsRequest(
        ImmutableList.of(ConfigRewrite.jobRewrite(
            new JobConfigRewrite(oldJob, newJob))));
    assertOkResponse(thrift.rewriteConfigs(request, SESSION));
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
    int nextCronRunMs = 100;
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

    expect(cronPredictor.predictNextRun(CRON_SCHEDULE))
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

  private Response okResponse(Result result) {
    return new Response()
        .setResponseCode(OK)
        .setDEPRECATEDversion(API_VERSION)
        .setServerInfo(SERVER_INFO)
        .setResult(result);
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
        .setJobName("immediate")
        .setOwner(ROLE_IDENTITY);
    IScheduledTask task1 = IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(immediateTaskConfig)));
    IScheduledTask task2 = IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(immediateTaskConfig.setNumCpus(2))));

    TaskConfig immediateTaskConfigTwo = defaultTask(false)
        .setJobName("immediateTwo")
        .setOwner(BAZ_ROLE_IDENTITY);
    IScheduledTask task3 = IScheduledTask.build(new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(immediateTaskConfigTwo)));

    TaskConfig immediateTaskConfigThree = defaultTask(false)
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

  @Test
  public void testKillAuthenticatesQueryRole() throws Exception {
    expectAuth(ROOT, false);
    expectAuth(ImmutableSet.of("foo", ROLE), true);

    Query.Builder query = Query.roleScoped("foo");

    storageUtil.expectTaskFetch(query, buildScheduledTask(JOB_NAME));
    scheduler.killTasks(query, USER);
    storageUtil.expectTaskFetch(query.active());
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());

    control.replay();

    assertOkResponse(thrift.killTasks(query.get(), DEFAULT_LOCK, SESSION));
  }

  @Test
  public void testGetVersion() throws Exception {
    control.replay();

    assertOkResponse(thrift.getVersion());
  }

  private static AddInstancesConfig createInstanceConfig(TaskConfig task) {
    return new AddInstancesConfig()
        .setTaskConfig(task)
        .setInstanceIds(ImmutableSet.of(0))
        .setKey(JOB_KEY.newBuilder());
  }

  @Test
  public void testAddInstances() throws Exception {
    AddInstancesConfig config = createInstanceConfig(defaultTask(true));
    expectAuth(ROLE, true);
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(false);
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(LOCK));
    scheduler.addInstances(
        eq(JOB_KEY),
        eq(ImmutableSet.copyOf(config.getInstanceIds())),
        anyObject(ITaskConfig.class));

    control.replay();

    assertOkResponse(thrift.addInstances(config, LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testAddInstancesWithNullLock() throws Exception {
    AddInstancesConfig config = createInstanceConfig(defaultTask(true));
    expectAuth(ROLE, true);
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(false);
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
    scheduler.addInstances(
        eq(JOB_KEY),
        eq(ImmutableSet.copyOf(config.getInstanceIds())),
        anyObject(ITaskConfig.class));
    control.replay();

    assertOkResponse(thrift.addInstances(config, null, SESSION));
  }

  @Test
  public void testAddInstancesFails() throws Exception {
    AddInstancesConfig config = createInstanceConfig(defaultTask(true));
    expectAuth(ROLE, true);
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(false);
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(LOCK));
    scheduler.addInstances(
        eq(JOB_KEY),
        eq(ImmutableSet.copyOf(config.getInstanceIds())),
        anyObject(ITaskConfig.class));
    expectLastCall().andThrow(new ScheduleException("Failed"));

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.addInstances(config, LOCK.newBuilder(), SESSION));
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
  public void testAddInstancesInvalidConfig() throws Exception {
    AddInstancesConfig config = createInstanceConfig(defaultTask(true));
    TaskConfig taskConfig = config.getTaskConfig().setExecutorConfig(null);
    config.setTaskConfig(taskConfig);
    expectAuth(ROLE, true);

    control.replay();

    assertResponse(INVALID_REQUEST, thrift.addInstances(config, LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testAddInstancesAuthFails() throws Exception {
    AddInstancesConfig config = createInstanceConfig(defaultTask(true));
    expectAuth(ROLE, false);

    control.replay();

    assertResponse(AUTH_FAILED, thrift.addInstances(config, LOCK.newBuilder(), SESSION));
  }

  @Test
  public void testAddInstancesFailsForCronJob() throws Exception {
    AddInstancesConfig config = createInstanceConfig(defaultTask(true));
    expectAuth(ROLE, true);
    expect(cronJobManager.hasJob(JOB_KEY)).andReturn(true);
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
  public void testGetQuota() throws Exception {
    QuotaInfo infoMock = createMock(QuotaInfo.class);
    expect(quotaManager.getQuotaInfo(ROLE)).andReturn(infoMock);
    expect(infoMock.guota()).andReturn(QUOTA);
    expect(infoMock.prodConsumption()).andReturn(CONSUMED);
    IResourceAggregate nonProdConsumed = IResourceAggregate.build(new ResourceAggregate(1, 0, 0));
    expect(infoMock.nonProdConsumption()).andReturn(nonProdConsumed);
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

  private static JobConfiguration makeJob() {
    return makeJob(nonProductionTask(), 1);
  }

  private static JobConfiguration makeJob(TaskConfig task) {
    return makeJob(task, 1);
  }

  private static JobConfiguration makeJob(TaskConfig task, int shardCount) {
    return new JobConfiguration()
        .setOwner(ROLE_IDENTITY)
        .setInstanceCount(shardCount)
        .setTaskConfig(task)
        .setKey(JOB_KEY.newBuilder());
  }

  private IExpectationSetters<?> expectAuth(Set<String> roles, boolean allowed)
      throws AuthFailedException {

    if (!allowed) {
      return expect(userValidator.checkAuthenticated(SESSION, roles))
          .andThrow(new AuthFailedException("Denied!"));
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
          anyObject(AuditCheck.class))).andThrow(new AuthFailedException("Denied!"));
    } else {
      return expect(userValidator.checkAuthorized(
          eq(SESSION),
          eq(capability),
          anyObject(AuditCheck.class))).andReturn(context);
    }
  }

  private static TaskConfig defaultTask(boolean production) {
    return new TaskConfig()
        .setOwner(new Identity("role", "user"))
        .setEnvironment(DEFAULT_ENVIRONMENT)
        .setJobName(JOB_NAME)
        .setContactEmail("testing@twitter.com")
        .setExecutorConfig(new ExecutorConfig("aurora", "data"))
        .setNumCpus(1)
        .setRamMb(1024)
        .setDiskMb(1024)
        .setProduction(production);
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
}

/*
 * Copyright 2013 Twitter, Inc.
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
package com.twitter.aurora.scheduler.thrift;

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

import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.auth.SessionValidator.AuthFailedException;
import com.twitter.aurora.gen.AssignedTask;
import com.twitter.aurora.gen.AuroraAdmin;
import com.twitter.aurora.gen.ConfigRewrite;
import com.twitter.aurora.gen.Constraint;
import com.twitter.aurora.gen.DrainHostsResult;
import com.twitter.aurora.gen.EndMaintenanceResult;
import com.twitter.aurora.gen.HostStatus;
import com.twitter.aurora.gen.Hosts;
import com.twitter.aurora.gen.Identity;
import com.twitter.aurora.gen.JobConfigRewrite;
import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.JobKey;
import com.twitter.aurora.gen.LimitConstraint;
import com.twitter.aurora.gen.MaintenanceStatusResult;
import com.twitter.aurora.gen.Quota;
import com.twitter.aurora.gen.Response;
import com.twitter.aurora.gen.ResponseCode;
import com.twitter.aurora.gen.Result;
import com.twitter.aurora.gen.RewriteConfigsRequest;
import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.SessionKey;
import com.twitter.aurora.gen.ShardConfigRewrite;
import com.twitter.aurora.gen.ShardKey;
import com.twitter.aurora.gen.StartMaintenanceResult;
import com.twitter.aurora.gen.StartUpdateResult;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.gen.TaskConstraint;
import com.twitter.aurora.gen.TaskQuery;
import com.twitter.aurora.gen.ValueConstraint;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.ScheduleException;
import com.twitter.aurora.scheduler.configuration.ConfigurationManager;
import com.twitter.aurora.scheduler.configuration.ParsedConfiguration;
import com.twitter.aurora.scheduler.quota.QuotaManager;
import com.twitter.aurora.scheduler.state.CronJobManager;
import com.twitter.aurora.scheduler.state.MaintenanceController;
import com.twitter.aurora.scheduler.state.SchedulerCore;
import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.aurora.scheduler.storage.backup.Recovery;
import com.twitter.aurora.scheduler.storage.backup.StorageBackup;
import com.twitter.aurora.scheduler.storage.testing.StorageTestUtil;
import com.twitter.aurora.scheduler.thrift.aop.AopModule;
import com.twitter.aurora.scheduler.thrift.auth.CapabilityValidator;
import com.twitter.aurora.scheduler.thrift.auth.CapabilityValidator.Capability;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.Clock;
import com.twitter.common.util.testing.FakeClock;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static com.twitter.aurora.auth.SessionValidator.SessionContext;
import static com.twitter.aurora.gen.Constants.DEFAULT_ENVIRONMENT;
import static com.twitter.aurora.gen.MaintenanceMode.DRAINING;
import static com.twitter.aurora.gen.MaintenanceMode.NONE;
import static com.twitter.aurora.gen.MaintenanceMode.SCHEDULED;
import static com.twitter.aurora.gen.ResponseCode.AUTH_FAILED;
import static com.twitter.aurora.gen.ResponseCode.ERROR;
import static com.twitter.aurora.gen.ResponseCode.INVALID_REQUEST;
import static com.twitter.aurora.gen.ResponseCode.OK;
import static com.twitter.aurora.gen.ResponseCode.WARNING;
import static com.twitter.aurora.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;
import static com.twitter.aurora.scheduler.configuration.ConfigurationManager.MAX_TASKS_PER_JOB;
import static com.twitter.aurora.scheduler.thrift.SchedulerThriftInterface.transitionMessage;
import static com.twitter.aurora.scheduler.thrift.auth.CapabilityValidator.Capability.ROOT;

// TODO(ksweeney): Get role from JobKey instead of Identity everywhere in here.
public class SchedulerThriftInterfaceTest extends EasyMockTest {

  private static final String ROLE = "bar_role";
  private static final String USER = "foo_user";
  private static final String JOB_NAME = "job_foo";
  private static final Identity ROLE_IDENTITY = new Identity(ROLE, USER);
  private static final SessionKey SESSION = new SessionKey();
  private static final JobKey JOB_KEY = JobKeys.from(ROLE, DEFAULT_ENVIRONMENT, JOB_NAME);

  private StorageTestUtil storageUtil;
  private SchedulerCore scheduler;
  private CapabilityValidator userValidator;
  private SessionContext context;
  private QuotaManager quotaManager;
  private StorageBackup backup;
  private Recovery recovery;
  private MaintenanceController maintenance;
  private AuroraAdmin.Iface thrift;
  private CronJobManager cronJobManager;

  @Before
  public void setUp() throws Exception {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    scheduler = createMock(SchedulerCore.class);
    userValidator = createMock(CapabilityValidator.class);
    context = createMock(SessionContext.class);
    setUpValidationExpectations();
    quotaManager = createMock(QuotaManager.class);
    backup = createMock(StorageBackup.class);
    recovery = createMock(Recovery.class);
    maintenance = createMock(MaintenanceController.class);
    cronJobManager = createMock(CronJobManager.class);

    // Use guice and install AuthModule to apply AOP-style auth layer.
    Module testModule = new AbstractModule() {
      @Override protected void configure() {
        bind(Clock.class).toInstance(new FakeClock());
        bind(Storage.class).toInstance(storageUtil.storage);
        bind(SchedulerCore.class).toInstance(scheduler);
        bind(CapabilityValidator.class).toInstance(userValidator);
        bind(QuotaManager.class).toInstance(quotaManager);
        bind(StorageBackup.class).toInstance(backup);
        bind(Recovery.class).toInstance(recovery);
        bind(MaintenanceController.class).toInstance(maintenance);
        bind(CronJobManager.class).toInstance(cronJobManager);
        bind(AuroraAdmin.Iface.class).to(SchedulerThriftInterface.class);
      }
    };
    Injector injector = Guice.createInjector(testModule, new AopModule());
    thrift = injector.getInstance(AuroraAdmin.Iface.class);
  }

  private void setUpValidationExpectations() throws Exception {
    expect(userValidator.toString(SESSION)).andReturn(USER).anyTimes();
    expect(context.getIdentity()).andReturn(USER).anyTimes();
  }

  @Test
  public void testCreateJob() throws Exception {
    JobConfiguration job = makeJob();
    expectAuth(ROLE, true);
    scheduler.createJob(ParsedConfiguration.fromUnparsed(job));

    control.replay();

    Response response = thrift.createJob(job, SESSION);
    assertEquals(ResponseCode.OK, response.getResponseCode());
  }

  @Test
  public void testCreateHomogeneousJobNoShards() throws Exception {
    JobConfiguration job = makeJob();
    job.setShardCount(0);
    job.unsetShardCount();
    expectAuth(ROLE, true);

    control.replay();

    Response response = thrift.createJob(job, SESSION);
    assertEquals(ResponseCode.INVALID_REQUEST, response.getResponseCode());
  }

  @Test
  public void testCreateHomogeneousJob() throws Exception {
    JobConfiguration job = makeJob();
    job.setShardCount(2);
    expectAuth(ROLE, true);
    ParsedConfiguration parsed = ParsedConfiguration.fromUnparsed(job);
    assertEquals(2, parsed.getTaskConfigs().size());
    scheduler.createJob(parsed);

    control.replay();

    Response response = thrift.createJob(job, SESSION);
    assertEquals(ResponseCode.OK, response.getResponseCode());
  }

  @Test
  public void testCreateJobBadRequest() throws Exception {
    JobConfiguration job = makeJob();
    expectAuth(ROLE, true);
    scheduler.createJob(ParsedConfiguration.fromUnparsed(job));
    expectLastCall().andThrow(new ScheduleException("Error!"));

    control.replay();

    Response response = thrift.createJob(job, SESSION);
    assertEquals(ResponseCode.INVALID_REQUEST, response.getResponseCode());
  }

  @Test
  public void testCreateJobAuthFailure() throws Exception {
    expectAuth(ROLE, false);

    control.replay();

    Response response = thrift.createJob(makeJob(), SESSION);
    assertEquals(ResponseCode.AUTH_FAILED, response.getResponseCode());
  }

  @Test
  public void testKillTasksImmediate() throws Exception {
    Query.Builder query = Query.unscoped().byJob(JOB_KEY).active();
    ScheduledTask task = new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setTask(new TaskConfig()
                .setOwner(ROLE_IDENTITY)));

    expectAuth(ROOT, false);
    expectAuth(ROLE, true);
    storageUtil.expectTaskFetch(query, task);
    scheduler.killTasks(query, USER);
    storageUtil.expectTaskFetch(query);

    control.replay();

    Response response = thrift.killTasks(query.get(), SESSION);
    assertEquals(ResponseCode.OK, response.getResponseCode());
  }

  @Test
  public void testKillTasksDelayed() throws Exception {
    Query.Builder query = Query.unscoped().byJob(JOB_KEY).active();
    ScheduledTask task = new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setTask(new TaskConfig()
                .setOwner(ROLE_IDENTITY)));

    expectAuth(ROOT, false);
    expectAuth(ROLE, true);
    storageUtil.expectTaskFetch(query, task);
    scheduler.killTasks(query, USER);
    storageUtil.expectTaskFetch(query, task).times(2);
    storageUtil.expectTaskFetch(query);

    control.replay();

    Response response = thrift.killTasks(query.get(), SESSION);
    assertEquals(ResponseCode.OK, response.getResponseCode());
  }

  @Test
  public void testKillTasksAuthFailure() throws Exception {
    Query.Builder query = Query.unscoped().byJob(JOB_KEY).active();
    ScheduledTask task = new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setTask(new TaskConfig()
                .setOwner(ROLE_IDENTITY)));

    expectAuth(ROOT, false);
    expectAuth(ROLE, false);
    storageUtil.expectTaskFetch(query, task);

    control.replay();

    Response response = thrift.killTasks(query.get(), SESSION);
    assertEquals(ResponseCode.AUTH_FAILED, response.getResponseCode());
  }

  @Test
  public void testAdminKillTasks() throws Exception {
    Query.Builder query = Query.unscoped().byJob(JOB_KEY).active();

    expectAuth(ROOT, true);
    scheduler.killTasks(query, USER);
    storageUtil.expectTaskFetch(query);

    control.replay();

    Response response = thrift.killTasks(query.get(), SESSION);
    assertEquals(ResponseCode.OK, response.getResponseCode());
  }

  @Test
  public void testKillTasksInvalidJobname() throws Exception {
    TaskQuery query = new TaskQuery()
        .setOwner(ROLE_IDENTITY)
        .setJobName("");

    control.replay();

    Response response = thrift.killTasks(query, SESSION);
    assertEquals(ResponseCode.INVALID_REQUEST, response.getResponseCode());
  }

  @Test
  public void testKillNonExistentTasks() throws Exception {
    Query.Builder query = Query.unscoped().byJob(JOB_KEY);

    expectAuth(ROOT, true);

    scheduler.killTasks(query, USER);
    expectLastCall().andThrow(new ScheduleException("No jobs matching query"));

    control.replay();

    Response response = thrift.killTasks(query.get(), SESSION);
    assertEquals(ResponseCode.INVALID_REQUEST, response.getResponseCode());
  }

  @Test
  public void testSetQuota() throws Exception {
    Quota quota = new Quota()
        .setNumCpus(10)
        .setDiskMb(100)
        .setRamMb(200);
    expectAuth(ROOT, true);
    quotaManager.setQuota(ROLE, quota);

    control.replay();

    Response response = thrift.setQuota(ROLE, quota, SESSION);
    assertEquals(ResponseCode.OK, response.getResponseCode());
  }

  @Test
  public void testProvisionerSetQuota() throws Exception {
    Quota quota = new Quota()
        .setNumCpus(10)
        .setDiskMb(100)
        .setRamMb(200);
    expectAuth(ROOT, false);
    expectAuth(Capability.PROVISIONER, true);
    quotaManager.setQuota(ROLE, quota);

    control.replay();

    Response response = thrift.setQuota(ROLE, quota, SESSION);
    assertEquals(ResponseCode.OK, response.getResponseCode());
  }

  @Test
  public void testSetQuotaAuthFailure() throws Exception {
    Quota quota = new Quota()
        .setNumCpus(10)
        .setDiskMb(100)
        .setRamMb(200);
    expectAuth(ROOT, false);
    expectAuth(Capability.PROVISIONER, false);

    control.replay();

    Response response = thrift.setQuota(ROLE, quota, SESSION);
    assertEquals(ResponseCode.AUTH_FAILED, response.getResponseCode());
  }

  @Test
  public void testForceTaskState() throws Exception {
    String taskId = "task_id_foo";
    ScheduleStatus status = ScheduleStatus.FAILED;

    scheduler.setTaskStatus(Query.taskScoped(taskId), status, transitionMessage(USER));
    // Expect auth is first called by an interceptor and then by SchedulerThriftInterface to extract
    // the SessionContext.
    // Note: This will change after AOP-style session validation passes in a SessionContext.
    expectAuth(ROOT, true).times(2);

    control.replay();

    Response response = thrift.forceTaskState(taskId, status, SESSION);
    assertEquals(ResponseCode.OK, response.getResponseCode());
  }

  @Test
  public void testForceTaskStateAuthFailure() throws Exception {
    expectAuth(ROOT, false);

    control.replay();

    Response response = thrift.forceTaskState("task", ScheduleStatus.FAILED, SESSION);
    assertEquals(ResponseCode.AUTH_FAILED, response.getResponseCode());
  }

  @Test
  public void testStartUpdate() throws Exception {
    JobConfiguration job = makeJob();
    String token = "token";

    expectAuth(ROLE, true);
    expect(scheduler.initiateJobUpdate(ParsedConfiguration.fromUnparsed(job)))
        .andReturn(Optional.of(token));

    control.replay();
    Response resp = thrift.startUpdate(job, SESSION);
    StartUpdateResult result = resp.getResult().getStartUpdateResult();
    assertEquals(OK, resp.getResponseCode());
    assertEquals(token, result.getUpdateToken());
    assertTrue(result.isRollingUpdateRequired());
  }

  @Test
  public void testStartCronUpdate() throws Exception {
    JobConfiguration job = makeJob();

    expectAuth(ROLE, true);
    expect(scheduler.initiateJobUpdate(ParsedConfiguration.fromUnparsed(job)))
        .andReturn(Optional.<String>absent());

    control.replay();
    Response resp = thrift.startUpdate(job, SESSION);
    StartUpdateResult result = resp.getResult().getStartUpdateResult();
    assertEquals(OK, resp.getResponseCode());
    assertFalse(result.isRollingUpdateRequired());
  }

  @Test
  public void testRestartShards() throws Exception {
    Set<Integer> shards = ImmutableSet.of(1, 6);

    expectAuth(ROLE, true);
    scheduler.restartShards(JOB_KEY, shards, USER);

    control.replay();

    Response resp = thrift.restartShards(JOB_KEY, shards, SESSION);
    assertEquals(ResponseCode.OK, resp.getResponseCode());
  }

  @Test
  public void testRestartShardsFails() throws Exception {
    Set<Integer> shards = ImmutableSet.of(1, 6);

    String message = "Injected.";
    expectAuth(ROLE, true);
    scheduler.restartShards(JOB_KEY, shards, USER);
    expectLastCall().andThrow(new ScheduleException(message));

    control.replay();

    Response resp = thrift.restartShards(JOB_KEY, shards, SESSION);
    assertEquals(ResponseCode.INVALID_REQUEST, resp.getResponseCode());
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
    assertEquals(
        INVALID_REQUEST,
        thrift.createJob(makeJob(task), SESSION).getResponseCode());
  }

  @Test
  public void testCreateJobBadCpu() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    TaskConfig task = productionTask().setNumCpus(0.0);
    assertEquals(
        INVALID_REQUEST,
        thrift.createJob(makeJob(task), SESSION).getResponseCode());
  }

  @Test
  public void testCreateJobBadRam() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    TaskConfig task = productionTask().setRamMb(-123);
    assertEquals(
        INVALID_REQUEST,
        thrift.createJob(makeJob(task), SESSION).getResponseCode());
  }

  @Test
  public void testCreateJobBadDisk() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    TaskConfig task = productionTask().setDiskMb(0);
    assertEquals(
        INVALID_REQUEST,
        thrift.createJob(makeJob(task), SESSION).getResponseCode());
  }

  @Test
  public void testCreateJobPopulateDefaults() throws Exception {
    TaskConfig task = new TaskConfig()
        .setContactEmail("testing@twitter.com")
        .setThermosConfig(new byte[]{1, 2, 3})  // Arbitrary opaque data.
        .setNumCpus(1.0)
        .setRamMb(1024)
        .setDiskMb(1024)
        .setIsService(true)
        .setProduction(true)
        .setOwner(ROLE_IDENTITY)
        .setJobName(JOB_NAME);
    JobConfiguration job = makeJob(task);

    expectAuth(ROLE, true);

    JobConfiguration parsed = job.deepCopy();
    parsed.getTaskConfig()
        .setShardId(0)
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

    scheduler.createJob(new ParsedConfiguration(parsed));

    control.replay();

    assertEquals(OK, thrift.createJob(job, SESSION).getResponseCode());
  }

  @Test
  public void testCreateJobExceedsTaskLimit() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    JobConfiguration job = makeJob(nonProductionTask(), MAX_TASKS_PER_JOB.get() + 1);
    assertEquals(INVALID_REQUEST, thrift.createJob(job, SESSION).getResponseCode());
  }

  @Test
  public void testRewriteShardTaskMissing() throws Exception {
    ShardKey shardKey = new ShardKey(JobKeys.from("foo", "bar", "baz"), 0);

    expectAuth(ROOT, true);
    storageUtil.expectTaskFetch(
        Query.shardScoped(shardKey.getJobKey(), shardKey.getShardId()).active());

    control.replay();

    RewriteConfigsRequest request = new RewriteConfigsRequest(
        ImmutableList.of(ConfigRewrite.shardRewrite(
            new ShardConfigRewrite(shardKey, productionTask(), productionTask()))));
    assertEquals(WARNING, thrift.rewriteConfigs(request, SESSION).getResponseCode());
  }

  @Test
  public void testRewriteShardCasMismatch() throws Exception {
    TaskConfig storedConfig = productionTask();
    TaskConfig modifiedConfig =
        storedConfig.deepCopy().setThermosConfig("rewritten".getBytes());
    ScheduledTask storedTask =
        new ScheduledTask().setAssignedTask(new AssignedTask().setTask(storedConfig));
    ShardKey shardKey = new ShardKey(
        JobKeys.from(
            storedConfig.getOwner().getRole(),
            storedConfig.getEnvironment(),
            storedConfig.getJobName()),
        0);

    expectAuth(ROOT, true);
    storageUtil.expectTaskFetch(
        Query.shardScoped(shardKey.getJobKey(), shardKey.getShardId()).active(), storedTask);

    control.replay();

    RewriteConfigsRequest request = new RewriteConfigsRequest(
        ImmutableList.of(ConfigRewrite.shardRewrite(
            new ShardConfigRewrite(shardKey, modifiedConfig, modifiedConfig))));
    assertEquals(WARNING, thrift.rewriteConfigs(request, SESSION).getResponseCode());
  }

  @Test
  public void testRewriteShard() throws Exception {
    TaskConfig storedConfig = productionTask();
    TaskConfig modifiedConfig =
        storedConfig.deepCopy().setThermosConfig("rewritten".getBytes());
    String taskId = "task_id";
    ScheduledTask storedTask = new ScheduledTask().setAssignedTask(
        new AssignedTask()
            .setTaskId(taskId)
            .setTask(storedConfig));
    ShardKey shardKey = new ShardKey(
        JobKeys.from(
            storedConfig.getOwner().getRole(),
            storedConfig.getEnvironment(),
            storedConfig.getJobName()),
        0);

    expectAuth(ROOT, true);
    storageUtil.expectTaskFetch(
        Query.shardScoped(shardKey.getJobKey(), shardKey.getShardId()).active(), storedTask);
    expect(storageUtil.taskStore.unsafeModifyInPlace(
        taskId,
        ConfigurationManager.applyDefaultsIfUnset(modifiedConfig))).andReturn(true);

    control.replay();

    RewriteConfigsRequest request = new RewriteConfigsRequest(
        ImmutableList.of(ConfigRewrite.shardRewrite(
            new ShardConfigRewrite(shardKey, storedConfig, modifiedConfig))));
    assertEquals(OK, thrift.rewriteConfigs(request, SESSION).getResponseCode());
  }

  @Test
  public void testRewriteJobCasMismatch() throws Exception {
    JobConfiguration oldJob = makeJob(productionTask());
    JobConfiguration newJob = oldJob.deepCopy();
    newJob.getTaskConfig().setThermosConfig("rewritten".getBytes());
    String manager = "manager_key";
    expectAuth(ROOT, true);
    expect(storageUtil.jobStore.fetchManagerIds()).andReturn(ImmutableSet.of(manager));
    expect(storageUtil.jobStore.fetchJobs(manager)).andReturn(ImmutableList.of(oldJob));

    control.replay();

    RewriteConfigsRequest request = new RewriteConfigsRequest(
        ImmutableList.of(ConfigRewrite.jobRewrite(
            new JobConfigRewrite(newJob, newJob))));
    assertEquals(WARNING, thrift.rewriteConfigs(request, SESSION).getResponseCode());
  }

  @Test
  public void testRewriteJobNotFound() throws Exception {
    JobConfiguration oldJob = makeJob(productionTask());
    JobConfiguration newJob = oldJob.deepCopy();
    newJob.getTaskConfig().setThermosConfig("rewritten".getBytes());
    String manager = "manager_key";
    expectAuth(ROOT, true);
    expect(storageUtil.jobStore.fetchManagerIds()).andReturn(ImmutableSet.of(manager));
    expect(storageUtil.jobStore.fetchJobs(manager)).andReturn(ImmutableList.<JobConfiguration>of());

    control.replay();

    RewriteConfigsRequest request = new RewriteConfigsRequest(
        ImmutableList.of(ConfigRewrite.jobRewrite(
            new JobConfigRewrite(oldJob, newJob))));
    assertEquals(WARNING, thrift.rewriteConfigs(request, SESSION).getResponseCode());
  }

  @Test
  public void testRewriteJobMultipleMatches() throws Exception {
    JobConfiguration oldJob = makeJob(productionTask());
    JobConfiguration newJob = oldJob.deepCopy();
    newJob.getTaskConfig().setThermosConfig("rewritten".getBytes());
    String manager = "manager_key";
    expectAuth(ROOT, true);
    expect(storageUtil.jobStore.fetchManagerIds()).andReturn(ImmutableSet.of(manager));
    expect(storageUtil.jobStore.fetchJobs(manager)).andReturn(ImmutableList.of(oldJob, makeJob()));

    control.replay();

    RewriteConfigsRequest request = new RewriteConfigsRequest(
        ImmutableList.of(ConfigRewrite.jobRewrite(
            new JobConfigRewrite(oldJob, newJob))));
    assertEquals(WARNING, thrift.rewriteConfigs(request, SESSION).getResponseCode());
  }

  @Test
  public void testRewriteJob() throws Exception {
    JobConfiguration oldJob = makeJob(productionTask());
    JobConfiguration newJob = oldJob.deepCopy();
    newJob.getTaskConfig().setThermosConfig("rewritten".getBytes());
    String manager = "manager_key";
    expectAuth(ROOT, true);
    expect(storageUtil.jobStore.fetchManagerIds()).andReturn(ImmutableSet.of(manager));
    expect(storageUtil.jobStore.fetchJobs(manager)).andReturn(ImmutableList.of(oldJob));
    storageUtil.jobStore.saveAcceptedJob(manager, ConfigurationManager.validateAndPopulate(newJob));

    control.replay();

    RewriteConfigsRequest request = new RewriteConfigsRequest(
        ImmutableList.of(ConfigRewrite.jobRewrite(
            new JobConfigRewrite(oldJob, newJob))));
    assertEquals(OK, thrift.rewriteConfigs(request, SESSION).getResponseCode());
  }

  @Test
  public void testUpdateJobExceedsTaskLimit() throws Exception {
    expectAuth(ROLE, true);
    JobConfiguration job = makeJob(nonProductionTask(), MAX_TASKS_PER_JOB.get());
    scheduler.createJob(ParsedConfiguration.fromUnparsed(job));
    expectAuth(ROLE, true);

    control.replay();

    thrift.createJob(job, SESSION);
    JobConfiguration updated = makeJob(nonProductionTask(), MAX_TASKS_PER_JOB.get() + 1);
    assertEquals(
        INVALID_REQUEST,
        thrift.startUpdate(updated, SESSION).getResponseCode());
  }

  @Test
  public void testCreateEmptyJob() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    JobConfiguration job = new JobConfiguration()
        .setKey(JOB_KEY)
        .setOwner(ROLE_IDENTITY)
        .setName(JOB_NAME);
    assertEquals(
        INVALID_REQUEST,
        thrift.createJob(job, SESSION).getResponseCode());
  }

  @Test
  public void testLimitConstraintForDedicatedJob() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    TaskConfig task = nonProductionTask();
    task.addToConstraints(dedicatedConstraint(1));
    assertEquals(
        INVALID_REQUEST,
        thrift.createJob(makeJob(task), SESSION).getResponseCode());
  }

  @Test
  public void testMultipleValueConstraintForDedicatedJob() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    TaskConfig task = nonProductionTask();
    task.addToConstraints(dedicatedConstraint(ImmutableSet.of("mesos", "test")));
    assertEquals(
        INVALID_REQUEST,
        thrift.createJob(makeJob(task), SESSION).getResponseCode());
  }

  @Test
  public void testUnauthorizedDedicatedJob() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    TaskConfig task = nonProductionTask();
    task.addToConstraints(dedicatedConstraint(ImmutableSet.of("mesos")));
    assertEquals(
        INVALID_REQUEST,
        thrift.createJob(makeJob(task), SESSION).getResponseCode());
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
        new Response().setResponseCode(OK).setResult(
            Result.maintenanceStatusResult(new MaintenanceStatusResult().setStatuses(none))),
        thrift.maintenanceStatus(hosts, SESSION));
    assertEquals(
        new Response().setResponseCode(OK).setResult(
            Result.startMaintenanceResult(new StartMaintenanceResult().setStatuses(scheduled))),
        thrift.startMaintenance(hosts, SESSION)
    );
    assertEquals(
        new Response().setResponseCode(OK).setResult(
            Result.drainHostsResult(new DrainHostsResult().setStatuses(draining))),
        thrift.drainHosts(hosts, SESSION));
    assertEquals(
        new Response().setResponseCode(OK).setResult(
            Result.maintenanceStatusResult(new MaintenanceStatusResult().setStatuses(draining))),
        thrift.maintenanceStatus(hosts, SESSION));
    assertEquals(
        new Response().setResponseCode(OK).setResult(
            Result.maintenanceStatusResult(new MaintenanceStatusResult().setStatuses(drained))),
        thrift.maintenanceStatus(hosts, SESSION));
    assertEquals(
        new Response().setResponseCode(OK).setResult(
            Result.endMaintenanceResult(new EndMaintenanceResult().setStatuses(none))),
        thrift.endMaintenance(hosts, SESSION));
  }

  @Test
  public void testGetJobs() throws Exception {
    TaskConfig ownedCronJobTask = nonProductionTask()
        .setJobName(JobKeys.TO_JOB_NAME.apply(JOB_KEY))
        .setOwner(ROLE_IDENTITY)
        .setEnvironment(JobKeys.TO_ENVIRONMENT.apply(JOB_KEY));
    JobConfiguration ownedCronJob = makeJob()
        .setCronSchedule("0 * * * *")
        .setTaskConfig(ownedCronJobTask);
    ScheduledTask ownedCronJobScheduledTask = new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(ownedCronJobTask));
    Identity otherOwner = new Identity("other", "other");
    JobConfiguration unownedCronJob = makeJob()
        .setOwner(otherOwner)
        .setCronSchedule("0 * * * *")
        .setKey(JOB_KEY.deepCopy().setRole("other"))
        .setTaskConfig(ownedCronJobTask.deepCopy().setOwner(otherOwner));
    TaskConfig ownedImmediateTaskInfo = defaultTask(false)
        .setJobName("immediate")
        .setOwner(ROLE_IDENTITY);
    Set<JobConfiguration> ownedCronJobOnly = ImmutableSet.of(ownedCronJob);
    Set<JobConfiguration> unownedCronJobOnly = ImmutableSet.of(unownedCronJob);
    Set<JobConfiguration> bothCronJobs = ImmutableSet.of(ownedCronJob, unownedCronJob);
    ScheduledTask ownedImmediateTask = new ScheduledTask()
        .setAssignedTask(
            new AssignedTask().setTask(ownedImmediateTaskInfo));
    JobConfiguration ownedImmediateJob = new JobConfiguration()
        .setKey(JOB_KEY.deepCopy().setName("immediate"))
        .setOwner(ROLE_IDENTITY)
        .setShardCount(1)
        .setTaskConfig(ownedImmediateTaskInfo);
    Query.Builder query = Query.roleScoped(ROLE).active();

    expect(cronJobManager.getJobs()).andReturn(ownedCronJobOnly);
    storageUtil.expectTaskFetch(query);

    expect(cronJobManager.getJobs()).andReturn(bothCronJobs);
    storageUtil.expectTaskFetch(query);

    expect(cronJobManager.getJobs()).andReturn(unownedCronJobOnly);
    storageUtil.expectTaskFetch(query, ownedImmediateTask);

    expect(cronJobManager.getJobs()).andReturn(ImmutableSet.<JobConfiguration>of());
    storageUtil.expectTaskFetch(query);

    // Handle the case where a cron job has a running task (same JobKey present in both stores).
    expect(cronJobManager.getJobs()).andReturn(ImmutableList.of(ownedCronJob));
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
        .setKey(JOB_KEY)
        .setTaskConfig(nonProductionTask());
    JobKey jobKey2 = JOB_KEY.deepCopy().setRole("other_role");
    JobConfiguration cronJobTwo = makeJob()
        .setCronSchedule("2 * * * *")
        .setKey(jobKey2)
        .setTaskConfig(nonProductionTask());
    TaskConfig immediateTaskConfig = defaultTask(false)
        .setJobName("immediate")
        .setOwner(ROLE_IDENTITY);
    ScheduledTask immediateTask = new ScheduledTask()
        .setAssignedTask(
            new AssignedTask().setTask(immediateTaskConfig));
    JobConfiguration immediateJob = new JobConfiguration()
        .setKey(JOB_KEY.deepCopy().setName("immediate"))
        .setOwner(ROLE_IDENTITY)
        .setShardCount(1)
        .setTaskConfig(immediateTaskConfig);

    Set<JobConfiguration> crons = ImmutableSet.of(cronJobOne, cronJobTwo);
    expect(cronJobManager.getJobs()).andReturn(crons);
    storageUtil.expectTaskFetch(Query.unscoped().active(), immediateTask);

    control.replay();

    Set<JobConfiguration> allJobs =
        ImmutableSet.<JobConfiguration>builder().addAll(crons).add(immediateJob).build();
    assertEquals(allJobs, thrift.getJobs(null).getResult().getGetJobsResult().getConfigs());
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

    assertEquals(AUTH_FAILED, thrift.snapshot(SESSION).getResponseCode());
    assertEquals(OK, thrift.snapshot(SESSION).getResponseCode());
    assertEquals(ERROR, thrift.snapshot(SESSION).getResponseCode());
  }

  @Test
  public void testKillAuthenticatesQueryRole() throws Exception {
    expectAuth(ROOT, false);
    expectAuth(ImmutableSet.of("foo", ROLE), true);

    Query.Builder query = Query.roleScoped("foo");
    ScheduledTask task = new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setTask(new TaskConfig()
                .setOwner(ROLE_IDENTITY)));

    storageUtil.expectTaskFetch(query, task);
    scheduler.killTasks(query, USER);
    storageUtil.expectTaskFetch(query.active());

    control.replay();

    assertEquals(OK, thrift.killTasks(query.get(), SESSION).getResponseCode());
  }

  private JobConfiguration makeJob() {
    return makeJob(nonProductionTask(), 1);
  }

  private JobConfiguration makeJob(TaskConfig task) {
    return makeJob(task, 1);
  }

  private JobConfiguration makeJob(TaskConfig task, int shardCount) {
    return new JobConfiguration()
        .setName(JOB_NAME)
        .setOwner(ROLE_IDENTITY)
        .setShardCount(shardCount)
        .setTaskConfig(task)
        .setKey(JOB_KEY);
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
      return expect(userValidator.checkAuthorized(SESSION, capability))
          .andThrow(new AuthFailedException("Denied!"));
    } else {
      return expect(userValidator.checkAuthorized(SESSION, capability))
          .andReturn(context);
    }
  }

  private static TaskConfig defaultTask(boolean production) {
    return new TaskConfig()
        .setOwner(new Identity("role", "user"))
        .setEnvironment(DEFAULT_ENVIRONMENT)
        .setJobName(JOB_NAME)
        .setContactEmail("testing@twitter.com")
        .setThermosConfig("data".getBytes())
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

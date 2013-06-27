package com.twitter.mesos.scheduler.thrift;

import java.util.Iterator;
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

import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.Clock;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.auth.SessionValidator.AuthFailedException;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Constraint;
import com.twitter.mesos.gen.CreateJobResponse;
import com.twitter.mesos.gen.DrainHostsResponse;
import com.twitter.mesos.gen.EndMaintenanceResponse;
import com.twitter.mesos.gen.ForceTaskStateResponse;
import com.twitter.mesos.gen.HostStatus;
import com.twitter.mesos.gen.Hosts;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.JobKey;
import com.twitter.mesos.gen.KillResponse;
import com.twitter.mesos.gen.LimitConstraint;
import com.twitter.mesos.gen.MaintenanceStatusResponse;
import com.twitter.mesos.gen.MesosAdmin;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.ResponseCode;
import com.twitter.mesos.gen.RestartShardsResponse;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.SessionKey;
import com.twitter.mesos.gen.SetQuotaResponse;
import com.twitter.mesos.gen.StartMaintenanceResponse;
import com.twitter.mesos.gen.StartUpdateResponse;
import com.twitter.mesos.gen.TaskConstraint;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.ValueConstraint;
import com.twitter.mesos.scheduler.CronJobManager;
import com.twitter.mesos.scheduler.MaintenanceController;
import com.twitter.mesos.scheduler.ScheduleException;
import com.twitter.mesos.scheduler.SchedulerCore;
import com.twitter.mesos.scheduler.base.JobKeys;
import com.twitter.mesos.scheduler.base.Query;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.configuration.ParsedConfiguration;
import com.twitter.mesos.scheduler.quota.QuotaManager;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.backup.Recovery;
import com.twitter.mesos.scheduler.storage.backup.StorageBackup;
import com.twitter.mesos.scheduler.storage.testing.StorageTestUtil;
import com.twitter.mesos.scheduler.thrift.auth.CapabilityValidator;
import com.twitter.mesos.scheduler.thrift.auth.CapabilityValidator.Capability;
import com.twitter.mesos.scheduler.thrift.auth.ThriftAuthModule;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static com.twitter.mesos.gen.Constants.DEFAULT_ENVIRONMENT;
import static com.twitter.mesos.gen.MaintenanceMode.DRAINING;
import static com.twitter.mesos.gen.MaintenanceMode.NONE;
import static com.twitter.mesos.gen.MaintenanceMode.SCHEDULED;
import static com.twitter.mesos.gen.ResponseCode.INVALID_REQUEST;
import static com.twitter.mesos.gen.ResponseCode.OK;
import static com.twitter.mesos.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;
import static com.twitter.mesos.scheduler.configuration.ConfigurationManager.MAX_TASKS_PER_JOB;
import static com.twitter.mesos.scheduler.thrift.SchedulerThriftInterface.transitionMessage;
import static com.twitter.mesos.scheduler.thrift.auth.CapabilityValidator.Capability.ROOT;

// TODO(ksweeney): Get role from JobKey instead of Identity everywhere in here.
public class SchedulerThriftInterfaceTest extends EasyMockTest {

  private static final String ROLE = "bar_role";
  private static final String USER = "foo_user";
  private static final String JOB_NAME = "job_foo";
  private static final Identity ROLE_IDENTITY = new Identity(ROLE, USER);
  private static final SessionKey SESSION = new SessionKey().setUser(USER);
  private static final JobKey JOB_KEY = JobKeys.from(ROLE, DEFAULT_ENVIRONMENT, JOB_NAME);

  private StorageTestUtil storageUtil;
  private SchedulerCore scheduler;
  private CapabilityValidator userValidator;
  private QuotaManager quotaManager;
  private StorageBackup backup;
  private Recovery recovery;
  private MaintenanceController maintenance;
  private MesosAdmin.Iface thrift;
  private CronJobManager cronJobManager;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    scheduler = createMock(SchedulerCore.class);
    userValidator = createMock(CapabilityValidator.class);
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
        bind(SchedulerController.class).to(SchedulerThriftInterface.class);
        bind(MesosAdmin.Iface.class).to(SchedulerThriftRouter.class);
      }
    };
    Injector injector = Guice.createInjector(testModule, new ThriftAuthModule());
    thrift = injector.getInstance(MesosAdmin.Iface.class);
  }

  @Test
  public void testCreateJob() throws Exception {
    JobConfiguration job = makeJob();
    expectAuth(ROLE, true);
    scheduler.createJob(ParsedConfiguration.fromUnparsed(job));

    control.replay();

    CreateJobResponse response = thrift.createJob(job, SESSION);
    assertEquals(ResponseCode.OK, response.getResponseCode());
  }

  @Test
  public void testCreateHomogeneousJobNoShards() throws Exception {
    JobConfiguration job = makeJob();
    job.unsetTaskConfigs();
    job.setTaskConfig(nonProductionTask());
    expectAuth(ROLE, true);

    control.replay();

    CreateJobResponse response = thrift.createJob(job, SESSION);
    assertEquals(ResponseCode.INVALID_REQUEST, response.getResponseCode());
  }

  @Test
  public void testCreateHomogeneousJob() throws Exception {
    JobConfiguration job = makeJob();
    job.unsetTaskConfigs();
    job.setTaskConfig(nonProductionTask());
    job.setShardCount(2);
    expectAuth(ROLE, true);
    ParsedConfiguration parsed = ParsedConfiguration.fromUnparsed(job);
    assertEquals(2, parsed.get().getTaskConfigsSize());
    scheduler.createJob(parsed);

    control.replay();

    CreateJobResponse response = thrift.createJob(job, SESSION);
    assertEquals(ResponseCode.OK, response.getResponseCode());
  }

  @Test
  public void testCreateJobBadRequest() throws Exception {
    JobConfiguration job = makeJob();
    expectAuth(ROLE, true);
    scheduler.createJob(ParsedConfiguration.fromUnparsed(job));
    expectLastCall().andThrow(new ScheduleException("Error!"));

    control.replay();

    CreateJobResponse response = thrift.createJob(job, SESSION);
    assertEquals(ResponseCode.INVALID_REQUEST, response.getResponseCode());
  }

  @Test
  public void testCreateJobAuthFailure() throws Exception {
    expectAuth(ROLE, false);

    control.replay();

    CreateJobResponse response = thrift.createJob(makeJob(), SESSION);
    assertEquals(ResponseCode.AUTH_FAILED, response.getResponseCode());
  }

  @Test
  public void testKillTasksImmediate() throws Exception {
    TaskQuery query = new TaskQuery()
        .setOwner(ROLE_IDENTITY)
        .setJobName("foo_job");
    ScheduledTask task = new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setTask(new TwitterTaskInfo()
                .setOwner(ROLE_IDENTITY)));

    expectAuth(ROOT, false);
    expectAuth(ROLE, true);
    storageUtil.expectTaskFetch(query, task);
    scheduler.killTasks(query, USER);
    storageUtil.expectTaskFetch(query);

    control.replay();

    KillResponse response = thrift.killTasks(query, SESSION);
    assertEquals(ResponseCode.OK, response.getResponseCode());
  }

  @Test
  public void testKillTasksDelayed() throws Exception {
    TaskQuery query = new TaskQuery()
        .setOwner(ROLE_IDENTITY)
        .setJobName("foo_job");
    ScheduledTask task = new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setTask(new TwitterTaskInfo()
                .setOwner(ROLE_IDENTITY)));

    expectAuth(ROOT, false);
    expectAuth(ROLE, true);
    storageUtil.expectTaskFetch(query, task);
    scheduler.killTasks(query, USER);
    storageUtil.expectTaskFetch(query, task).times(2);
    storageUtil.expectTaskFetch(query);

    control.replay();

    KillResponse response = thrift.killTasks(query, SESSION);
    assertEquals(ResponseCode.OK, response.getResponseCode());
  }

  @Test
  public void testKillTasksAuthFailure() throws Exception {
    TaskQuery query = new TaskQuery()
        .setOwner(ROLE_IDENTITY)
        .setJobName("foo_job");
    ScheduledTask task = new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setTask(new TwitterTaskInfo()
                .setOwner(ROLE_IDENTITY)));

    expectAuth(ROOT, false);
    expectAuth(ROLE, false);
    storageUtil.expectTaskFetch(query, task);

    control.replay();

    KillResponse response = thrift.killTasks(query, SESSION);
    assertEquals(ResponseCode.AUTH_FAILED, response.getResponseCode());
  }

  @Test
  public void testAdminKillTasks() throws Exception {
    TaskQuery query = new TaskQuery()
        .setOwner(ROLE_IDENTITY)
        .setJobName("foo_job");

    expectAuth(ROOT, true);
    scheduler.killTasks(query, USER);
    storageUtil.expectTaskFetch(query);

    control.replay();

    KillResponse response = thrift.killTasks(query, SESSION);
    assertEquals(ResponseCode.OK, response.getResponseCode());
  }

  @Test
  public void testKillTasksInvalidJobname() throws Exception {
    TaskQuery query = new TaskQuery()
        .setOwner(ROLE_IDENTITY)
        .setJobName("");

    control.replay();

    KillResponse response = thrift.killTasks(query, SESSION);
    assertEquals(ResponseCode.INVALID_REQUEST, response.getResponseCode());
  }

  @Test
  public void testKillNonExistentTasks() throws Exception {
    TaskQuery query = new TaskQuery()
        .setOwner(ROLE_IDENTITY)
        .setJobName("foo_job");

    expectAuth(ROOT, true);

    scheduler.killTasks(query, USER);
    expectLastCall().andThrow(new ScheduleException("No jobs matching query"));

    control.replay();

    KillResponse response = thrift.killTasks(query, SESSION);
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

    SetQuotaResponse response = thrift.setQuota(ROLE, quota, SESSION);
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

    SetQuotaResponse response = thrift.setQuota(ROLE, quota, SESSION);
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

    SetQuotaResponse response = thrift.setQuota(ROLE, quota, SESSION);
    assertEquals(ResponseCode.AUTH_FAILED, response.getResponseCode());
  }

  @Test
  public void testForceTaskState() throws Exception {
    String taskId = "task_id_foo";
    ScheduleStatus status = ScheduleStatus.FAILED;

    scheduler.setTaskStatus(Query.byId(taskId), status, transitionMessage(SESSION.getUser()));
    expectAuth(ROOT, true);

    control.replay();

    ForceTaskStateResponse response = thrift.forceTaskState(taskId, status, SESSION);
    assertEquals(ResponseCode.OK, response.getResponseCode());
  }

  @Test
  public void testForceTaskStateAuthFailure() throws Exception {
    expectAuth(ROOT, false);

    control.replay();

    ForceTaskStateResponse response = thrift.forceTaskState(
        "task",
        ScheduleStatus.FAILED,
        SESSION);
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
    StartUpdateResponse resp = thrift.startUpdate(job, SESSION);
    assertEquals(token, resp.getUpdateToken());
    assertEquals(ResponseCode.OK, resp.getResponseCode());
    assertTrue(resp.isRollingUpdateRequired());
  }

  @Test
  public void testStartCronUpdate() throws Exception {
    JobConfiguration job = makeJob();

    expectAuth(ROLE, true);
    expect(scheduler.initiateJobUpdate(ParsedConfiguration.fromUnparsed(job)))
        .andReturn(Optional.<String>absent());

    control.replay();
    StartUpdateResponse resp = thrift.startUpdate(job, SESSION);
    assertEquals(ResponseCode.OK, resp.getResponseCode());
    assertFalse(resp.isRollingUpdateRequired());
  }

  @Test
  public void testRestartShards() throws Exception {
    Set<Integer> shards = ImmutableSet.of(1, 6);

    expectAuth(ROLE, true);
    scheduler.restartShards(JOB_KEY, shards, USER);

    control.replay();

    RestartShardsResponse resp = thrift.restartShards(JOB_KEY, shards, SESSION);
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

    RestartShardsResponse resp = thrift.restartShards(JOB_KEY, shards, SESSION);
    assertEquals(ResponseCode.INVALID_REQUEST, resp.getResponseCode());
    assertEquals(message, resp.getMessage());
  }

  @Test
  public void testCreateJobNoResources() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    TwitterTaskInfo task = productionTask();
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

    TwitterTaskInfo task = productionTask().setNumCpus(0.0);
    assertEquals(
        INVALID_REQUEST,
        thrift.createJob(makeJob(task), SESSION).getResponseCode());
  }

  @Test
  public void testCreateJobBadRam() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    TwitterTaskInfo task = productionTask().setRamMb(-123);
    assertEquals(
        INVALID_REQUEST,
        thrift.createJob(makeJob(task), SESSION).getResponseCode());
  }

  @Test
  public void testCreateJobBadDisk() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    TwitterTaskInfo task = productionTask().setDiskMb(0);
    assertEquals(
        INVALID_REQUEST,
        thrift.createJob(makeJob(task), SESSION).getResponseCode());
  }

  @Test
  public void testCreateJobPopulateDefaults() throws Exception {
    TwitterTaskInfo task = new TwitterTaskInfo()
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
    for (TwitterTaskInfo parsedTask : parsed.getTaskConfigs()) {
      parsedTask.setHealthCheckIntervalSecs(30)
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
    }
    // Task configs are placed in a HashSet after deepCopy, equals() does not play nicely between
    // HashSet and ImmutableSet - dropping an ImmutableSet in place keeps equals() happy.
    parsed.setTaskConfigs(ImmutableSet.copyOf(parsed.getTaskConfigs()));

    scheduler.createJob(new ParsedConfiguration(parsed));

    control.replay();

    assertEquals(ResponseCode.OK, thrift.createJob(job, SESSION).getResponseCode());
  }

  private Set<TwitterTaskInfo> taskCopies(TwitterTaskInfo task, int copies) {
    ImmutableSet.Builder<TwitterTaskInfo> tasks = ImmutableSet.builder();
    for (int i = 0; i < copies; i++) {
      tasks.add(task.deepCopy().setShardId(i));
    }
    return tasks.build();
  }

  @Test
  public void testCreateJobExceedsTaskLimit() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    JobConfiguration job = makeJob(taskCopies(nonProductionTask(), MAX_TASKS_PER_JOB.get() + 1));
    assertEquals(INVALID_REQUEST, thrift.createJob(job, SESSION).getResponseCode());
  }

  @Test
  public void testUpdateJobExceedsTaskLimit() throws Exception {
    expectAuth(ROLE, true);
    JobConfiguration job = makeJob(taskCopies(nonProductionTask(), MAX_TASKS_PER_JOB.get()));
    scheduler.createJob(ParsedConfiguration.fromUnparsed(job));
    expectAuth(ROLE, true);

    control.replay();

    thrift.createJob(job, SESSION);
    JobConfiguration updated =
        makeJob(taskCopies(nonProductionTask(), MAX_TASKS_PER_JOB.get() + 1));
    assertEquals(
        INVALID_REQUEST,
        thrift.startUpdate(updated, SESSION).getResponseCode());
  }

  @Test
  public void testCreateEmptyJob() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    JobConfiguration job = new JobConfiguration()
        .setOwner(ROLE_IDENTITY)
        .setName(JOB_NAME)
        .setTaskConfigs(ImmutableSet.<TwitterTaskInfo>of());
    assertEquals(
        INVALID_REQUEST,
        thrift.createJob(job, SESSION).getResponseCode());
  }

  @Test
  public void testLimitConstraintForDedicatedJob() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    TwitterTaskInfo task = nonProductionTask();
    task.addToConstraints(dedicatedConstraint(1));
    assertEquals(
        INVALID_REQUEST,
        thrift.createJob(makeJob(task), SESSION).getResponseCode());
  }

  @Test
  public void testMultipleValueConstraintForDedicatedJob() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    TwitterTaskInfo task = nonProductionTask();
    task.addToConstraints(dedicatedConstraint(ImmutableSet.of("mesos", "test")));
    assertEquals(
        INVALID_REQUEST,
        thrift.createJob(makeJob(task), SESSION).getResponseCode());
  }

  @Test
  public void testUnauthorizedDedicatedJob() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    TwitterTaskInfo task = nonProductionTask();
    task.addToConstraints(dedicatedConstraint(ImmutableSet.of("mesos")));
    assertEquals(
        INVALID_REQUEST,
        thrift.createJob(makeJob(task), SESSION).getResponseCode());
  }

  @Test
  public void testRejectsMixedProductionMode() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    TwitterTaskInfo nonProduction = nonProductionTask();
    TwitterTaskInfo production = productionTask();
    assertEquals(
        INVALID_REQUEST,
        thrift
            .createJob(makeJob(nonProduction, production), SESSION).getResponseCode());
  }

  @Test
  public void testCreateJobMissingShardIds() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    JobConfiguration job = makeJob(productionTask());
    job.getTaskConfigs().iterator().next().unsetShardId();
    assertEquals(INVALID_REQUEST, thrift.createJob(job, SESSION).getResponseCode());
  }

  @Test
  public void testCreateJobDuplicateShardIds() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    // Tasks are internally placed in a Set, so a differentiating value is needed to prevent
    // them from colliding.
    JobConfiguration job =
        makeJob(productionTask().setNumCpus(1.0), productionTask().setNumCpus(2.0));
    for (TwitterTaskInfo task : job.getTaskConfigs()) {
      task.setShardId(0);
    }
    assertEquals(INVALID_REQUEST, thrift.createJob(job, SESSION).getResponseCode());
  }

  @Test
  public void testCreateJobShardIdHole() throws Exception {
    expectAuth(ROLE, true);

    control.replay();

    JobConfiguration job = makeJob(productionTask(), productionTask());
    job.setTaskConfigs(ImmutableSet.of(
        Iterables.get(job.getTaskConfigs(), 0).setShardId(0),
        Iterables.get(job.getTaskConfigs(), 0).setShardId(2)));
    assertEquals(INVALID_REQUEST, thrift.createJob(job, SESSION).getResponseCode());
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
        new MaintenanceStatusResponse().setResponseCode(OK).setStatuses(none),
        thrift.maintenanceStatus(hosts, SESSION));
    assertEquals(
        new StartMaintenanceResponse().setResponseCode(OK).setStatuses(scheduled),
        thrift.startMaintenance(hosts, SESSION));
    assertEquals(
        new DrainHostsResponse().setResponseCode(OK).setStatuses(draining),
        thrift.drainHosts(hosts, SESSION));
    assertEquals(
        new MaintenanceStatusResponse().setResponseCode(OK).setStatuses(draining),
        thrift.maintenanceStatus(hosts, SESSION));
    assertEquals(
        new MaintenanceStatusResponse().setResponseCode(OK).setStatuses(drained),
        thrift.maintenanceStatus(hosts, SESSION));
    assertEquals(
        new EndMaintenanceResponse().setResponseCode(OK).setStatuses(none),
        thrift.endMaintenance(hosts, SESSION));
  }

  @Test
  public void testGetJobs() throws Exception {
    TwitterTaskInfo ownedCronJobTask = nonProductionTask()
        .setJobName(JobKeys.TO_JOB_NAME.apply(JOB_KEY))
        .setOwner(ROLE_IDENTITY)
        .setEnvironment(JobKeys.TO_ENVIRONMENT.apply(JOB_KEY));
    JobConfiguration ownedCronJob = makeJob()
        .setCronSchedule("0 * * * *")
        .setTaskConfigs(ImmutableSet.of(ownedCronJobTask));
    ScheduledTask ownedCronJobScheduledTask = new ScheduledTask()
        .setAssignedTask(new AssignedTask().setTask(ownedCronJobTask));
    Identity otherOwner = new Identity("other", "other");
    JobConfiguration unownedCronJob = makeJob()
        .setOwner(otherOwner)
        .setCronSchedule("0 * * * *")
        .setKey(JOB_KEY.deepCopy().setRole("other"))
        .setTaskConfigs(ImmutableSet.of(ownedCronJobTask.deepCopy().setOwner(otherOwner)));
    TwitterTaskInfo ownedImmediateTaskInfo = defaultTask(false)
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

    assertEquals(ownedCronJob, Iterables.getOnlyElement(thrift.getJobs(ROLE).getConfigs()));

    assertEquals(ownedCronJob, Iterables.getOnlyElement(thrift.getJobs(ROLE).getConfigs()));

    Set<JobConfiguration> queryResult3 = thrift.getJobs(ROLE).getConfigs();
    assertEquals(ownedImmediateJob, Iterables.getOnlyElement(queryResult3));
    assertEquals(ownedImmediateTaskInfo, Iterables.getOnlyElement(queryResult3).getTaskConfig());

    assertTrue(thrift.getJobs(ROLE).getConfigs().isEmpty());

    assertEquals(ownedCronJob, Iterables.getOnlyElement(thrift.getJobs(ROLE).getConfigs()));
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

    assertEquals(ResponseCode.AUTH_FAILED, thrift.snapshot(SESSION).getResponseCode());
    assertEquals(ResponseCode.OK, thrift.snapshot(SESSION).getResponseCode());
    assertEquals(ResponseCode.ERROR, thrift.snapshot(SESSION).getResponseCode());
  }

  private JobConfiguration makeJob() {
    return makeJob(nonProductionTask());
  }

  private JobConfiguration makeJob(TwitterTaskInfo task, TwitterTaskInfo... tasks) {
    return makeJob(ImmutableSet.<TwitterTaskInfo>builder().add(task).add(tasks).build());
  }

  private JobConfiguration makeJob(Set<TwitterTaskInfo> tasks) {
    Iterator<TwitterTaskInfo> iterator = tasks.iterator();
    for (int i = 0; i < tasks.size(); i++) {
      iterator.next().setShardId(i);
    }
    return new JobConfiguration()
        .setName(JOB_NAME)
        .setOwner(ROLE_IDENTITY)
        .setTaskConfigs(tasks)
        .setKey(JOB_KEY);
  }

  private IExpectationSetters<?> expectAuth(String role, boolean allowed)
      throws AuthFailedException {

    userValidator.checkAuthenticated(SESSION, role);
    if (!allowed) {
      return expectLastCall().andThrow(new AuthFailedException("Denied!"));
    } else {
      return expectLastCall();
    }
  }

  private IExpectationSetters<?> expectAuth(Capability capability, boolean allowed)
      throws AuthFailedException {

    userValidator.checkAuthorized(SESSION, capability);
    if (!allowed) {
      return expectLastCall().andThrow(new AuthFailedException("Denied!"));
    } else {
      return expectLastCall();
    }
  }

  private static TwitterTaskInfo defaultTask(boolean production) {
    return new TwitterTaskInfo()
        .setContactEmail("testing@twitter.com")
        .setThermosConfig("data".getBytes())
        .setNumCpus(1)
        .setRamMb(1024)
        .setDiskMb(1024)
        .setProduction(production)
        .setEnvironment(DEFAULT_ENVIRONMENT);
  }

  private static TwitterTaskInfo productionTask() {
    return defaultTask(true);
  }

  private static TwitterTaskInfo nonProductionTask() {
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

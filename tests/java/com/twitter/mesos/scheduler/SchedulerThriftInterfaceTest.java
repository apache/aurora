package com.twitter.mesos.scheduler;

import com.google.common.collect.ImmutableSet;

import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.CreateJobResponse;
import com.twitter.mesos.gen.ForceTaskStateResponse;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.KillResponse;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.ResponseCode;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.SessionKey;
import com.twitter.mesos.gen.SetQuotaResponse;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.auth.SessionValidator;
import com.twitter.mesos.scheduler.auth.SessionValidator.AuthFailedException;
import com.twitter.mesos.scheduler.quota.QuotaManager;

import static com.twitter.mesos.scheduler.SchedulerThriftInterface.transitionMessage;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;

/**
 * @author William Farner
 */
public class SchedulerThriftInterfaceTest extends EasyMockTest {

  private static final String ROLE = "bar_role";
  private static final String USER = "foo_user";
  private static final Identity ROLE_IDENTITY = new Identity(ROLE, USER);
  private static final SessionKey SESSION = new SessionKey().setUser(USER);

  private SchedulerCore scheduler;
  private SessionValidator sessionValidator;
  private QuotaManager quotaManager;
  private SchedulerThriftInterface thriftInterface;

  @Before
  public void setUp() {
    scheduler = createMock(SchedulerCore.class);
    sessionValidator = createMock(SessionValidator.class);
    quotaManager = createMock(QuotaManager.class);
    thriftInterface = new SchedulerThriftInterface(scheduler, sessionValidator, quotaManager,
        Amount.of(1L, Time.MILLISECONDS), Amount.of(1L, Time.SECONDS));
  }

  @Test
  public void testCreateJob() throws Exception {
    JobConfiguration job = makeJob();
    expectAuth(ROLE, true);
    scheduler.createJob(job);

    control.replay();

    CreateJobResponse response = thriftInterface.createJob(job, SESSION);
    assertEquals(ResponseCode.OK, response.getResponseCode());
  }

  @Test
  public void testCreateJobBadRequest() throws Exception {
    JobConfiguration job = makeJob();
    expectAuth(ROLE, true);
    scheduler.createJob(job);
    expectLastCall().andThrow(new ScheduleException("Error!"));

    control.replay();

    CreateJobResponse response = thriftInterface.createJob(job, SESSION);
    assertEquals(ResponseCode.INVALID_REQUEST, response.getResponseCode());
  }

  @Test
  public void testCreateJobAuthFailure() throws Exception {
    expectAuth(ROLE, false);

    control.replay();

    CreateJobResponse response = thriftInterface.createJob(makeJob(), SESSION);
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

    expectAdminAuth(false);
    expectAuth(ROLE, true);
    Capture<TaskQuery> queryCapture = new Capture<TaskQuery>();
    expect(scheduler.getTasks(capture(queryCapture)))
        .andReturn(ImmutableSet.of(task));
    Capture<TaskQuery> killQueryCapture = new Capture<TaskQuery>();
    scheduler.killTasks(capture(killQueryCapture), eq(USER));
    expect(scheduler.getTasks(capture(queryCapture)))
        .andReturn(ImmutableSet.<ScheduledTask>of());
    control.replay();

    KillResponse response = thriftInterface.killTasks(query, SESSION);
    assertEquals(ResponseCode.OK, response.getResponseCode());
    assertEquals(queryCapture.getValue(), query);
    assertEquals(killQueryCapture.getValue(), query);
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

    expectAdminAuth(false);
    expectAuth(ROLE, true);
    Capture<TaskQuery> queryCapture = new Capture<TaskQuery>();
    expect(scheduler.getTasks(capture(queryCapture)))
        .andReturn(ImmutableSet.of(task));
    Capture<TaskQuery> killQueryCapture = new Capture<TaskQuery>();
    scheduler.killTasks(capture(killQueryCapture), eq(USER));
    expect(scheduler.getTasks(capture(queryCapture)))
        .andReturn(ImmutableSet.of(task)).times(2);
    expect(scheduler.getTasks(capture(queryCapture)))
        .andReturn(ImmutableSet.<ScheduledTask>of());
    control.replay();

    KillResponse response = thriftInterface.killTasks(query, SESSION);
    assertEquals(ResponseCode.OK, response.getResponseCode());
    assertEquals(queryCapture.getValue(), query);
    assertEquals(killQueryCapture.getValue(), query);
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

    expectAdminAuth(false);
    expectAuth(ROLE, false);
    Capture<TaskQuery> queryCapture = new Capture<TaskQuery>();
    expect(scheduler.getTasks(capture(queryCapture)))
        .andReturn(ImmutableSet.of(task));

    control.replay();

    KillResponse response = thriftInterface.killTasks(query, SESSION);
    assertEquals(ResponseCode.AUTH_FAILED, response.getResponseCode());
    assertEquals(queryCapture.getValue(), query);
  }

  @Test
  public void testAdminKillTasks() throws Exception {
    TaskQuery query = new TaskQuery()
        .setOwner(ROLE_IDENTITY)
        .setJobName("foo_job");

    expectAdminAuth(true);
    Capture<TaskQuery> killQueryCapture = new Capture<TaskQuery>();
    scheduler.killTasks(capture(killQueryCapture), eq(USER));
    expect(scheduler.getTasks(capture(killQueryCapture)))
        .andReturn(ImmutableSet.<ScheduledTask>of());
    control.replay();

    KillResponse response = thriftInterface.killTasks(query, SESSION);
    assertEquals(ResponseCode.OK, response.getResponseCode());
    assertEquals(killQueryCapture.getValue(), query);
  }

  @Test
  public void testKillNonExistentTasks() throws Exception {
    TaskQuery query = new TaskQuery()
        .setOwner(ROLE_IDENTITY)
        .setJobName("foo_job");

    expectAdminAuth(true);

    Capture<TaskQuery> killQueryCapture = new Capture<TaskQuery>();
    scheduler.killTasks(capture(killQueryCapture), eq(USER));
    expectLastCall().andThrow(new ScheduleException("No jobs matching query"));
    control.replay();

    KillResponse response = thriftInterface.killTasks(query, SESSION);
    assertEquals(ResponseCode.INVALID_REQUEST, response.getResponseCode());
  }

  @Test
  public void testSetQuota() throws Exception {
    Quota quota = new Quota()
        .setNumCpus(10)
        .setDiskMb(100)
        .setRamMb(200);
    expectAdminAuth(true);
    quotaManager.setQuota(ROLE, quota);

    control.replay();

    SetQuotaResponse response = thriftInterface.setQuota(ROLE, quota, SESSION);
    assertEquals(ResponseCode.OK, response.getResponseCode());
  }

  @Test
  public void testSetQuotaAuthFailure() throws Exception {
    Quota quota = new Quota()
        .setNumCpus(10)
        .setDiskMb(100)
        .setRamMb(200);
    expectAdminAuth(false);

    control.replay();

    SetQuotaResponse response = thriftInterface.setQuota(ROLE, quota, SESSION);
    assertEquals(ResponseCode.AUTH_FAILED, response.getResponseCode());
  }

  @Test
  public void testForceTaskState() throws Exception {
    String taskId = "task_id_foo";
    ScheduleStatus status = ScheduleStatus.FAILED;

    scheduler.setTaskStatus(Query.byId(taskId), status, transitionMessage(SESSION.getUser()));
    expectAdminAuth(true);

    control.replay();

    ForceTaskStateResponse response =
        thriftInterface.forceTaskState(taskId, status, SESSION);
    assertEquals(ResponseCode.OK, response.getResponseCode());
  }

  @Test
  public void testForceTaskStateAuthFailure() throws Exception {
    expectAdminAuth(false);

    control.replay();

    ForceTaskStateResponse response =
        thriftInterface.forceTaskState("task", ScheduleStatus.FAILED, SESSION);
    assertEquals(ResponseCode.AUTH_FAILED, response.getResponseCode());
  }

  private JobConfiguration makeJob() {
    TwitterTaskInfo task = new TwitterTaskInfo();

    JobConfiguration job = new JobConfiguration()
        .setName("foo")
        .setOwner(ROLE_IDENTITY);
    job.addToTaskConfigs(task);
    return job;
  }

  private void expectAuth(String role, boolean allowed) throws AuthFailedException {
    sessionValidator.checkAuthenticated(SESSION, role);
    if (!allowed) {
      expectLastCall().andThrow(new AuthFailedException("Denied!"));
    }
  }

  private void expectAdminAuth(boolean allowed) throws AuthFailedException {
    expectAuth(SchedulerThriftInterface.ADMIN_ROLE.get(), allowed);
  }
}

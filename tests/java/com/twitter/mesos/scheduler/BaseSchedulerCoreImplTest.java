package com.twitter.mesos.scheduler;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.twitter.mesos.gen.LimitConstraint;
import org.apache.mesos.Protos.Attribute;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskDescription;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.Protos.Value.Ranges;
import org.apache.mesos.Protos.Value.Scalar;
import org.apache.mesos.Protos.Value.Text;
import org.apache.mesos.Protos.Value.Type;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.base.Closure;
import com.twitter.common.collections.Pair;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.testing.TearDownRegistry;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Constraint;
import com.twitter.mesos.gen.CronCollisionPolicy;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskConstraint;
import com.twitter.mesos.gen.TaskEvent;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.UpdateResult;
import com.twitter.mesos.gen.ValueConstraint;
import com.twitter.mesos.scheduler.SchedulerCore.RestartException;
import com.twitter.mesos.scheduler.SchedulingFilter.Veto;
import com.twitter.mesos.scheduler.StateManagerVars.MutableState;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import com.twitter.mesos.scheduler.quota.QuotaManager;
import com.twitter.mesos.scheduler.quota.QuotaManager.QuotaManagerImpl;
import com.twitter.mesos.scheduler.quota.Quotas;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.Storage.Work.NoResult;

import static com.twitter.mesos.gen.ScheduleStatus.ASSIGNED;
import static com.twitter.mesos.gen.ScheduleStatus.FAILED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLING;
import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static com.twitter.mesos.gen.ScheduleStatus.STARTING;
import static com.twitter.mesos.gen.ScheduleStatus.UPDATING;
import static com.twitter.mesos.gen.UpdateResult.SUCCESS;
import static com.twitter.mesos.scheduler.configuration.ConfigurationManager.*;
import static org.easymock.EasyMock.expectLastCall;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Base integration test for the SchedulerCoreImpl, subclasses should supply a concrete Storage
 * system.
 *
 * TODO(William Farner): Test all the different cases for setTaskStaus:
 *    - Killed tasks get removed.
 *    - Failed tasks have failed count incremented.
 *    - Tasks above maxTaskFailures have _all_ tasks in the job removed.
 *    - Daemon tasks are rescheduled.
 *
 * TODO(William Farner): Add test cases for when the storage has pre-loaded task data.
 *
 * @author William Farner
 */
public abstract class BaseSchedulerCoreImplTest extends EasyMockTest {

  private static final String FRAMEWORK_ID = "FrameworkId";

  private static final Identity OWNER_A = new Identity("Test_Role_A", "Test_User_A");
  private static final String JOB_A = "Test_Job_A";
  private static final String JOB_A_KEY = Tasks.jobKey(OWNER_A, JOB_A);
  private static final int ONE_GB = 1024;
  private static final int FOUR_GB = 4096;
  private static final Quota DEFAULT_TASK_QUOTA = new Quota(1.0, ONE_GB, ONE_GB);
  private static final int DEFAULT_TASKS_IN_QUOTA = 10;

  private static final Identity OWNER_B = new Identity("Test_Role_B", "Test_User_B");
  private static final String JOB_B = "Test_Job_B";

  private static final SlaveID SLAVE_ID = SlaveID.newBuilder().setValue("SlaveId").build();
  private static final String SLAVE_HOST_1 = "SlaveHost1";

  private static final OfferID OFFER_ID = OfferID.newBuilder().setValue("OfferId").build();

  private Driver driver;
  private StateManager stateManager;
  private SchedulerCoreImpl scheduler;
  private CronJobManager cron;
  private QuotaManager quotaManager;
  private FakeClock clock;

  @Before
  public void setUp() throws Exception {
    driver = createMock(Driver.class);
    clock = new FakeClock();
  }

  /**
   * Subclasses should create the {@code Storage} implementation to be used by the
   * {@link SchedulerCoreImpl} under test.
   *
   * @return the {@code Storage} to for the SchedulerCoreImpl to use under tests
   * @throws Exception if there is a problem creating the storage implementation
   */
  protected abstract Storage createStorage() throws Exception;

  private void buildScheduler() throws Exception {
    buildScheduler(createStorage());
  }

  private static Quota scale(Quota quota, int factor) {
    return new Quota()
        .setNumCpus(quota.getNumCpus() * factor)
        .setRamMb(quota.getRamMb() * factor)
        .setDiskMb(quota.getDiskMb() * factor);
  }

  private void buildScheduler(Storage storage) throws Exception {
    ImmediateJobManager immediateManager = new ImmediateJobManager();
    cron = new CronJobManager(storage, new TearDownRegistry(this));
    stateManager = new StateManager(storage, clock, new MutableState(), driver);
    quotaManager = new QuotaManagerImpl(storage);
    SchedulingFilter schedulingFilter =
        new SchedulingFilterImpl(ImmutableMap.<String, String>of(), storage);
    scheduler = new SchedulerCoreImpl(cron, immediateManager, stateManager, schedulingFilter,
        quotaManager);
    cron.schedulerCore = scheduler;
    immediateManager.schedulerCore = scheduler;
    scheduler.prepare();
    scheduler.initialize();
    scheduler.start();

    // Apply a default quota for users so we don't have to give quota for every test.
    quotaManager.setQuota(OWNER_A.getRole(), scale(DEFAULT_TASK_QUOTA, DEFAULT_TASKS_IN_QUOTA));
    quotaManager.setQuota(OWNER_B.getRole(), scale(DEFAULT_TASK_QUOTA, DEFAULT_TASKS_IN_QUOTA));
  }

  @Test
  public void testOfferNoTasks() throws Exception {
    control.replay();
    buildScheduler();

    Offer offer = createOffer(SLAVE_ID, SLAVE_HOST_1, 4, FOUR_GB, ONE_GB);
    assertThat(scheduler.createTask(offer), is(Optional.<TaskDescription>absent()));
  }

  @Test(expected = TaskDescriptionException.class)
  public void testCreateJobNoResources() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo task = productionTask().deepCopy();
    task.getConfiguration().remove("num_cpus");
    task.getConfiguration().remove("ram_mb");
    task.getConfiguration().remove("disk_mb");

    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 1);
    scheduler.createJob(job);
  }

  @Test(expected = ScheduleException.class)
  public void testCreateJobNoQuota() throws Exception {
    control.replay();
    buildScheduler();

    quotaManager.setQuota(OWNER_A.getRole(), Quotas.NO_QUOTA);

    JobConfiguration job = makeJob(OWNER_A, JOB_A, 1);
    scheduler.createJob(job);
  }

  @Test
  public void testCreateNonproductionJobNoQuota() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo task = nonProductionTask();
    scheduler.createJob(makeJob(OWNER_A, JOB_A, task, 100));
    assertThat(getTasks(Query.byRole(OWNER_A.getRole())).size(), is(100));
  }

  @Test(expected = ScheduleException.class)
  public void testCreateJobExceedsQuota() throws Exception {
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, DEFAULT_TASKS_IN_QUOTA + 1);
    scheduler.createJob(job);
  }

  @Test(expected = TaskDescriptionException.class)
  public void testCreateJobExceedsTaskLimit() throws Exception {
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, nonProductionTask(),
        MAX_TASKS_PER_JOB.get() + 1);
    scheduler.createJob(job);
  }

  @Test(expected = TaskDescriptionException.class)
  public void testUpdateJobExceedsTaskLimit() throws Exception {
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, nonProductionTask(),
        MAX_TASKS_PER_JOB.get());
    scheduler.createJob(job);

    job = makeJob(OWNER_A, JOB_A, nonProductionTask(),
        MAX_TASKS_PER_JOB.get() + 1);
    scheduler.startUpdate(job);
  }

  @Test
  public void testCreateJob() throws Exception {
    int numTasks = 10;

    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, numTasks);
    scheduler.createJob(job);
    assertTaskCount(numTasks);

    Set<ScheduledTask> tasks = scheduler.getTasks(queryJob(OWNER_A, JOB_A));
    assertThat(tasks.size(), is(numTasks));
    for (ScheduledTask state : tasks) {
      assertThat(state.getStatus(), is(PENDING));
      assertThat(state.getAssignedTask().isSetTaskId(), is(true));
      assertThat(state.getAssignedTask().isSetSlaveId(), is(false));
      // Need to clear shard ID since that was assigned in our makeJob function.
      assertThat(state.getAssignedTask().getTask().setShardId(0),
          is(populateFields(job,
              productionTask().setShardId(0))));
    }
  }

  private static Constraint dedicatedConstraint(Set<String> values) {
    return new Constraint(DEDICATED_ATTRIBUTE,
        TaskConstraint.value(new ValueConstraint(false, values)));
  }

  private static Constraint dedicatedConstraint(int value) {
    return new Constraint(DEDICATED_ATTRIBUTE, TaskConstraint.limit(new LimitConstraint(value)));
  }

  @Test
  public void testDedicatedJob() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo newTask = nonProductionTask();
    newTask.addToConstraints(dedicatedConstraint(ImmutableSet.of(OWNER_A.getRole())));
    scheduler.createJob(makeJob(OWNER_A, JOB_A, ImmutableSet.of(newTask)));

    assertThat(getOnlyTask(queryJob(OWNER_A, JOB_A)).getStatus(), is(PENDING));
  }

  @Test
  public void testDedicatedJobKey() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo newTask = nonProductionTask();
    newTask.addToConstraints(dedicatedConstraint(ImmutableSet.of(Tasks.jobKey(OWNER_A, JOB_A))));
    scheduler.createJob(makeJob(OWNER_A, JOB_A, ImmutableSet.of(newTask)));

    assertThat(getOnlyTask(queryJob(OWNER_A, JOB_A)).getStatus(), is(PENDING));
  }

  @Test(expected = ScheduleException.class)
  public void testLimitConstraintForDedicatedJob() throws Exception {
    control.replay();
    buildScheduler();
    TwitterTaskInfo task = nonProductionTask();
    task.addToConstraints(dedicatedConstraint(1));
    scheduler.createJob(makeJob(OWNER_A, JOB_A, ImmutableSet.of(task)));
  }

  @Test(expected = ScheduleException.class)
  public void testMultipleValueConstraintForDedicatedJob() throws Exception {
    control.replay();
    buildScheduler();
    TwitterTaskInfo task = nonProductionTask();
    task.addToConstraints(dedicatedConstraint(ImmutableSet.of("mesos", "test")));
    scheduler.createJob(makeJob(OWNER_A, JOB_A, ImmutableSet.of(task)));
  }

  @Test(expected = ScheduleException.class)
  public void testUnauthorizedDedicatedJob() throws Exception {
    control.replay();
    buildScheduler();
    TwitterTaskInfo task = nonProductionTask();
    task.addToConstraints(dedicatedConstraint(ImmutableSet.of("mesos")));
    scheduler.createJob(makeJob(OWNER_A, JOB_A, ImmutableSet.of(task)));
  }

  @Test
  public void testLoadTasksFromStorage() throws Exception {
    final String storedTaskId = "task_on_disk";

    control.replay();

    Storage storage = createStorage();

    storage.start(Work.NOOP);

    final TwitterTaskInfo storedTask = new TwitterTaskInfo()
        .setOwner(OWNER_A)
        .setJobName(JOB_A)
        .setNumCpus(1.0)
        .setRamMb(ONE_GB)
        .setDiskMb(500)
        .setShardId(0)
        .setStartCommand("ls")
        .setRequestedPorts(ImmutableSet.<String>of())
        .setAvoidJobs(ImmutableSet.<String>of())
        .setConstraints(ImmutableSet.<Constraint>of());

    storage.doInTransaction(new NoResult.Quiet() {
      @Override protected void execute(Storage.StoreProvider storeProvider) {
        storeProvider.getTaskStore().saveTasks(ImmutableSet.of(new ScheduledTask()
            .setStatus(PENDING)
            .setAssignedTask(
                new AssignedTask()
                    .setTaskId(storedTaskId)
                    .setTask(storedTask))));
      }
    });

    buildScheduler(storage);

    // Check that the missing event was synthesized.
    assertThat(Iterables.getLast(getTask(storedTaskId).getTaskEvents()).getStatus(), is(PENDING));

    Offer offer = createOffer(SLAVE_ID, SLAVE_HOST_1, 4, FOUR_GB, ONE_GB);
    Optional<TaskDescription> launchedTask = scheduler.createTask(offer);

    // Since task fields are backfilled with defaults, the production flag and thermos config
    // should be filled.
    assertThat(extractTask(launchedTask).getTask(),
        is(new TwitterTaskInfo(storedTask).setProduction(false).setThermosConfig(new byte[] {})));

    assertThat(getTask(storedTaskId).getStatus(), is(ASSIGNED));
  }

  private AssignedTask extractTask(Optional<TaskDescription> task) throws CodingException {
    assertTrue(task.isPresent());
    return ThriftBinaryCodec.decode(AssignedTask.class, task.get().getData().toByteArray());
  }

  @Test
  public void testShardUniquenessCorrection() throws Exception {
    control.replay();

    Storage storage = createStorage();

    storage.start(Work.NOOP);

    final AtomicInteger taskId = new AtomicInteger();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, 10);
    final Set<ScheduledTask> badTasks = ImmutableSet.copyOf(Iterables
        .transform(job.getTaskConfigs(),
            new Function<TwitterTaskInfo, ScheduledTask>() {
              @Override
              public ScheduledTask apply(TwitterTaskInfo task) {
                return new ScheduledTask()
                    .setStatus(RUNNING)
                    .setAssignedTask(
                        new AssignedTask()
                            .setTaskId("task-" + taskId.incrementAndGet())
                            .setTask(task.setShardId(0)));
              }
            }));

    storage.doInTransaction(new NoResult.Quiet() {
      @Override protected void execute(Storage.StoreProvider storeProvider) {
        storeProvider.getTaskStore().saveTasks(badTasks);
      }
    });

    buildScheduler(storage);

    assertThat(getTasks(Query.byStatus(RUNNING)).size(), is(1));
    assertThat(getTasks(Query.byStatus(KILLED)).size(), is(9));
  }

  @Test
  public void testBackfillRequestedPorts() throws Exception {
    final String storedTaskId = "task_on_disk";

    control.replay();

    Storage storage = createStorage();

    storage.start(Work.NOOP);

    final TwitterTaskInfo storedTask = new TwitterTaskInfo()
        .setOwner(OWNER_A)
        .setJobName(JOB_A)
        .setNumCpus(1.0)
        .setRamMb(ONE_GB)
        .setDiskMb(500)
        .setShardId(0)
        .setStartCommand("ls %port:foo%")
        .setAvoidJobs(ImmutableSet.<String>of());

    storage.doInTransaction(new NoResult.Quiet() {
      @Override
      protected void execute(Storage.StoreProvider storeProvider) {
        storeProvider.getTaskStore().saveTasks(ImmutableSet.of(new ScheduledTask()
            .setStatus(PENDING)
            .setAssignedTask(
                new AssignedTask()
                    .setTaskId(storedTaskId)
                    .setTask(storedTask))));
      }
    });

    buildScheduler(storage);

    Offer offer =
        createOffer(SLAVE_ID, SLAVE_HOST_1, 4, FOUR_GB, ONE_GB, ImmutableSet.of(Pair.of(80, 81)));
    Optional<TaskDescription> launchedTask = scheduler.createTask(offer);

    assertEquals(ImmutableSet.of("foo"), extractTask(launchedTask).getTask().getRequestedPorts());
  }

  @Test
  public void testBackfillRequestedPortsForCronJob() throws Exception {

    control.replay();

    Storage storage = createStorage();

    storage.start(Work.NOOP);

    final TwitterTaskInfo storedTask = new TwitterTaskInfo()
        .setOwner(OWNER_A)
        .setJobName(JOB_A)
        .setNumCpus(1.0)
        .setRamMb(ONE_GB)
        .setDiskMb(500)
        .setShardId(0)
        .setStartCommand("ls %port:foo%")
        .setAvoidJobs(ImmutableSet.<String>of());

    storage.doInTransaction(new NoResult.Quiet() {
      @Override
      protected void execute(Storage.StoreProvider storeProvider) {
        storeProvider.getJobStore().saveAcceptedJob(
            CronJobManager.MANAGER_KEY, makeJob(OWNER_A, JOB_A, storedTask, 1)
            .setCronSchedule("1 1 1 1 1"));
      }
    });

    buildScheduler(storage);

    assertTaskCount(0);

    scheduler.startCronJob(OWNER_A.getRole(), JOB_A);

    assertEquals(ImmutableSet.of("foo"),
        getOnlyTask(queryJob(OWNER_A, JOB_A)).getAssignedTask().getTask().getRequestedPorts());
  }

  @Test
  public void testCreateJobNoHdfs() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo task = new TwitterTaskInfo().setConfiguration(
        ImmutableMap.<String, String> builder()
            .put("start_command", "date")
            .put("num_cpus", "1.0")
            .put("ram_mb", "1024")
            .put("disk_mb", "1024")
            .build());

    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 1);
    scheduler.createJob(job);
    assertTaskCount(1);

    TwitterTaskInfo task2 = new TwitterTaskInfo().setConfiguration(
        ImmutableMap.<String, String>builder()
            .put("start_command", "date")
            .put("hdfs_path", "")
            .put("num_cpus", "1.0")
            .put("ram_mb", "1024")
            .put("disk_mb", "1024")
            .build());
    JobConfiguration job2 = makeJob(OWNER_A, JOB_B, task2, 1);
    scheduler.createJob(job2);
    assertTaskCount(2);
  }

  @Test
  public void testRejectsBadIdentifiers() throws Exception {
    control.replay();
    buildScheduler();

    Identity validIdentity = new Identity("foo", "bar");
    Identity[] invalidIdentities = {
      new Identity().setRole("foo"),
      new Identity("foo/", "bar"),
      new Identity("foo", "&bar"),
      new Identity().setUser("bar")
    };

    String validJob = "baz";
    String[] invalidJobs = { "&baz", "/baz", "baz&", "" };

    for (Identity ident : invalidIdentities) {
      for (String job : invalidJobs) {
        expectRejected(ident, job);
      }
    }

    for (String job : invalidJobs) {
      expectRejected(validIdentity, job);
    }

    for (Identity ident : invalidIdentities) {
      expectRejected(ident, validJob);
    }
  }

  private void expectRejected(Identity owner, String jobName) throws ScheduleException {
    try {
      scheduler.createJob(makeJob(owner, jobName, 1));
      fail("Job owner/name should have been rejected.");
    } catch (TaskDescriptionException e) {
      // Expected.
    }
  }

  @Test
  public void testSortableTaskIds() throws Exception {
    control.replay();
    buildScheduler();

    for (ScheduledTask task : getTasks(Query.GET_ALL)) {
      assertThat(task.getAssignedTask().getTask().getOwner(), is(OWNER_A));
    }
  }

  @Test(expected = ScheduleException.class)
  public void testCreateDuplicateJob() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
    assertTaskCount(1);

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
  }

  @Test(expected = ScheduleException.class)
  public void testCreateDuplicateCronJob() throws Exception {
    control.replay();
    buildScheduler();

    // Cron jobs are scheduled on a delay, so this job's tasks will not be scheduled immediately,
    // but duplicate jobs should still be rejected.
    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1)
        .setCronSchedule("1 1 1 1 1"));
    assertTaskCount(0);

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
  }

  @Test
  public void testStartCronJob() throws Exception {
    // Create a cron job, ask the scheduler to start it, and ensure that the tasks exist
    // in the PENDING state.
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1)
        .setCronSchedule("1 1 1 1 1"));
    assertTaskCount(0);

    scheduler.startCronJob(OWNER_A.getRole(), JOB_A);
    assertThat(getOnlyTask(queryJob(OWNER_A, JOB_A)).getStatus(), is(PENDING));
  }

  @Test(expected = ScheduleException.class)
  public void testStartNonexistentCronJob() throws Exception {
    // Try to start a cron job that doesn't exist.
    control.replay();
    buildScheduler();

    scheduler.startCronJob(OWNER_A.getRole(), JOB_A);
  }

  @Test
  public void testStartNonCronJob() throws Exception {
    // Create a NON cron job and try to start it as though it were a cron job, and ensure that
    // no cron tasks are created.
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
    String taskId = Tasks.id(getOnlyTask(queryJob(OWNER_A,JOB_A)));

    try {
      scheduler.startCronJob(OWNER_A.getRole(), JOB_A);
      fail("Start should have failed.");
    } catch (ScheduleException e) {
      // Expected.
    }

    assertThat(getTask(taskId).getStatus(), is(PENDING));
    assertThat(cron.hasJob(JOB_A_KEY), is(false));
  }

  @Test(expected = ScheduleException.class)
  public void testStartNonOwnedCronJob() throws Exception {
    // Try to start a cron job that is not owned by us.
    // Should throw an exception.
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1)
        .setCronSchedule("1 1 1 1 1"));
    assertTaskCount(0);

    scheduler.startCronJob(OWNER_B.getRole(), JOB_A);
  }

  @Test
  public void testStartRunningCronJob() throws Exception {
    // Start a cron job that is already started by an earlier
    // call and is PENDING. Make sure it follows the cron collision policy.
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, 1)
        .setCronSchedule("1 1 1 1 1")
        .setCronCollisionPolicy(CronCollisionPolicy.KILL_EXISTING);

    scheduler.createJob(job);
    assertTaskCount(0);
    assertThat(cron.hasJob(JOB_A_KEY), is(true));

    cron.cronTriggered(job);
    assertTaskCount(1);

    String taskId = Tasks.id(getOnlyTask(queryJob(OWNER_A,JOB_A)));

    // Now start the same cron job immediately.
    scheduler.startCronJob(OWNER_A.getRole(), JOB_A);
    assertTaskCount(1);
    assertThat(getOnlyTask(queryJob(OWNER_A, JOB_A)).getStatus(), is(PENDING));

    // Make sure the pending job is the new one.
    String newTaskId = Tasks.id(getOnlyTask(queryJob(OWNER_A,JOB_A)));
    assertThat(taskId.equals(newTaskId), is(false));
  }

  @Test(expected = TaskDescriptionException.class)
  public void testCreateEmptyJob() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(new JobConfiguration().setOwner(OWNER_A).setName(JOB_A));
  }

  @Test(expected = TaskDescriptionException.class)
  public void testRejectsMixedProductionMode() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo nonProduction = nonProductionTask();
    TwitterTaskInfo production = productionTask();
    scheduler.createJob(makeJob(OWNER_A, JOB_A,
        ImmutableList.of(nonProduction, production)));
  }

  @Test(expected = TaskDescriptionException.class)
  public void testCreateJobMissingShardIds() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(new JobConfiguration().setOwner(OWNER_A).setName(JOB_A).setTaskConfigs(
        ImmutableSet.of(productionTask())));
  }

  @Test(expected = TaskDescriptionException.class)
  public void testCreateJobDuplicateShardIds() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(new JobConfiguration().setOwner(OWNER_A).setName(JOB_A).setTaskConfigs(
        ImmutableSet.of(
            productionTask().setShardId(0).setStartCommand("foo"),
            productionTask().setShardId(0).setStartCommand("bar"))));
  }

  @Test(expected = TaskDescriptionException.class)
  public void testCreateJobShardIdHole() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(new JobConfiguration().setOwner(OWNER_A).setName(JOB_A).setTaskConfigs(
        ImmutableSet.of(
            productionTask().setShardId(0),
            productionTask().setShardId(2))));
  }

  @Test
  public void testHonorsScheduleFilter() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 10));

    assertTaskCount(10);

    Offer offer = createOffer(SLAVE_ID, SLAVE_HOST_1, 4, FOUR_GB, 1);

    assertFalse(scheduler.createTask(offer).isPresent());
    assertFalse(scheduler.createTask(offer).isPresent());
    assertFalse(scheduler.createTask(offer).isPresent());

    // No tasks should have moved out of the pending state.
    assertThat(getTasksByStatus(PENDING).size(), is(10));
  }

  @Test
  public void testRestartTask() throws Exception {
    expectKillTask(1);

    control.replay();
    buildScheduler();

    scheduler.registered(FRAMEWORK_ID);
    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), STARTING);
    changeStatus(queryByOwner(OWNER_A), RUNNING);

    String taskId = Tasks.id(getOnlyTask(queryByOwner(OWNER_A)));

    Set<String> restartRequest = ImmutableSet.of(taskId);
    scheduler.restartTasks(restartRequest);

    // Mimick the master notifying the scheduler of a task state change.
    changeStatus(query(restartRequest), KILLED);

    assertThat(getTasks(Query.activeQuery(Tasks.jobKey(OWNER_A, JOB_A))).size(), is(1));

    ScheduledTask restartedTask = getOnlyTask(Query.byStatus(KILLED));
    assertThat(Tasks.id(restartedTask), is(taskId));

    ScheduledTask newTask = getOnlyTask(Query.byStatus(PENDING));
    assertThat(newTask.getAncestorId(), is(taskId));
    assertThat(newTask.getAssignedTask().getTask().getShardId(),
        is(restartedTask.getAssignedTask().getTask().getShardId()));
  }

  @Test
  public void testKillTask() throws Exception {
    driver.killTask(EasyMock.<String>anyObject());
    // We only expect three kills because the first test does not move out of PENDING.
    expectLastCall().times(3);

    control.replay();
    buildScheduler();

    scheduler.registered(FRAMEWORK_ID);

    for (Set<ScheduleStatus> statuses : ImmutableSet.of(
        ImmutableSet.<ScheduleStatus>of(),
        EnumSet.of(ASSIGNED),
        EnumSet.of(ASSIGNED, STARTING),
        EnumSet.of(ASSIGNED, STARTING, RUNNING))) {

      scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
      String taskId = Tasks.id(getOnlyTask(Query.activeQuery(OWNER_A, JOB_A)));

      for (ScheduleStatus status : statuses) {
        changeStatus(taskId, status);
      }

      scheduler.killTasks(queryByOwner(OWNER_A), OWNER_A.getUser());

      if (!statuses.isEmpty()) {
        // If there was no move out of the PENDING state, the task is deleted outright.
        assertThat(getTask(taskId).getStatus(), is(KILLING));
      }

      // SImulate a KILLED ack from the executor.
      changeStatus(queryByOwner(OWNER_A), KILLED);

      assertThat(getTasks(Query.activeQuery(OWNER_A, JOB_A)).size(), is(0));
    }
  }

  @Test(expected = RestartException.class)
  public void testRestartUnknownTask() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), STARTING);
    changeStatus(queryByOwner(OWNER_A), RUNNING);

    String taskId = Tasks.id(getOnlyTask(queryByOwner(OWNER_A)));

    Set<String> restartRequest = Sets.newHashSet(taskId + 1);
    scheduler.restartTasks(restartRequest);
  }

  @Test(expected = RestartException.class)
  public void testRestartInactiveTask() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), STARTING);
    changeStatus(queryByOwner(OWNER_A), RUNNING);
    changeStatus(queryByOwner(OWNER_A), FINISHED);

    String taskId = Iterables.getOnlyElement(Iterables.transform(
        scheduler.getTasks(queryByOwner(OWNER_A)), Tasks.SCHEDULED_TO_ID));
    scheduler.restartTasks(ImmutableSet.of(taskId));
  }

  @Test
  public void testDaemonTasksRescheduled() throws Exception {
    control.replay();
    buildScheduler();

    // Schedule 5 daemon and 5 non-daemon tasks.
    scheduler.createJob(makeJob(OWNER_A, JOB_A, 5));
    TwitterTaskInfo task = productionTask("daemon", "true");
    scheduler.createJob(makeJob(OWNER_A, JOB_A + "daemon", task, 5));

    assertThat(getTasksByStatus(PENDING).size(), is(10));
    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), STARTING);
    assertThat(getTasksByStatus(STARTING).size(), is(10));

    changeStatus(queryByOwner(OWNER_A), RUNNING);
    assertThat(getTasksByStatus(RUNNING).size(), is(10));

    // Daemon tasks will move back into PENDING state after finishing.
    changeStatus(queryByOwner(OWNER_A), FINISHED);
    Set<ScheduledTask> newTasks = getTasksByStatus(PENDING);
    assertThat(newTasks.size(), is(5));
    for (ScheduledTask state : newTasks) {
      assertThat(state.getAssignedTask().getTask().getShardId(),
          is(getTask(state.getAncestorId()).getAssignedTask().getTask().getShardId()));
    }

    assertThat(getTasksByStatus(FINISHED).size(), is(10));
  }

  @Test
  public void testDaemonTaskIgnoresMaxFailures() throws Exception {
    control.replay();
    buildScheduler();

    int maxFailures = 5;
    int totalFailures = 10;

    // Schedule a daemon task.
    TwitterTaskInfo task = productionTask(
        "max_task_failures", String.valueOf(maxFailures),
        "daemon", "true");
    scheduler.createJob(makeJob(OWNER_A, JOB_A, task, 1));
    assertTaskCount(1);

    // Fail the task more than maxFailures.
    for (int i = 1; i <= totalFailures; i++) {
      String taskId = Tasks.id(getOnlyTask(Query.activeQuery(OWNER_A, JOB_A)));

      changeStatus(taskId, ASSIGNED);
      changeStatus(taskId, STARTING);
      changeStatus(taskId, RUNNING);
      assertThat(getTask(taskId).getFailureCount(), is(i - 1));
      changeStatus(taskId, FAILED);

      assertTaskCount(i + 1);
      ScheduledTask rescheduled = getOnlyTask(Query.byStatus(PENDING));
      assertThat(rescheduled.getFailureCount(), is(i));
    }

    assertThat(getTasksByStatus(FAILED).size(), is(totalFailures));
    assertThat(getTasksByStatus(PENDING).size(), is(1));
  }

  @Test
  public void testTaskRescheduleOnKill() throws Exception {
    control.replay();
    buildScheduler();

    // Create 5 non-daemon and 5 daemon tasks.
    scheduler.createJob(makeJob(OWNER_A, JOB_A, 5));
    TwitterTaskInfo task = productionTask("daemon", "true");
    scheduler.createJob(makeJob(OWNER_A, JOB_A + "daemon", task, 5));

    assertThat(getTasksByStatus(PENDING).size(), is(10));
    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), STARTING);
    assertThat(getTasksByStatus(STARTING).size(), is(10));
    changeStatus(queryByOwner(OWNER_A), RUNNING);
    assertThat(getTasksByStatus(RUNNING).size(), is(10));

    // All tasks will move back into PENDING state after getting KILLED.
    changeStatus(queryByOwner(OWNER_A), KILLED);
    Set<ScheduledTask> newTasks = getTasksByStatus(PENDING);
    assertThat(newTasks.size(), is(10));
    assertThat(getTasksByStatus(KILLED).size(), is(10));
  }

  @Test
  public void testNoTransitionFromTerminalState() throws Exception {
    expectKillTask(1);

    control.replay();
    buildScheduler();

    scheduler.registered(FRAMEWORK_ID);
    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), STARTING);
    changeStatus(queryByOwner(OWNER_A), RUNNING);
    scheduler.killTasks(queryByOwner(OWNER_A), OWNER_A.getUser());
    changeStatus(queryByOwner(OWNER_A), KILLED);

    String taskId = Tasks.id(getOnlyTask(queryByOwner(OWNER_A)));

    // This transition should be rejected.
    changeStatus(queryByOwner(OWNER_A), LOST);

    assertThat(getTask(taskId).getStatus(), is(KILLED));
  }

  @Test
  public void testFailedTaskIncrementsFailureCount() throws Exception {
    int maxFailures = 5;
    control.replay();
    buildScheduler();

    TwitterTaskInfo task = productionTask("max_task_failures", String.valueOf(maxFailures));
    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 1);
    scheduler.createJob(job);
    assertTaskCount(1);

    Set<ScheduledTask> tasks = scheduler.getTasks(queryJob(OWNER_A, JOB_A));
    assertThat(tasks.size(), is(1));

    for (int i = 1; i <= maxFailures; i++) {
      String taskId = Tasks.id(getOnlyTask(Query.activeQuery(OWNER_A, JOB_A)));

      changeStatus(taskId, ASSIGNED);
      changeStatus(taskId, STARTING);
      changeStatus(taskId, RUNNING);
      assertThat(getTask(taskId).getFailureCount(), is(i - 1));
      changeStatus(taskId, FAILED);

      if (i != maxFailures) {
        assertTaskCount(i + 1);
        ScheduledTask rescheduled = getOnlyTask(Query.byStatus(PENDING));
        assertThat(rescheduled.getFailureCount(), is(i));
      } else {
        assertTaskCount(maxFailures);
      }
    }

    assertThat(getTasksByStatus(FAILED).size(), is(maxFailures));
    assertThat(getTasksByStatus(PENDING).size(), is(0));
  }

  @Test
  public void testCronJobLifeCycle() throws Exception {
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, 10);
    job.setCronSchedule("1 1 1 1 1");
    scheduler.createJob(job);
    assertTaskCount(0);
    assertThat(cron.hasJob(JOB_A_KEY), is(true));

    // Simulate a triggering of the cron job.
    cron.cronTriggered(job);
    assertTaskCount(10);
    assertThat(getTasks(new Query(new TaskQuery()
        .setOwner(OWNER_A).setJobName(JOB_A).setStatuses(Sets.newHashSet(PENDING)))).size(),
        is(10));

    assertTaskCount(10);

    changeStatus(queryByOwner(OWNER_A), ASSIGNED);
    changeStatus(queryByOwner(OWNER_A), STARTING);
    assertTaskCount(10);
    changeStatus(queryByOwner(OWNER_A), RUNNING);
    assertTaskCount(10);
    changeStatus(queryByOwner(OWNER_A), FINISHED);
  }

  @Test
  public void testCronNoSuicide() throws Exception {
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, 10);
    job.setCronSchedule("1 1 1 1 1")
        .setCronCollisionPolicy(CronCollisionPolicy.KILL_EXISTING);
    scheduler.createJob(job);
    assertTaskCount(0);

    try {
      scheduler.createJob(job);
      fail();
    } catch (ScheduleException e) {
      // Expected.
    }
    assertThat(cron.hasJob(JOB_A_KEY), is(true));

    // Simulate a triggering of the cron job.
    cron.cronTriggered(job);
    assertTaskCount(10);

    Set<String> taskIds = ImmutableSet.copyOf(Iterables.transform(getTasksOwnedBy(OWNER_A),
        Tasks.SCHEDULED_TO_ID));

    // Simulate a triggering of the cron job.
    cron.cronTriggered(job);
    assertTaskCount(10);

    Set<String> newTaskIds = ImmutableSet.copyOf(Iterables.transform(getTasksOwnedBy(OWNER_A),
        Tasks.SCHEDULED_TO_ID));

    assertThat(Sets.intersection(taskIds, newTaskIds).isEmpty(), is(true));

    try {
      scheduler.createJob(job);
      fail();
    } catch (ScheduleException e) {
      // Expected.
    }
    assertThat(cron.hasJob(JOB_A_KEY), is(true));
  }

  @Test
  public void testKillPendingTask() throws Exception {
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, 1);
    scheduler.createJob(job);
    assertTaskCount(1);

    Set<ScheduledTask> tasks = scheduler.getTasks(queryJob(OWNER_A, JOB_A));
    assertThat(tasks.size(), is(1));

    String taskId = Tasks.id(Iterables.get(tasks, 0));

    scheduler.killTasks(Query.byId(taskId), OWNER_A.getUser());
    assertTaskCount(0);
  }

  @Test
  public void testKillRunningTask() throws Exception {
    expectKillTask(1);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
    String taskId = Tasks.id(getOnlyTask(queryByOwner(OWNER_A)));
    changeStatus(taskId, ASSIGNED);
    changeStatus(taskId, STARTING);
    changeStatus(taskId, RUNNING);
    scheduler.killTasks(query(taskId), OWNER_A.getUser());
    assertThat(getTask(taskId).getStatus(), is(KILLING));
    assertThat(getTasks(queryByOwner(OWNER_A)).size(), is(1));
    changeStatus(taskId, KILLED);
    assertThat(getTask(taskId).getStatus(), is(KILLED));
  }

  @Test
  public void testKillCronTask() throws Exception {
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, 1);
    job.setCronSchedule("1 1 1 1 1");
    scheduler.createJob(job);

    // This will fail if the cron task could not be found.
    scheduler.killTasks(queryJob(OWNER_A, JOB_A), OWNER_A.getUser());
  }

  @Test
  public void testLostTaskRescheduled() throws Exception {
    control.replay();
    buildScheduler();

    int maxFailures = 5;
    TwitterTaskInfo task = productionTask("max_task_failures", String.valueOf(maxFailures));
    JobConfiguration job = makeJob(OWNER_A, JOB_A, task, 1);
    scheduler.createJob(job);
    assertTaskCount(1);

    Set<ScheduledTask> tasks = scheduler.getTasks(queryJob(OWNER_A, JOB_A));
    assertThat(tasks.size(), is(1));

    changeStatus(Query.byStatus(PENDING), ASSIGNED);

    Query pendingQuery = Query.byStatus(PENDING);
    changeStatus(Query.byStatus(ASSIGNED), LOST);
    assertThat(getOnlyTask(pendingQuery).getStatus(), is(PENDING));
    assertTaskCount(2);

    changeStatus(Query.byStatus(PENDING), ASSIGNED);
    changeStatus(Query.byStatus(ASSIGNED), LOST);
    assertThat(getOnlyTask(pendingQuery).getStatus(), is(PENDING));
    assertTaskCount(3);
  }

  @Test
  public void testKillJob() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 10));
    assertTaskCount(10);

    scheduler.killTasks(queryJob(OWNER_A, JOB_A), OWNER_A.getUser());
    assertTaskCount(0);
  }

  @Test
  public void testKillJob2() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 5));
    assertTaskCount(5);

    scheduler.createJob(makeJob(OWNER_A, JOB_A + "2", 5));
    assertTaskCount(10);

    scheduler.killTasks(queryJob(OWNER_A, JOB_A + "2"), OWNER_A.getUser());
    assertTaskCount(5);

    for (ScheduledTask state : scheduler.getTasks(Query.GET_ALL)) {
      assertThat(state.getAssignedTask().getTask().getJobName(), is(JOB_A));
    }
  }

  private void sendOffer(Offer offer, String taskId, String slaveHost)
      throws Exception {
    sendOffer(offer, taskId, slaveHost, ImmutableSet.<String>of(), ImmutableSet.<Integer>of());
  }

  private void sendOffer(Offer offer, String taskId, String slaveHost,
      Set<String> portNames, Set<Integer> ports) throws Exception {
    AssignedTask task = getTask(taskId).getAssignedTask().deepCopy();

    ImmutableList.Builder<Resource> resourceBuilder = ImmutableList.<Resource>builder()
        .add(Resources.makeMesosResource(Resources.CPUS, task.getTask().getNumCpus()))
        .add(Resources.makeMesosResource(Resources.DISK_MB, task.getTask().getDiskMb()))
        .add(Resources.makeMesosResource(Resources.RAM_MB, task.getTask().getRamMb()));
    if (ports.size() > 0) {
        resourceBuilder.add(Resources.makeMesosRangeResource(Resources.PORTS, ports));
    }
    List<Resource> resources = resourceBuilder.build();

    Optional<TaskDescription> launched = scheduler.createTask(offer);
    AssignedTask assigned = extractTask(launched);

    assertThat(launched.get().getResourcesList(), is(resources));
    assertThat(assigned, is(getTask(taskId).getAssignedTask()));
    assertThat(assigned.getSlaveHost(), is(slaveHost));
    Map<String, Integer> assignedPorts = assigned.getAssignedPorts();
    assertThat(assignedPorts.keySet(), is(portNames));
    assertEquals(ports, ImmutableSet.copyOf(assignedPorts.values()));
  }

  @Test
  public void testSlaveDeletesTasks() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 2));

    String taskId1 = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 0)));
    String taskId2 = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 1)));

    Offer offer = createOffer(SLAVE_ID, SLAVE_HOST_1, 4, FOUR_GB, ONE_GB);
    sendOffer(offer, taskId1, SLAVE_HOST_1);
    sendOffer(offer, taskId2, SLAVE_HOST_1);

    changeStatus(taskId1, STARTING);
    changeStatus(taskId1, RUNNING);
    changeStatus(taskId2, STARTING);
    changeStatus(taskId2, FINISHED);

    scheduler.tasksDeleted(ImmutableSet.of(taskId1, taskId2));

    // The expected outcome is that one task is moved into the LOST state and rescheduled.
    // The FINISHED task's state is deleted.
    assertTaskCount(2);
    assertThat(getOnlyTask(Query.byId(taskId1)).getStatus(), is(LOST));
    assertThat(getTasks(Query.byId(taskId2)).size(), is(0));

    ScheduledTask rescheduled = Iterables.getOnlyElement(getTasksByStatus(PENDING));
    assertThat(rescheduled.getAncestorId(), is(taskId1));
  }

  @Test
  public void testSchedulingOrder() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo task1 = nonProductionTask("priority", "10");
    TwitterTaskInfo task2 = productionTask("priority", "0");
    TwitterTaskInfo task3 = nonProductionTask("priority", "11");

    scheduler.createJob(makeJob(OWNER_A, JOB_A, task1, 2));
    scheduler.createJob(makeJob(OWNER_B, JOB_A, task2, 2));
    scheduler.createJob(makeJob(OWNER_A, JOB_B, task3, 2));

    String taskId1a = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 0)));
    String taskId1b = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 1)));
    String taskId2a = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_B, JOB_A), 0)));
    String taskId2b = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_B, JOB_A), 1)));
    String taskId3a = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_B), 0)));
    String taskId3b = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_B), 1)));

    Offer offer = createOffer(SLAVE_ID, SLAVE_HOST_1, 4, FOUR_GB, ONE_GB);
    sendOffer(offer, taskId2a, SLAVE_HOST_1);
    sendOffer(offer, taskId2b, SLAVE_HOST_1);
    sendOffer(offer, taskId1a, SLAVE_HOST_1);
    sendOffer(offer, taskId1b, SLAVE_HOST_1);
    sendOffer(offer, taskId3a, SLAVE_HOST_1);
    sendOffer(offer, taskId3b, SLAVE_HOST_1);
  }

  @Test
  public void testStartAndFinishUpdate() throws Exception {
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, 1);
    scheduler.createJob(job);
    Optional<String> updateToken = Optional.of(scheduler.startUpdate(job));
    scheduler.finishUpdate(OWNER_A.getRole(), job.getName(), updateToken, SUCCESS);

    // If the finish update succeeded internally, we should be able to start a new update.
    scheduler.startUpdate(job);
  }

  @Test
  public void testFinishUpdateNotFound() throws Exception {
    control.replay();
    buildScheduler();

    try {
      scheduler.finishUpdate("foo", "foo", Optional.of("foo"), SUCCESS);
      fail("Call should have failed.");
    } catch (ScheduleException e) {
      // Expected.
    }

    try {
      scheduler.finishUpdate("foo", "foo", Optional.<String>absent(), SUCCESS);
      fail("Call should have failed.");
    } catch (ScheduleException e) {
      // Expected.
    }
  }

  @Test
  public void testFinishUpdateInvalidToken() throws Exception {
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, 1);
    scheduler.createJob(job);
    Optional<String> token = Optional.of(scheduler.startUpdate(job));

    try {
      scheduler.finishUpdate(OWNER_B.getRole(), job.getName(), Optional.of("foo"), SUCCESS);
      fail("Finish update should have failed.");
    } catch (ScheduleException e) {
      // expected.
    }

    scheduler.finishUpdate(OWNER_A.getRole(), job.getName(), token, SUCCESS);
  }

  @Test
  public void testRejectsSimultaneousUpdates() throws Exception {
    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, 1);
    scheduler.createJob(job);
    String token = scheduler.startUpdate(job);

    try {
      scheduler.startUpdate(job);
      fail("Second update should have failed.");
    } catch (ScheduleException e) {
      // expected.
    }

    scheduler.finishUpdate(OWNER_A.getRole(), JOB_A, Optional.of(token), SUCCESS);
  }

  private final Function<Integer, String> newCommandFactory = new Function<Integer, String>() {
    @Override public String apply(Integer shardId) {
      return "updated start command for shard " + shardId;
    }
  };

  private final Function<Integer, String> oldCommandFactory = new Function<Integer, String>() {
    @Override public String apply(Integer shardId) {
      return "start command for shard " + shardId;
    }
  };

  private void verifyUpdate(Set<ScheduledTask> tasks, JobConfiguration job,
      Closure<ScheduledTask> updatedTaskChecker) {
    Map<Integer, ScheduledTask> fetchedShards =
        Maps.uniqueIndex(tasks, Tasks.SCHEDULED_TO_SHARD_ID);
    Map<Integer, TwitterTaskInfo> originalConfigsByShard =
        Maps.uniqueIndex(job.getTaskConfigs(), Tasks.INFO_TO_SHARD_ID);
    assertThat(fetchedShards.keySet(), is(originalConfigsByShard.keySet()));
    for (ScheduledTask task : tasks) {
      updatedTaskChecker.execute(task);
    }
  }

  private abstract class UpdaterTest {
    void runTest(int numTasks, int additionalTasks) throws Exception {
      control.replay();
      buildScheduler();

      JobConfiguration job = makeJob(OWNER_A, JOB_A, productionTask().deepCopy(), numTasks);
      for (TwitterTaskInfo config : job.getTaskConfigs()) {
        config.putToConfiguration("start_command", oldCommandFactory.apply(config.getShardId()));
      }
      scheduler.createJob(job);

      JobConfiguration updatedJob =
          makeJob(OWNER_A, JOB_A, productionTask().deepCopy(), numTasks + additionalTasks);
      for (TwitterTaskInfo config : updatedJob.getTaskConfigs()) {
        config.putToConfiguration("start_command", newCommandFactory.apply(config.getShardId()));
      }
      String updateToken = scheduler.startUpdate(updatedJob);

      Set<Integer> jobShards = ImmutableSet.copyOf(Iterables.transform(
          updatedJob.getTaskConfigs(), Tasks.INFO_TO_SHARD_ID));

      UpdateResult result =
          performRegisteredUpdate(updatedJob, updateToken, jobShards, numTasks, additionalTasks);

      Set<ScheduledTask> tasks = getTasks(Query.byStatus(RUNNING));
      verify(tasks, job, updatedJob);

      scheduler.finishUpdate(OWNER_A.role, JOB_A, Optional.of(updateToken), result);
      scheduler.startUpdate(job);
    }

    abstract UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
        Set<Integer> jobShards, int numTasks, int additionalTasks) throws Exception;

    abstract void verify(Set<ScheduledTask> tasks, JobConfiguration oldJob,
        JobConfiguration updatedJob);

  }

  @Test
  public void testUpdateShards() throws Exception {
    int numTasks = 10;
    int additionalTasks = 0;
    // Kill Tasks called at RUNNING->UPDATING
    expectKillTask(numTasks);

    new UpdaterTest() {
      @Override UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
          Set<Integer> jobShards, int numTasks, int additionalTasks) throws Exception{
        changeStatus(queryByOwner(OWNER_A), ASSIGNED);
        changeStatus(queryByOwner(OWNER_A), RUNNING);

        scheduler.updateShards(OWNER_A.role, JOB_A, jobShards, updateToken);
        assertThat(getTasks(Query.byStatus(UPDATING)).size(), is(numTasks));

        changeStatus(queryByOwner(OWNER_A), FINISHED);
        changeStatus(Query.byStatus(PENDING), ASSIGNED);
        changeStatus(Query.byStatus(ASSIGNED), RUNNING);

        return SUCCESS;
      }

      @Override void verify(Set<ScheduledTask> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {
        verifyUpdate(tasks, oldJob, new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask state) {
            TwitterTaskInfo task = Tasks.SCHEDULED_TO_INFO.apply(state);
            assertThat(task.getStartCommand(), is(newCommandFactory.apply(task.getShardId())));
          }
        });
      }
    }.runTest(numTasks, additionalTasks);
  }

  @Test
  public void testRollback() throws Exception {
    int numTasks = 10;
    int additionalTasks = 0;
    // Kill Tasks called at RUNNING->UPDATING.
    expectKillTask(numTasks);

    new UpdaterTest() {
      @Override UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
          Set<Integer> jobShards, int numTasks, int additionalTask) throws Exception{
        changeStatus(queryByOwner(OWNER_A), ASSIGNED);
        changeStatus(queryByOwner(OWNER_A), RUNNING);

        scheduler.updateShards(OWNER_A.role, JOB_A, jobShards, updateToken);
        assertThat(getTasks(Query.byStatus(UPDATING)).size(), is(numTasks));

        changeStatus(queryByOwner(OWNER_A), KILLED);

        scheduler.rollbackShards(OWNER_A.role, JOB_A, jobShards, updateToken);

        changeStatus(Query.byStatus(PENDING), ASSIGNED);
        changeStatus(Query.byStatus(ASSIGNED), RUNNING);

        return UpdateResult.FAILED;
      }

      @Override void verify(Set<ScheduledTask> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {
        verifyUpdate(tasks, oldJob, new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask state) {
            TwitterTaskInfo task = Tasks.SCHEDULED_TO_INFO.apply(state);
            assertThat(task.getStartCommand(), is(oldCommandFactory.apply(task.getShardId())));
          }
        });
      }
    }.runTest(numTasks, additionalTasks);
  }

  @Test
  public void testInvalidTransition() throws Exception {
    int numTasks = 10;
    int additionalTasks = 0;
    // Kill Tasks called at RUNNING->UPDATING and UPDATING->RUNNING (Invalid).
    int expectedKillTasks = 20;
    expectKillTask(expectedKillTasks);

    new UpdaterTest() {
      @Override UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
          Set<Integer> jobShards, int numTasks, int additionalTasks) throws Exception{
        changeStatus(queryByOwner(OWNER_A), ASSIGNED);
        changeStatus(queryByOwner(OWNER_A), RUNNING);

        scheduler.updateShards(OWNER_A.role, JOB_A, jobShards, updateToken);
        assertThat(getTasks(Query.byStatus(UPDATING)).size(), is(numTasks));

        changeStatus(queryByOwner(OWNER_A), RUNNING);
        changeStatus(queryByOwner(OWNER_A), KILLED);
        changeStatus(Query.byStatus(PENDING), ASSIGNED);
        changeStatus(Query.byStatus(ASSIGNED), RUNNING);

        return SUCCESS;
      }

      @Override void verify(Set<ScheduledTask> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {
        verifyUpdate(tasks, oldJob, new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask state) {
            TwitterTaskInfo task = Tasks.SCHEDULED_TO_INFO.apply(state);
            assertThat(task.getStartCommand(), is(newCommandFactory.apply(task.getShardId())));
          }
        });
      }
    }.runTest(numTasks, additionalTasks);
  }

  @Test
  public void testPendingToUpdating() throws Exception {
    int numTasks = 10;
    int additionalTasks = 0;

    new UpdaterTest() {
      @Override UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
          Set<Integer> jobShards, int numTasks, int additionalTasks) throws Exception{
        scheduler.updateShards(OWNER_A.role, JOB_A, jobShards, updateToken);
        assertThat(getTasks(Query.byStatus(PENDING)).size(), is(numTasks));

        changeStatus(Query.byStatus(PENDING), ASSIGNED);
        changeStatus(Query.byStatus(ASSIGNED), RUNNING);

        return SUCCESS;
      }

      @Override void verify(Set<ScheduledTask> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {
        verifyUpdate(tasks, oldJob, new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask state) {
            TwitterTaskInfo task = Tasks.SCHEDULED_TO_INFO.apply(state);
            assertThat(task.getStartCommand(), is(newCommandFactory.apply(task.getShardId())));
          }
        });
      }
    }.runTest(numTasks, additionalTasks);
  }

  @Test
  public void testIncreaseShardsUpdate() throws Exception {
    int numTasks = 5;
    int additionalTasks = 5;
    // Kill Tasks called at RUNNING->UPDATING.
    expectKillTask(numTasks);

    new UpdaterTest() {
      @Override UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
          Set<Integer> jobShards, int numTasks, int additionalTasks) throws Exception{
        changeStatus(queryByOwner(OWNER_A), ASSIGNED);
        changeStatus(queryByOwner(OWNER_A), RUNNING);

        scheduler.updateShards(OWNER_A.role, JOB_A, jobShards, updateToken);

        changeStatus(Query.byStatus(UPDATING), KILLED);

        assertThat(getTasks(Query.byStatus(PENDING)).size(), is(numTasks + additionalTasks));

        changeStatus(Query.byStatus(PENDING), ASSIGNED);
        changeStatus(Query.byStatus(ASSIGNED), RUNNING);

        return SUCCESS;
      }

      @Override void verify(Set<ScheduledTask> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {
        verifyUpdate(tasks, updatedJob, new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask state) {
            TwitterTaskInfo task = Tasks.SCHEDULED_TO_INFO.apply(state);
            assertThat(task.getStartCommand(), is(newCommandFactory.apply(task.getShardId())));
          }
        });
      }
    }.runTest(numTasks, additionalTasks);
  }

  @Test
  public void testDecreaseShardsUpdate() throws Exception {
    int numTasks = 10;
    int additionalTasks = -5;
    // Kill Tasks called at RUNNING->UPDATING.
    int expectedKillTasks = 10;
    expectKillTask(expectedKillTasks);

    new UpdaterTest() {
      @Override UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
          Set<Integer> jobShards, int numTasks, int additionalTasks) throws Exception{
        changeStatus(queryByOwner(OWNER_A), ASSIGNED);
        changeStatus(queryByOwner(OWNER_A), RUNNING);

        scheduler.updateShards(OWNER_A.role, JOB_A, jobShards, updateToken);

        changeStatus(Query.byStatus(UPDATING), KILLED);

        assertThat(getTasks(Query.byStatus(PENDING)).size(), is(numTasks + additionalTasks));

        changeStatus(Query.byStatus(RUNNING), KILLING);
        changeStatus(Query.byStatus(PENDING), ASSIGNED);
        changeStatus(Query.byStatus(ASSIGNED), RUNNING);

        return SUCCESS;
      }

      @Override void verify(Set<ScheduledTask> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {
        verifyUpdate(tasks, updatedJob, new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask state) {
            TwitterTaskInfo task = Tasks.SCHEDULED_TO_INFO.apply(state);
            assertThat(task.getStartCommand(), is(newCommandFactory.apply(task.getShardId())));
          }
        });
      }
    }.runTest(numTasks, additionalTasks);
  }

  @Test
  public void testIncreaseShardsRollback() throws Exception {
    int numTasks = 5;
    int additionalTasks = 5;
    // Kill Tasks called at RUNNING->UPDATING.
    expectKillTask(numTasks);

    new UpdaterTest() {
      @Override UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
          Set<Integer> jobShards, int numTasks, int additionalTasks) throws Exception{
        changeStatus(queryByOwner(OWNER_A), ASSIGNED);
        changeStatus(queryByOwner(OWNER_A), RUNNING);

        scheduler.updateShards(OWNER_A.role, JOB_A, jobShards, updateToken);

        changeStatus(Query.byStatus(UPDATING), KILLED);

        assertThat(getTasks(Query.byStatus(PENDING)).size(), is(numTasks + additionalTasks));

        scheduler.rollbackShards(OWNER_A.role, JOB_A, jobShards, updateToken);

        changeStatus(Query.byStatus(PENDING), ASSIGNED);
        changeStatus(Query.byStatus(ASSIGNED), RUNNING);

        return UpdateResult.FAILED;
      }

      @Override void verify(Set<ScheduledTask> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {
        verifyUpdate(tasks, oldJob, new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask state) {
            TwitterTaskInfo task = Tasks.SCHEDULED_TO_INFO.apply(state);
            assertThat(task.getStartCommand(), is(oldCommandFactory.apply(task.getShardId())));
          }
        });
      }
    }.runTest(numTasks, additionalTasks);
  }

  @Test(expected = ScheduleException.class)
  public void testIncreaseShardsExceedsQuota() throws Exception {
    int numTasks = 10;
    int additionalTasks = 1;

    control.replay();
    buildScheduler();

    JobConfiguration job = makeJob(OWNER_A, JOB_A, productionTask().deepCopy(), numTasks);
    for (TwitterTaskInfo config : job.getTaskConfigs()) {
      config.putToConfiguration("start_command", oldCommandFactory.apply(config.getShardId()));
    }
    scheduler.createJob(job);

    JobConfiguration updatedJob =
        makeJob(OWNER_A, JOB_A, productionTask().deepCopy(), numTasks + additionalTasks);
    for (TwitterTaskInfo config : updatedJob.getTaskConfigs()) {
      config.putToConfiguration("start_command", newCommandFactory.apply(config.getShardId()));
    }
    scheduler.startUpdate(updatedJob);
  }

  @Test
  public void testDecreaseShardsRollback() throws Exception {
    int numTasks = 10;
    int additionalTasks = -5;
    // Kill Tasks called at RUNNING->UPDATING and PENDING->ROLLBACK
    int expectedKillTasks = 5;
    expectKillTask(expectedKillTasks);

    new UpdaterTest() {
      @Override UpdateResult performRegisteredUpdate(JobConfiguration job, String updateToken,
          Set<Integer> jobShards, int numTasks, int additionalTasks) throws Exception{
        changeStatus(queryByOwner(OWNER_A), ASSIGNED);
        changeStatus(queryByOwner(OWNER_A), RUNNING);

        scheduler.updateShards(OWNER_A.role, JOB_A, jobShards, updateToken);

        changeStatus(Query.byStatus(UPDATING), KILLED);

        assertThat(getTasks(Query.byStatus(PENDING)).size(), is(numTasks + additionalTasks));

        scheduler.rollbackShards(OWNER_A.role, JOB_A, jobShards, updateToken);

        changeStatus(Query.byStatus(PENDING), ASSIGNED);
        changeStatus(Query.byStatus(ASSIGNED), RUNNING);

        return UpdateResult.FAILED;
      }

      @Override void verify(Set<ScheduledTask> tasks, JobConfiguration oldJob,
          JobConfiguration updatedJob) {
        verifyUpdate(tasks, oldJob, new Closure<ScheduledTask>() {
          @Override public void execute(ScheduledTask state) {
            TwitterTaskInfo task = Tasks.SCHEDULED_TO_INFO.apply(state);
            assertThat(task.getStartCommand(), is(oldCommandFactory.apply(task.getShardId())));
          }
        });
      }
    }.runTest(numTasks, additionalTasks);
  }

  @Test
  public void testTaskIdExpansion() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo config = productionTask("start_command", "%task_id%");

    scheduler.createJob(makeJob(OWNER_A, JOB_A, config, 1));

    String taskId = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 0)));
    Offer offer = createOffer(SLAVE_ID, SLAVE_HOST_1, 4, FOUR_GB, ONE_GB);
    sendOffer(offer, taskId, SLAVE_HOST_1);

    AssignedTask task = getTask(taskId).getAssignedTask();
    assertThat(task.getTask().getStartCommand(), is(taskId));
  }

  @Test
  public void testShardIdExpansion() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo config = productionTask("start_command", "%shard_id%");

    scheduler.createJob(makeJob(OWNER_A, JOB_A, config, 1));

    String taskId = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 0)));
    Offer offer = createOffer(SLAVE_ID, SLAVE_HOST_1, 4, FOUR_GB, ONE_GB);
    sendOffer(offer, taskId, SLAVE_HOST_1);

    AssignedTask task = getTask(taskId).getAssignedTask();
    assertThat(task.getTask().getStartCommand(), is("0"));
  }

  @Test
  public void testPortResource() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo config = productionTask("start_command", "%port:one% %port:two% %port:three%")
        .setRequestedPorts(ImmutableSet.of("one", "two", "three"));

    scheduler.createJob(makeJob(OWNER_A, JOB_A, config, 1));

    String taskId = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 0)));

    Set<Integer> assignedPorts = ImmutableSet.of(80, 81, 82);
    Offer threePorts = createOffer(SLAVE_ID, SLAVE_HOST_1, 4, FOUR_GB, ONE_GB,
        ImmutableSet.of(Pair.of(80, 82)));
    sendOffer(threePorts, taskId, SLAVE_HOST_1, ImmutableSet.of("one", "two", "three"),
        assignedPorts);

    AssignedTask task = getTask(taskId).getAssignedTask();

    assertThat(ImmutableSet.copyOf(Splitter.on(" ").split(task.getTask().getStartCommand())),
        is(ImmutableSet.of("80", "81", "82")));
  }

  @Test
  public void testPortResourceResetAfterReschedule() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo config = productionTask("start_command", "%port:one%")
        .setRequestedPorts(ImmutableSet.of("one"));

    scheduler.createJob(makeJob(OWNER_A, JOB_A, config, 1));

    String taskId = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 0)));

    Set<Integer> assignedPorts = ImmutableSet.of(80);
    Offer threePorts = createOffer(SLAVE_ID, SLAVE_HOST_1, 4, FOUR_GB, ONE_GB,
        ImmutableSet.of(Pair.of(80, 80)));
    sendOffer(threePorts, taskId, SLAVE_HOST_1, ImmutableSet.of("one"), assignedPorts);

    AssignedTask task = getTask(taskId).getAssignedTask();
    assertThat(task.getTask().getStartCommand(), is("80"));

    // The task should be rescheduled.
    changeStatus(taskId, LOST);

    String newTaskId = Tasks.id(getOnlyTask(Query.liveShard(Tasks.jobKey(OWNER_A, JOB_A), 0)));
    assertThat(getTask(newTaskId).getAssignedTask().getTask().getStartCommand(), is("%port:one%"));

    assignedPorts = ImmutableSet.of(86);
    Offer threeOtherPorts = createOffer(SLAVE_ID, SLAVE_HOST_1, 4, FOUR_GB, ONE_GB,
        ImmutableSet.of(Pair.of(86, 86)));
    sendOffer(threeOtherPorts, newTaskId, SLAVE_HOST_1, ImmutableSet.of("one"),
        assignedPorts);

    task = getTask(newTaskId).getAssignedTask();
    assertThat(task.getTask().getStartCommand(), is("86"));
  }

  @Test
  public void testBadExpansion() throws Exception {
    control.replay();
    buildScheduler();

    TwitterTaskInfo task = productionTask("start_command", "%port%");
    scheduler.createJob(makeJob(OWNER_A, "foo", task, 1));
  }

  @Test
  public void testAuditMessage() throws Exception {
    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));

    String taskId = Tasks.id(getOnlyTask(queryByOwner(OWNER_A)));
    changeStatus(taskId, ASSIGNED);
    changeStatus(taskId, STARTING);
    changeStatus(taskId, FAILED, "bad stuff happened");

    String hostname = getLocalHost();

    Iterator<Pair<ScheduleStatus, String>> expectedEvents =
        ImmutableList.<Pair<ScheduleStatus, String>>builder()
            .add(Pair.<ScheduleStatus, String>of(PENDING, null))
            .add(Pair.<ScheduleStatus, String>of(ASSIGNED, null))
            .add(Pair.<ScheduleStatus, String>of(STARTING, null))
            .add(Pair.<ScheduleStatus, String>of(FAILED, "bad stuff happened"))
            .build()
        .iterator();
    for (TaskEvent event : getTask(taskId).getTaskEvents()) {
      Pair<ScheduleStatus, String> expected = expectedEvents.next();
      assertEquals(expected.getFirst(), event.getStatus());
      assertEquals(expected.getSecond(), event.getMessage());
      assertEquals(hostname, event.getScheduler());
    }
  }

  @Test
  public void testStuckInKilling() throws Exception {
    // One kill from transition to KILLING, another when the task is observed stuck.
    expectKillTask(2);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
    String taskId = Tasks.id(getOnlyTask(queryByOwner(OWNER_A)));
    changeStatus(taskId, ASSIGNED);
    changeStatus(taskId, STARTING);
    changeStatus(taskId, KILLING);

    clock.advance(StateManager.MISSING_TASK_GRACE_PERIOD.get());
    clock.advance(StateManager.MISSING_TASK_GRACE_PERIOD.get());

    stateManager.scanOutstandingTasks();

    assertEquals(1, getTasksByStatus(LOST).size());
  }

  @Test
  public void testStuckInAssigned() throws Exception {
    expectKillTask(1);

    control.replay();
    buildScheduler();

    scheduler.createJob(makeJob(OWNER_A, JOB_A, 1));
    String taskId = Tasks.id(getOnlyTask(queryByOwner(OWNER_A)));
    changeStatus(taskId, ASSIGNED);

    clock.advance(StateManager.MISSING_TASK_GRACE_PERIOD.get());
    clock.advance(StateManager.MISSING_TASK_GRACE_PERIOD.get());

    stateManager.scanOutstandingTasks();

    assertTaskCount(2);
    assertEquals(1, getTasksByStatus(PENDING).size());
    assertEquals(1, getTasksByStatus(LOST).size());
  }

  private static String getLocalHost() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw Throwables.propagate(e);
    }
  }

  // TODO(William Farner): Inject a task ID generation function into StateManager so that we can
  //     expect specific task IDs to be killed here.
  private void expectKillTask(int numTasks) {
    driver.killTask(EasyMock.<String>anyObject());
    expectLastCall().times(numTasks);
  }

  private void assertTaskCount(int numTasks) {
    assertThat(scheduler.getTasks(Query.GET_ALL).size(), is(numTasks));
  }

  private static JobConfiguration makeJob(Identity owner, String jobName, TwitterTaskInfo task,
      int numTasks) {
    List<TwitterTaskInfo> tasks = Lists.newArrayList();
    for (int i = 0; i < numTasks; i++) {
      tasks.add(new TwitterTaskInfo(task).setOwner(owner).setJobName(jobName));
    }
    return makeJob(owner, jobName, tasks);
  }

  private static JobConfiguration makeJob(Identity owner, String jobName, int numDefaultTasks) {
    return makeJob(owner, jobName, productionTask(), numDefaultTasks);
  }

  private static JobConfiguration makeJob(Identity owner, String jobName,
      Iterable<TwitterTaskInfo> tasks) {
    JobConfiguration job = new JobConfiguration();
    job.setOwner(owner).setName(jobName);
    int i = 0;
    for (TwitterTaskInfo task : tasks) {
      job.addToTaskConfigs(new TwitterTaskInfo(task).setShardId(i++));
    }
    return job;
  }

  private static Offer createOffer(SlaveID slave, String slaveHost, double cpu,
      double ramMb, double diskMb) {
    return createOffer(slave, slaveHost, cpu, ramMb, diskMb,
        ImmutableSet.<Pair<Integer, Integer>>of());
  }

  private static Offer createOffer(SlaveID slave, String slaveHost, double cpu,
      double ramMb, double diskMb, Set<Pair<Integer, Integer>> ports) {

    Ranges portRanges = Ranges.newBuilder()
        .addAllRange(Iterables.transform(ports, new Function<Pair<Integer, Integer>, Range>() {
          @Override public Range apply(Pair<Integer, Integer> range) {
            return Range.newBuilder().setBegin(range.getFirst()).setEnd(range.getSecond()).build();
          }
        }))
        .build();

    return Offer.newBuilder()
        .addResources(Resource.newBuilder().setType(Type.SCALAR).setName(Resources.CPUS)
            .setScalar(Scalar.newBuilder().setValue(cpu)))
        .addResources(Resource.newBuilder().setType(Type.SCALAR).setName(Resources.RAM_MB)
            .setScalar(Scalar.newBuilder().setValue(ramMb)))
        .addResources(Resource.newBuilder().setType(Type.SCALAR).setName(Resources.DISK_MB)
            .setScalar(Scalar.newBuilder().setValue(diskMb)))
        .addResources(Resource.newBuilder().setType(Type.RANGES).setName(Resources.PORTS)
            .setRanges(portRanges))
        .addAttributes(Attribute.newBuilder().setType(Type.TEXT)
            .setName(HOST_CONSTRAINT)
            .setText(Text.newBuilder().setValue(slaveHost)))
        .setSlaveId(slave)
        .setHostname(slaveHost)
        .setFrameworkId(FrameworkID.newBuilder().setValue(FRAMEWORK_ID).build())
        .setId(OFFER_ID)
        .build();
  }

  private static TwitterTaskInfo defaultTask(boolean production, String... additionalParams) {
    Preconditions.checkArgument((additionalParams.length % 2) == 0,
        "Additional params count must be even.");

    Map<String, String> params = Maps.newHashMap(ImmutableMap.<String, String>builder()
        .put("start_command", "date")
        .put("num_cpus", "1.0")
        .put("ram_mb", "1024")
        .put("disk_mb", "1024")
        .put("hdfs_path", "/fake/path")
        .put("production", Boolean.toString(production))
        .build());

    for (int i = 0; i < additionalParams.length; i += 2) {
      params.put(additionalParams[i], additionalParams[i + 1]);
    }

    TwitterTaskInfo task = new TwitterTaskInfo().setConfiguration(ImmutableMap.copyOf(params));

    // Avoid hitting per-host scheduling constraints.
    task.addToConstraints(hostLimitConstraint(100));
    return task;
  }

  private static TwitterTaskInfo productionTask(String... additionalParams) {
    return defaultTask(true, additionalParams);
  }

  private static TwitterTaskInfo nonProductionTask(String... additionalParams) {
    return defaultTask(false, additionalParams);
  }

  private ScheduledTask getTask(String taskId) {
    return getOnlyTask(query(taskId));
  }

  private ScheduledTask getOnlyTask(Query query) {
    return Iterables.getOnlyElement(scheduler.getTasks((query)));
  }

  private Set<ScheduledTask> getTasks(Query query) {
    return scheduler.getTasks(query);
  }

  private Set<ScheduledTask> getTasksByStatus(ScheduleStatus status) {
    return scheduler.getTasks(Query.byStatus(status));
  }

  private Set<ScheduledTask> getTasksOwnedBy(Identity owner) {
    return scheduler.getTasks(query(owner, null, null));
  }

  private Query query(Iterable<String> taskIds) {
    return query(null, null, taskIds);
  }

  private Query query(String... taskIds) {
    return query(null, null, ImmutableList.copyOf(taskIds));
  }

  private Query queryByOwner(Identity owner) {
    return query(owner, null, null);
  }

  private Query queryJob(Identity owner, String jobName) {
    return query(owner, jobName, null);
  }

  private Query query(@Nullable Identity owner, @Nullable String jobName,
      @Nullable Iterable<String> taskIds) {
    TaskQuery query = new TaskQuery();
    if (owner != null) {
      query.setOwner(owner);
    }
    if (jobName != null) {
      query.setJobName(jobName);
    }
    if (taskIds != null) {
      query.setTaskIds(Sets.newHashSet(taskIds));
    }

    return new Query(query);
  }

  public void changeStatus(Query query, ScheduleStatus status, @Nullable String message) {
    scheduler.setTaskStatus(query, status, message);
  }

  public void changeStatus(Query query, ScheduleStatus status) {
    changeStatus(query, status, null);
  }

  public void changeStatus(String taskId, ScheduleStatus status) {
    changeStatus(taskId, status, null);
  }

  public void changeStatus(String taskId, ScheduleStatus status, @Nullable String message) {
    changeStatus(query(Arrays.asList(taskId)), status, message);
  }

  private static final ImmutableSet<Veto> ALWAYS_VETO = ImmutableSet.of(new Veto("Fake veto"));

  private static final ImmutableSet<Veto> NO_VETO = ImmutableSet.of();
}

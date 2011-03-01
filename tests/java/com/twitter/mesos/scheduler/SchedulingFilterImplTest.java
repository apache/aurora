package com.twitter.mesos.scheduler;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.SchedulerCore.TaskState;
import com.twitter.mesos.scheduler.SchedulingFilter.SchedulingFilterImpl;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author William Farner
 */
public class SchedulingFilterImplTest extends EasyMockTest {

  private static final String HOST_A = "hostA";
  private static final String HOST_B = "hostB";
  private static final String HOST_C = "hostC";

  private static final String JOB_A = "myJobA";
  private static final String JOB_B = "myJobB";
  private static final String OWNER_A = "ownerA";

  private static final Map<String, String> EMPTY_MAP = Maps.newHashMap();
  private static final long DEFAULT_DISK = 1000;

  private static final long DEFAULT_RAM = 1000;
  private static final int DEFAULT_CPUS = 4;
  private static final TwitterTaskInfo DEFAULT_OFFER = new TwitterTaskInfo()
      .setDiskMb(DEFAULT_DISK).setRamMb(DEFAULT_RAM).setNumCpus(DEFAULT_CPUS);

  private SchedulingFilter defaultFilter;
  private SchedulerCore scheduler;

  @Before
  public void setUp() {
    scheduler = createMock(SchedulerCore.class);
    defaultFilter = new SchedulingFilterImpl(scheduler, EMPTY_MAP);
  }

  @Test
  public void testMeetsOffer() throws Exception {
    expectGetTasks();
    control.replay();

    Predicate<ScheduledTask> filter = defaultFilter.makeFilter(DEFAULT_OFFER, HOST_A);
    assertThat(filter.apply(makeScheduledTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)), is(true));
    assertThat(filter.apply(makeScheduledTask(DEFAULT_CPUS - 1, DEFAULT_RAM - 1, DEFAULT_DISK - 1)),
        is(true));
  }

  @Test
  public void testInsufficientResources() throws Exception {
    expectGetTasks();
    control.replay();

    Predicate<ScheduledTask> filter = defaultFilter.makeFilter(DEFAULT_OFFER, HOST_A);
    assertThat(filter.apply(makeScheduledTask(DEFAULT_CPUS + 1, DEFAULT_RAM + 1, DEFAULT_DISK + 1)),
        is(false));
    assertThat(filter.apply(makeScheduledTask(DEFAULT_CPUS + 1, DEFAULT_RAM, DEFAULT_DISK)),
        is(false));
    assertThat(filter.apply(makeScheduledTask(DEFAULT_CPUS, DEFAULT_RAM + 1, DEFAULT_DISK)),
        is(false));
    assertThat(filter.apply(makeScheduledTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK + 1)),
        is(false));
  }

  @Test
  public void testJobAllowedOnMachine() throws Exception {
    expectGetTasks();
    expectGetTasks();
    control.replay();

    SchedulingFilter filterBuilder = new SchedulingFilterImpl(scheduler, ImmutableMap.of(
        HOST_B, Tasks.jobKey(OWNER_A, JOB_A),
        HOST_C, Tasks.jobKey(OWNER_A, JOB_A)));

    assertThat(filterBuilder.makeFilter(DEFAULT_OFFER, HOST_B).apply(
        makeTask(OWNER_A, JOB_A, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)), is(true));
    assertThat(filterBuilder.makeFilter(DEFAULT_OFFER, HOST_C).apply(
        makeTask(OWNER_A, JOB_A, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)), is(true));
  }

  @Test
  public void testJobNotAllowedOnMachine() throws Exception {
    expectGetTasks();
    expectGetTasks();
    control.replay();

    SchedulingFilter filterBuilder = new SchedulingFilterImpl(scheduler, ImmutableMap.of(
        HOST_B, Tasks.jobKey(OWNER_A, JOB_A),
        HOST_C, Tasks.jobKey(OWNER_A, JOB_A)));

    // HOST_B can only run OWNER_A/JOB_A.
    assertThat(filterBuilder.makeFilter(DEFAULT_OFFER, HOST_A).apply(
        makeTask(OWNER_A, JOB_B, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)), is(true));
    assertThat(filterBuilder.makeFilter(DEFAULT_OFFER, HOST_B).apply(
        makeTask(OWNER_A, JOB_B, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)), is(false));
  }

  @Test
  public void testMachineNotAllowedToRunJob() throws Exception {
    expectGetTasks();
    expectGetTasks();
    control.replay();

    SchedulingFilter filterBuilder = new SchedulingFilterImpl(scheduler, ImmutableMap.of(
        HOST_B, Tasks.jobKey(OWNER_A, JOB_A),
        HOST_C, Tasks.jobKey(OWNER_A, JOB_A)));

    // OWNER_A/JOB_A can only run on HOST_B or HOST_C
    assertThat(filterBuilder.makeFilter(DEFAULT_OFFER, HOST_A).apply(
        makeTask(OWNER_A, JOB_B, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)), is(true));
    assertThat(filterBuilder.makeFilter(DEFAULT_OFFER, HOST_A).apply(
        makeTask(OWNER_A, JOB_A, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)), is(false));
  }

  @Test
  public void testHonorsMaxPerHost() throws Exception {
    expectGetTasks();
    expectGetTasks(makeTask(0, 2));
    expectGetTasks(makeTask(0, 2), makeTask(1, 2));
    expectGetTasks(makeTask(0, 2), makeTask(1, 2));

    control.replay();

    assertThat(defaultFilter.makeFilter(DEFAULT_OFFER, HOST_A).apply(makeTask(0, 2)), is(true));
    assertThat(defaultFilter.makeFilter(DEFAULT_OFFER, HOST_A).apply(makeTask(1, 2)), is(true));
    assertThat(defaultFilter.makeFilter(DEFAULT_OFFER, HOST_A).apply(makeTask(2, 2)), is(false));

    // Newer task takes precedence over existing tasks.
    assertThat(defaultFilter.makeFilter(DEFAULT_OFFER, HOST_A).apply(makeTask(2, 3)), is(true));
  }

  @Test
  public void testHonorsAvoidJobs() throws Exception {
    expectGetTasks(makeTask(0, "jim", "jobA", ImmutableSet.of("sue/jobC")));
    expectGetTasks(makeTask(0, "jim", "jobA", ImmutableSet.of("sue/jobC")));
    expectGetTasks(makeTask(0, "jim", "jobA", ImmutableSet.of("sue/jobC")));

    control.replay();

    assertThat(defaultFilter.makeFilter(DEFAULT_OFFER, HOST_A).apply(
        makeTask(0, "jack", "jobB", ImmutableSet.<String>of())), is(true));
    assertThat(defaultFilter.makeFilter(DEFAULT_OFFER, HOST_A).apply(
        makeTask(0, "jack", "jobB", ImmutableSet.of("jim/jobA"))), is(false));
    assertThat(defaultFilter.makeFilter(DEFAULT_OFFER, HOST_A).apply(
        makeTask(0, "sue", "jobC", ImmutableSet.<String>of())), is(false));
  }

  private void expectGetTasks(ScheduledTask... tasks) {
    ImmutableSet<TaskState> states = ImmutableSet.copyOf(Iterables
        .transform(ImmutableList.copyOf(tasks), new Function<ScheduledTask, TaskState>() {
          @Override public TaskState apply(ScheduledTask task) {
            return new TaskState(task, new VolatileTaskState(Tasks.id(task)));
          }
        }));
    expect(scheduler.getTasks((Query) anyObject())).andReturn(states);
  }

  private ScheduledTask makeTask(int shard, int maxPerHost) throws Exception {
    ScheduledTask task = makeTask(OWNER_A, JOB_A, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK);
    task.getAssignedTask().getTask()
        .setOwner(OWNER_A)
        .setJobName(JOB_A)
        .setShardId(shard)
        .setMaxPerHost(maxPerHost);
    return task;
  }

  private ScheduledTask makeTask(int shard, String owner, String jobName, Set<String> avoidJobs)
      throws Exception {
    ScheduledTask task = makeTask(owner, jobName, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK);
    task.getAssignedTask().getTask()
        .setOwner(owner)
        .setJobName(jobName)
        .setShardId(shard)
        .setAvoidJobs(avoidJobs);
    return task;
  }

  private int taskId = 1;
  private ScheduledTask makeTask(String owner, String jobName, int cpus, long ramMb,
      long diskMb) throws Exception {
    return new ScheduledTask().setAssignedTask(new AssignedTask().setTask(
        ConfigurationManager.applyDefaultsIfUnset(new TwitterTaskInfo()
            .setOwner(owner)
            .setJobName(jobName)
            .setNumCpus(cpus)
            .setRamMb(ramMb)
            .setDiskMb(diskMb)
            .setMaxPerHost(1))).setTaskId(String.valueOf(taskId++)));
  }

  private ScheduledTask makeScheduledTask(int cpus, long ramMb, long diskMb) throws Exception {
    return makeTask(OWNER_A, JOB_A, cpus, ramMb, diskMb);
  }
}

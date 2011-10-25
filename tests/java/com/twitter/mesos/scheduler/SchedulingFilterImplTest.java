package com.twitter.mesos.scheduler;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.TwitterTaskInfo;
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

  private static final String ROLE_A = "roleA";
  private static final String USER_A = "userA";
  private static final Identity OWNER_A = new Identity(ROLE_A, USER_A);

  private static final Map<String, String> EMPTY_MAP = Maps.newHashMap();
  private static final long DEFAULT_DISK = 1000;

  private static final long DEFAULT_RAM = 1000;
  private static final int DEFAULT_CPUS = 4;
  private static final Resources DEFAULT_OFFER =
      new Resources(DEFAULT_CPUS, Amount.of(DEFAULT_RAM, Data.MB), 0);

  private SchedulingFilter defaultFilter;
  private Function<Query, Iterable<TwitterTaskInfo>> taskFetcher;

  @Before
  public void setUp() {
    taskFetcher = createMock(new Clazz<Function<Query,Iterable<TwitterTaskInfo>>>() {});
    defaultFilter = new SchedulingFilterImpl(EMPTY_MAP);
  }

  @Test
  public void testMeetsOffer() throws Exception {
    control.replay();

    Predicate<TwitterTaskInfo> filter = defaultFilter.staticFilter(DEFAULT_OFFER, null);
    assertThat(filter.apply(makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)), is(true));
    assertThat(filter.apply(makeTask(DEFAULT_CPUS - 1, DEFAULT_RAM - 1, DEFAULT_DISK - 1)),
        is(true));
  }

  @Test
  public void testSufficientPorts() throws Exception {
    control.replay();

    Resources twoPorts = new Resources(DEFAULT_CPUS, Amount.of(DEFAULT_RAM, Data.MB), 2);

    Predicate<TwitterTaskInfo> filter = defaultFilter.staticFilter(twoPorts, null);

    TwitterTaskInfo noPortTask = makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)
        .setStartCommand("%task_id%");
    TwitterTaskInfo onePortTask = makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)
        .setStartCommand("%port:one% %port:one% %port:one% %port:one%");
    TwitterTaskInfo twoPortTask = makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)
        .setStartCommand("%port:one% %port:two% %port:two%");
    TwitterTaskInfo threePortTask = makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)
        .setStartCommand("%port:one% %port:two% %port:three%");
    assertThat(filter.apply(noPortTask), is(true));
    assertThat(filter.apply(onePortTask), is(true));
    assertThat(filter.apply(twoPortTask), is(true));
    assertThat(filter.apply(threePortTask), is(false));
  }

  @Test
  public void testInsufficientResources() throws Exception {
    control.replay();

    Predicate<TwitterTaskInfo> filter = defaultFilter.staticFilter(DEFAULT_OFFER, null);
    assertThat(filter.apply(makeTask(DEFAULT_CPUS + 1, DEFAULT_RAM + 1, DEFAULT_DISK + 1)),
        is(false));
    assertThat(filter.apply(makeTask(DEFAULT_CPUS + 1, DEFAULT_RAM, DEFAULT_DISK)),
        is(false));
    assertThat(filter.apply(makeTask(DEFAULT_CPUS, DEFAULT_RAM + 1, DEFAULT_DISK)),
        is(false));
  }

  @Test
  public void testJobAllowedOnMachine() throws Exception {
    control.replay();

    SchedulingFilter filterBuilder = new SchedulingFilterImpl(ImmutableMap.of(
        HOST_B, Tasks.jobKey(OWNER_A, JOB_A),
        HOST_C, Tasks.jobKey(OWNER_A, JOB_A)));

    assertThat(filterBuilder.staticFilter(DEFAULT_OFFER, HOST_B).apply(
        makeTask(OWNER_A, JOB_A, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)), is(true));
    assertThat(filterBuilder.staticFilter(DEFAULT_OFFER, HOST_C).apply(
        makeTask(OWNER_A, JOB_A, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)), is(true));
  }

  @Test
  public void testJobNotAllowedOnMachine() throws Exception {
    control.replay();

    SchedulingFilter filterBuilder = new SchedulingFilterImpl(ImmutableMap.of(
        HOST_B, Tasks.jobKey(OWNER_A, JOB_A),
        HOST_C, Tasks.jobKey(OWNER_A, JOB_A)));

    // HOST_B can only run OWNER_A/JOB_A.
    assertThat(filterBuilder.staticFilter(DEFAULT_OFFER, HOST_A).apply(
        makeTask(OWNER_A, JOB_B, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)), is(true));
    assertThat(filterBuilder.staticFilter(DEFAULT_OFFER, HOST_B).apply(
        makeTask(OWNER_A, JOB_B, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)), is(false));
  }

  @Test
  public void testMachineNotAllowedToRunJob() throws Exception {
    control.replay();

    SchedulingFilter filterBuilder = new SchedulingFilterImpl(ImmutableMap.of(
        HOST_B, Tasks.jobKey(OWNER_A, JOB_A),
        HOST_C, Tasks.jobKey(OWNER_A, JOB_A)));

    // OWNER_A/JOB_A can only run on HOST_B or HOST_C
    assertThat(filterBuilder.staticFilter(DEFAULT_OFFER, HOST_A).apply(
        makeTask(OWNER_A, JOB_B, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)), is(true));
    assertThat(filterBuilder.staticFilter(DEFAULT_OFFER, HOST_A).apply(
        makeTask(OWNER_A, JOB_A, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)), is(false));
  }

  @Test
  public void testHonorsMaxPerHost() throws Exception {
    expectGetTasks();
    expectGetTasks(makeTask(0, 2));
    expectGetTasks(makeTask(0, 2), makeTask(1, 2));
    expectGetTasks(makeTask(0, 2), makeTask(1, 2));

    control.replay();

    assertThat(defaultFilter.dynamicHostFilter(taskFetcher, HOST_A).apply(makeTask(0, 2)),
        is(true));
    assertThat(defaultFilter.dynamicHostFilter(taskFetcher, HOST_A).apply(makeTask(1, 2)),
        is(true));
    assertThat(defaultFilter.dynamicHostFilter(taskFetcher, HOST_A).apply(makeTask(2, 2)),
        is(false));

    // Newer task takes precedence over existing tasks.
    assertThat(defaultFilter.dynamicHostFilter(taskFetcher, HOST_A).apply(makeTask(2, 3)),
        is(true));
  }

  @Test
  public void testHonorsAvoidJobs() throws Exception {
    Identity jimmy = new Identity("jim", "jim");
    expectGetTasks(makeTask(0, jimmy, "jobA", ImmutableSet.of("sue/jobC")));
    expectGetTasks(makeTask(0, jimmy, "jobA", ImmutableSet.of("sue/jobC")));
    expectGetTasks(makeTask(0, jimmy, "jobA", ImmutableSet.of("sue/jobC")));

    control.replay();

    assertThat(defaultFilter.dynamicHostFilter(taskFetcher, HOST_A).apply(
        makeTask(0, new Identity("jack", "jack"), "jobB", ImmutableSet.<String>of())), is(true));
    assertThat(defaultFilter.dynamicHostFilter(taskFetcher, HOST_A).apply(
        makeTask(0, new Identity("jack", "jack"), "jobB", ImmutableSet.of("jim/jobA"))), is(false));
    assertThat(defaultFilter.dynamicHostFilter(taskFetcher, HOST_A).apply(
        makeTask(0, new Identity("sue", "sue"), "jobC", ImmutableSet.<String>of())), is(false));
  }

  private void expectGetTasks(TwitterTaskInfo... tasks) {
    expect(taskFetcher.apply((Query) anyObject())).andReturn(ImmutableList.copyOf(tasks));
  }

  private TwitterTaskInfo makeTask(int shard, int maxPerHost) throws Exception {
    TwitterTaskInfo task = makeTask(OWNER_A, JOB_A, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK);
    task.setOwner(OWNER_A)
        .setJobName(JOB_A)
        .setShardId(shard)
        .setMaxPerHost(maxPerHost);
    return task;
  }

  private TwitterTaskInfo makeTask(int shard, Identity owner, String jobName, Set<String> avoidJobs)
      throws Exception {
    TwitterTaskInfo task = makeTask(owner, jobName, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK);
    task.setOwner(owner)
        .setJobName(jobName)
        .setShardId(shard)
        .setAvoidJobs(avoidJobs);
    return task;
  }

  private TwitterTaskInfo makeTask(Identity owner, String jobName, int cpus, long ramMb,
      long diskMb) throws Exception {
    return ConfigurationManager.applyDefaultsIfUnset(new TwitterTaskInfo()
        .setOwner(owner)
        .setJobName(jobName)
        .setNumCpus(cpus)
        .setRamMb(ramMb)
        .setDiskMb(diskMb)
        .setMaxPerHost(1));
  }

  private TwitterTaskInfo makeTask(int cpus, long ramMb, long diskMb) throws Exception {
    return makeTask(OWNER_A, JOB_A, cpus, ramMb, diskMb);
  }
}

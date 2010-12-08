package com.twitter.mesos.scheduler;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.SchedulingFilter.SchedulingFilterImpl;
import com.twitter.mesos.scheduler.TaskStore.TaskState;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author wfarner
 */
public class SchedulingFilterImplTest {

  private static final String HOST_A = "hostA";
  private static final String HOST_B = "hostB";
  private static final String HOST_C = "hostC";

  private static final String JOB_A = "myJobA";
  private static final String JOB_B = "myJobB";
  private static final String OWNER_A = "ownerA";

  private static final Map<String, String> EMPTY_MAP = Maps.newHashMap();
  private static final SchedulingFilter DEFAULT_FILTER = new SchedulingFilterImpl(EMPTY_MAP);

  private static final long DEFAULT_DISK = 1000;
  private static final long DEFAULT_RAM = 1000;
  private static final int DEFAULT_CPUS = 4;
  private static final TwitterTaskInfo DEFAULT_OFFER = new TwitterTaskInfo()
      .setDiskMb(DEFAULT_DISK).setRamMb(DEFAULT_RAM).setNumCpus(DEFAULT_CPUS);

  @Test
  public void testMeetsOffer() {
    Predicate<TaskState> filter = DEFAULT_FILTER.makeFilter(DEFAULT_OFFER, HOST_A);
    assertThat(filter.apply(makeScheduledTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)), is(true));
    assertThat(filter.apply(makeScheduledTask(DEFAULT_CPUS - 1, DEFAULT_RAM - 1, DEFAULT_DISK - 1)),
        is(true));
  }

  @Test
  public void testInsufficientResources() {
    Predicate<TaskState> filter = DEFAULT_FILTER.makeFilter(DEFAULT_OFFER, HOST_A);
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
  public void testJobAllowedOnMachine() {
    SchedulingFilter filterBuilder = new SchedulingFilterImpl(ImmutableMap.of(
        HOST_B, Tasks.jobKey(OWNER_A, JOB_A),
        HOST_C, Tasks.jobKey(OWNER_A, JOB_A)));

    assertThat(filterBuilder.makeFilter(DEFAULT_OFFER, HOST_B).apply(
        makeScheduledTask(OWNER_A, JOB_A, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)), is(true));
    assertThat(filterBuilder.makeFilter(DEFAULT_OFFER, HOST_C).apply(
        makeScheduledTask(OWNER_A, JOB_A, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)), is(true));
  }

  @Test
  public void testJobNotAllowedOnMachine() {
    SchedulingFilter filterBuilder = new SchedulingFilterImpl(ImmutableMap.of(
        HOST_B, Tasks.jobKey(OWNER_A, JOB_A),
        HOST_C, Tasks.jobKey(OWNER_A, JOB_A)));

    // HOST_B can only run OWNER_A/JOB_A.
    assertThat(filterBuilder.makeFilter(DEFAULT_OFFER, HOST_A).apply(
        makeScheduledTask(OWNER_A, JOB_B, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)), is(true));
    assertThat(filterBuilder.makeFilter(DEFAULT_OFFER, HOST_B).apply(
        makeScheduledTask(OWNER_A, JOB_B, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)), is(false));
  }

  @Test
  public void testMachineNotAllowedToRunJob() {
    SchedulingFilter filterBuilder = new SchedulingFilterImpl(ImmutableMap.of(
        HOST_B, Tasks.jobKey(OWNER_A, JOB_A),
        HOST_C, Tasks.jobKey(OWNER_A, JOB_A)));

    // OWNER_A/JOB_A can only run on HOST_B or HOST_C
    assertThat(filterBuilder.makeFilter(DEFAULT_OFFER, HOST_A).apply(
        makeScheduledTask(OWNER_A, JOB_B, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)), is(true));
    assertThat(filterBuilder.makeFilter(DEFAULT_OFFER, HOST_A).apply(
        makeScheduledTask(OWNER_A, JOB_A, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)), is(false));
  }

  private static TaskState makeScheduledTask(String owner, String jobName, int cpus,
      long ramMb, long diskMb) {
    return new TaskState(new ScheduledTask().setAssignedTask(new AssignedTask().setTask(
        new TwitterTaskInfo().setOwner(owner).setJobName(jobName).setNumCpus(cpus).setRamMb(ramMb)
            .setDiskMb(diskMb))));
  }

  private static TaskState makeScheduledTask(int cpus, long ramMb, long diskMb) {
    return makeScheduledTask(OWNER_A, JOB_A, cpus, ramMb, diskMb);
  }
}

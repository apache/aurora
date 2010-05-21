package com.twitter.nexus.scheduler;

import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Provides;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.nexus.gen.JobConfiguration;
import com.twitter.nexus.gen.ScheduleStatus;
import com.twitter.nexus.gen.TaskQuery;
import com.twitter.nexus.gen.TrackedTask;
import com.twitter.nexus.gen.TwitterTaskInfo;
import com.twitter.nexus.scheduler.CronJobScheduler;
import com.twitter.nexus.scheduler.ImmediateJobScheduler;
import com.twitter.nexus.scheduler.ScheduleException;
import com.twitter.nexus.scheduler.SchedulerCore;
import com.twitter.nexus.scheduler.persistence.NoPersistence;
import com.twitter.nexus.scheduler.persistence.PersistenceLayer;
import nexus.SlaveOffer;
import nexus.StringMap;
import nexus.TaskDescription;
import org.apache.thrift.TDeserializer;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Unit test for the SchedulerCore.
 *
 * @author wfarner
 */
public class SchedulerCoreTest {
  private SchedulerCore scheduler;

  private static final String JOB_NAME = "Test_Job";
  private static final String JOB_OWNER = "Test_Owner";

  private static final int SLAVE_ID = 5;

  private static final Amount<Long, Data> ONE_GB = Amount.of(1L, Data.GB);

  @Before
  public void setUp() {
    scheduler = new SchedulerCore(new CronJobScheduler(), new ImmediateJobScheduler(),
        new NoPersistence());
  }

  @Test
  public void testCreateJob() throws ScheduleException {
    int numTasks = 10;
    TwitterTaskInfo taskObj = new TwitterTaskInfo();
    JobConfiguration job = makeJob(JOB_OWNER, JOB_NAME, taskObj, numTasks);
    scheduler.createJob(job);

    assertTaskCount(numTasks);

    Iterable<TrackedTask> tasks = scheduler.getTasks(
        new TaskQuery().setOwner(JOB_OWNER).setJobName(JOB_NAME));
    assertThat(Iterables.size(tasks), is(numTasks));
    for (TrackedTask task : tasks) {
      assertThat(task.getStatus(), is(ScheduleStatus.PENDING));
      assertThat(task.isSetTaskId(), is(true));
      assertThat(task.isSetSlaveId(), is(false));
      assertThat(task.getTask(), is(taskObj));
    }
  }

  @Test
  public void testCreateDuplicateJob() throws ScheduleException {
    scheduler.createJob(makeJob(JOB_OWNER, JOB_NAME, new TwitterTaskInfo(), 1));
    assertTaskCount(1);

    try {
      scheduler.createJob(makeJob(JOB_OWNER, JOB_NAME, new TwitterTaskInfo(), 1));
      fail();
    } catch (ScheduleException e) {
      // Expected
    }

    assertTaskCount(1);
  }

  @Test
  public void testCreateDuplicateCronJob() throws ScheduleException {
    // Cron jobs are scheduled on a delay, so this job's tasks will not be scheduled immediately,
    // but duplicate jobs should still be rejected.
    JobConfiguration job = makeJob(JOB_OWNER, JOB_NAME, new TwitterTaskInfo(), 1)
        .setCronSchedule("* * * * *");
    scheduler.createJob(job);
    assertTaskCount(0);

    try {
      scheduler.createJob(makeJob(JOB_OWNER, JOB_NAME, new TwitterTaskInfo(), 1));
      fail();
    } catch (ScheduleException e) {
      // Expected
    }

    assertTaskCount(0);
  }

  @Test
  public void testJobLifeCycle() throws Exception {
    int numTasks = 10;
    TwitterTaskInfo taskObj = new TwitterTaskInfo();
    JobConfiguration job = makeJob(JOB_OWNER, JOB_NAME, taskObj, numTasks);
    scheduler.createJob(job);

    assertTaskCount(numTasks);

    /**
     * TODO(wfarner): Complete this once constructing a SlaveOffer object doesn't require swig.
    TaskDescription desc = scheduler.offer(
        makeOffer(SLAVE_ID, 1, ONE_GB.as(Data.BYTES)));
    assertThat(desc, is(not(null)));
    assertThat(desc.getSlaveId(), is(SLAVE_ID));

    TwitterTaskInfo taskInfo = new TwitterTaskInfo();
    new TDeserializer().deserialize(taskInfo, desc.getArg());
    assertThat(taskInfo, is(taskObj));
     */

    // TODO(wfarner): Complete.
  }

  @Test
  public void testDaemonTasksRescheduled() throws ScheduleException {
    // TODO(wfarner): Implement.
  }

  @Test
  public void testCronJobLifeCycle() {
    // TODO(wfarner): Figure out how to test the lifecycle of a cron job.
  }

  private void assertTaskCount(int numTasks) {
    assertThat(Iterables.size(scheduler.getTasks(new TaskQuery())), is(numTasks));
  }

  private static JobConfiguration makeJob(String owner, String jobName, TwitterTaskInfo task,
      int numTasks) {
    JobConfiguration job = new JobConfiguration();
    job.setOwner(owner)
        .setName(jobName);
    for (int i = 0; i < numTasks; i++) {
      job.addToTaskConfigs(new TwitterTaskInfo(task));
    }

    return job;
  }

  private static SlaveOffer makeOffer(int slaveId, int cpus, long ramBytes) {
    SlaveOffer offer = new SlaveOffer();
    offer.setSlaveId(slaveId);
    offer.setHost("Host_" + slaveId);
    StringMap params = new StringMap();
    params.set("cpus", String.valueOf(cpus));
    params.set("mem", String.valueOf(ramBytes));
    offer.setParams(params);
    return offer;
  }
}

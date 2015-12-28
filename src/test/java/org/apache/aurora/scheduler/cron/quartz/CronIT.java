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
package org.apache.aurora.scheduler.cron.quartz;

import java.util.concurrent.CountDownLatch;

import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.MesosContainer;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.cron.CronJobManager;
import org.apache.aurora.scheduler.cron.CrontabEntry;
import org.apache.aurora.scheduler.cron.SanitizedCronJob;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.db.DbUtil;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.junit.Before;
import org.junit.Test;
import org.quartz.JobExecutionContext;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerListener;

import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CronIT extends EasyMockTest {
  public static final CrontabEntry CRONTAB_ENTRY = CrontabEntry.parse("* * * * *");

  private static final IJobKey JOB_KEY = JobKeys.from("roll", "b", "c");
  private static final Identity IDENTITY = new Identity()
      .setRole(JOB_KEY.getRole())
      .setUser("user");

  private static final IJobConfiguration CRON_JOB = IJobConfiguration.build(
      new JobConfiguration()
          .setCronSchedule(CRONTAB_ENTRY.toString())
          .setKey(JOB_KEY.newBuilder())
          .setInstanceCount(2)
          .setOwner(IDENTITY)
          .setTaskConfig(makeTaskConfig()));

  private Injector injector;
  private StateManager stateManager;
  private Storage storage;
  private AuroraCronJob auroraCronJob;

  @Before
  public void setUp() throws Exception {
    stateManager = createMock(StateManager.class);
    storage = DbUtil.createStorage();
    auroraCronJob = createMock(AuroraCronJob.class);

    injector = Guice.createInjector(
        // Override to verify that Guice is actually used for construction of the AuroraCronJob.
        // TODO(ksweeney): Use the production class here.
        Modules.override(new CronModule()).with(new AbstractModule() {
          @Override
          protected void configure() {
            bind(AuroraCronJob.class).toInstance(auroraCronJob);
          }
        }), new AbstractModule() {
          @Override
          protected void configure() {
            bind(ConfigurationManager.class).toInstance(TaskTestUtil.CONFIGURATION_MANAGER);
            bind(Clock.class).toInstance(Clock.SYSTEM_CLOCK);
            bind(StateManager.class).toInstance(stateManager);
            bind(Storage.class).toInstance(storage);
          }
        });
  }

  private static TaskConfig makeTaskConfig() {
    TaskConfig config = TaskTestUtil.makeConfig(JOB_KEY).newBuilder();
    config.setIsService(false);
    // Bypassing a command-line argument in ConfigurationManager that by default disallows the
    // docker container type.
    config.setContainer(Container.mesos(new MesosContainer()));
    return config;
  }

  private Service boot() {
    Service service = injector.getInstance(CronLifecycle.class);
    service.startAsync().awaitRunning();
    return service;
  }

  @Test
  public void testCronSchedulerLifecycle() throws Exception {
    control.replay();

    Scheduler scheduler = injector.getInstance(Scheduler.class);
    assertFalse(scheduler.isStarted());

    Service cronLifecycle = boot();

    assertTrue(cronLifecycle.isRunning());
    assertTrue(scheduler.isStarted());

    cronLifecycle.stopAsync().awaitTerminated();

    assertFalse(cronLifecycle.isRunning());
    assertTrue(scheduler.isShutdown());
  }

  @Test
  public void testJobsAreScheduled() throws Exception {
    auroraCronJob.execute(isA(JobExecutionContext.class));

    control.replay();
    final Scheduler scheduler = injector.getInstance(Scheduler.class);

    storage.write((NoResult.Quiet)
        storeProvider -> storeProvider.getCronJobStore().saveAcceptedJob(CRON_JOB));

    final CountDownLatch cronRan = new CountDownLatch(1);
    scheduler.getListenerManager().addTriggerListener(new CountDownWhenComplete(cronRan));
    Service service = boot();

    cronRan.await();

    service.stopAsync().awaitTerminated();
  }

  @Test
  public void testKillExistingDogpiles() throws Exception {
    // Test that a trigger for a job that hasn't finished running is ignored.
    final CronJobManager cronJobManager = injector.getInstance(CronJobManager.class);

    final CountDownLatch firstExecutionTriggered = new CountDownLatch(1);
    final CountDownLatch firstExecutionCompleted = new CountDownLatch(1);
    final CountDownLatch secondExecutionTriggered = new CountDownLatch(1);
    final CountDownLatch secondExecutionCompleted = new CountDownLatch(1);

    auroraCronJob.execute(isA(JobExecutionContext.class));
    expectLastCall().andAnswer(() -> {
      firstExecutionTriggered.countDown();
      firstExecutionCompleted.await();
      return null;
    });
    auroraCronJob.execute(isA(JobExecutionContext.class));
    expectLastCall().andAnswer(() -> {
      secondExecutionTriggered.countDown();
      secondExecutionCompleted.await();
      return null;
    });

    control.replay();

    boot();

    cronJobManager.createJob(SanitizedCronJob.fromUnsanitized(
        TaskTestUtil.CONFIGURATION_MANAGER,
        CRON_JOB));
    cronJobManager.startJobNow(JOB_KEY);
    firstExecutionTriggered.await();
    cronJobManager.startJobNow(JOB_KEY);
    assertEquals(1, secondExecutionTriggered.getCount());
    firstExecutionCompleted.countDown();
    secondExecutionTriggered.await();
    secondExecutionTriggered.countDown();
  }

  private static class CountDownWhenComplete implements TriggerListener {
    private final CountDownLatch countDownLatch;

    CountDownWhenComplete(CountDownLatch countDownLatch) {
      this.countDownLatch = countDownLatch;
    }

    @Override
    public String getName() {
      return CountDownWhenComplete.class.getName();
    }

    @Override
    public void triggerFired(Trigger trigger, JobExecutionContext context) {
      // No-op.
    }

    @Override
    public boolean vetoJobExecution(Trigger trigger, JobExecutionContext context) {
      return false;
    }

    @Override
    public void triggerMisfired(Trigger trigger) {
      // No-op.
    }

    @Override
    public void triggerComplete(
        Trigger trigger,
        JobExecutionContext context,
        Trigger.CompletedExecutionInstruction triggerInstructionCode) {

      countDownLatch.countDown();
      // No-op.
    }
  }
}

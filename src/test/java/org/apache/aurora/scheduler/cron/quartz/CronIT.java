/**
 * Copyright 2014 Apache Software Foundation
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
package org.apache.aurora.scheduler.cron.quartz;

import java.util.concurrent.CountDownLatch;

import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

import com.google.inject.util.Modules;
import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.base.ExceptionalCommand;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.Clock;

import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.cron.CronJobManager;
import org.apache.aurora.scheduler.cron.CrontabEntry;
import org.apache.aurora.scheduler.cron.SanitizedCronJob;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.state.PubsubTestUtil;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.mem.MemStorage;
import org.easymock.Capture;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;
import org.quartz.JobExecutionContext;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerListener;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertEquals;
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
          .setTaskConfig(new TaskConfig()
              .setJobName(JOB_KEY.getName())
              .setEnvironment(JOB_KEY.getEnvironment())
              .setOwner(IDENTITY)
              .setExecutorConfig(new ExecutorConfig()
                  .setName("cmd.exe")
                  .setData("echo hello world"))
              .setNumCpus(7)
              .setRamMb(8)
              .setDiskMb(9))
  );

  private ShutdownRegistry shutdownRegistry;
  private EventSink eventSink;
  private Injector injector;
  private StateManager stateManager;
  private Storage storage;
  private AuroraCronJob auroraCronJob;

  private Capture<ExceptionalCommand<?>> shutdown;

  @Before
  public void setUp() throws Exception {
    shutdownRegistry = createMock(ShutdownRegistry.class);
    stateManager = createMock(StateManager.class);
    storage = MemStorage.newEmptyStorage();
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
            bind(Clock.class).toInstance(Clock.SYSTEM_CLOCK);
            bind(ShutdownRegistry.class).toInstance(shutdownRegistry);
            bind(StateManager.class).toInstance(stateManager);
            bind(Storage.class).toInstance(storage);

            PubsubTestUtil.installPubsub(binder());
          }
        });
    eventSink = PubsubTestUtil.startPubsub(injector);

    shutdown = createCapture();
    shutdownRegistry.addAction(capture(shutdown));
  }

  private void boot() {
    eventSink.post(new PubsubEvent.SchedulerActive());
  }

  @Test
  public void testCronSchedulerLifecycle() throws Exception {
    control.replay();

    Scheduler scheduler = injector.getInstance(Scheduler.class);
    assertTrue(!scheduler.isStarted());

    boot();
    Service cronLifecycle = injector.getInstance(CronLifecycle.class);

    assertTrue(cronLifecycle.isRunning());
    assertTrue(scheduler.isStarted());

    shutdown.getValue().execute();

    assertTrue(!cronLifecycle.isRunning());
    assertTrue(scheduler.isShutdown());
  }

  @Test
  public void testJobsAreScheduled() throws Exception {
    auroraCronJob.execute(isA(JobExecutionContext.class));

    control.replay();
    final CronJobManager cronJobManager = injector.getInstance(CronJobManager.class);
    final Scheduler scheduler = injector.getInstance(Scheduler.class);

    storage.write(new Storage.MutateWork.NoResult.Quiet() {
      @Override
      public void execute(Storage.MutableStoreProvider storeProvider) {
        storeProvider.getJobStore().saveAcceptedJob(
            cronJobManager.getManagerKey(),
            CRON_JOB);
      }
    });

    final CountDownLatch cronRan = new CountDownLatch(1);
    scheduler.getListenerManager().addTriggerListener(new CountDownWhenComplete(cronRan));
    boot();

    cronRan.await();

    shutdown.getValue().execute();
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
    expectLastCall().andAnswer(new IAnswer<Void>() {
      @Override
      public Void answer() throws Throwable {
        firstExecutionTriggered.countDown();
        firstExecutionCompleted.await();
        return null;
      }
    });
    auroraCronJob.execute(isA(JobExecutionContext.class));
    expectLastCall().andAnswer(new IAnswer<Object>() {
      @Override
      public Void answer() throws Throwable {
        secondExecutionTriggered.countDown();
        secondExecutionCompleted.await();
        return null;
      }
    });

    control.replay();

    boot();

    cronJobManager.createJob(SanitizedCronJob.fromUnsanitized(CRON_JOB));
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

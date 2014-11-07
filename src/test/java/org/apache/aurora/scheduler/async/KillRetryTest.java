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
package org.apache.aurora.scheduler.async;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ScheduledExecutorService;

import javax.inject.Singleton;

import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.EventBus;
import com.google.common.testing.TearDown;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.twitter.common.application.StartupStage;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.base.ExceptionalCommand;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.StatsProvider;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.BackoffStrategy;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.apache.aurora.scheduler.mesos.Driver;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeScheduledExecutor;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.KILLING;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class KillRetryTest extends EasyMockTest {

  private Driver driver;
  private StorageTestUtil storageUtil;
  private BackoffStrategy backoffStrategy;
  private FakeScheduledExecutor clock;
  private EventBus eventBus;
  private FakeStatsProvider statsProvider;

  @Before
  public void setUp() throws Exception {
    driver = createMock(Driver.class);
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
    backoffStrategy = createMock(BackoffStrategy.class);
    final ScheduledExecutorService executorMock = createMock(ScheduledExecutorService.class);
    clock = FakeScheduledExecutor.scheduleExecutor(executorMock);
    addTearDown(new TearDown() {
      @Override
      public void tearDown() {
        clock.assertEmpty();
      }
    });
    statsProvider = new FakeStatsProvider();

    Injector injector = Guice.createInjector(
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(Driver.class).toInstance(driver);
            bind(Storage.class).toInstance(storageUtil.storage);
            bind(ScheduledExecutorService.class).toInstance(executorMock);
            PubsubEventModule.installForTest(binder());
            PubsubEventModule.bindSubscriber(binder(), KillRetry.class);
            bind(KillRetry.class).in(Singleton.class);
            bind(BackoffStrategy.class).toInstance(backoffStrategy);
            bind(StatsProvider.class).toInstance(statsProvider);
            bind(UncaughtExceptionHandler.class)
                .toInstance(createMock(UncaughtExceptionHandler.class));
            install(new LifecycleModule());
          }
        }
    );
    eventBus = injector.getInstance(EventBus.class);
    injector.getInstance(Key.get(ExceptionalCommand.class, StartupStage.class)).execute();
  }

  private static IScheduledTask makeTask(String id, ScheduleStatus status) {
    return IScheduledTask.build(new ScheduledTask()
        .setStatus(status)
        .setAssignedTask(new AssignedTask().setTaskId(id)));
  }

  private void moveToKilling(String taskId) {
    eventBus.post(TaskStateChange.transition(makeTask(taskId, KILLING), RUNNING));
  }

  private static Query.Builder killingQuery(String taskId) {
    return Query.taskScoped(taskId).byStatus(KILLING);
  }

  private void expectGetRetryDelay(long prevRetryMs, long retryInMs) {
    expect(backoffStrategy.calculateBackoffMs(prevRetryMs)).andReturn(retryInMs);
  }

  private void expectRetry(String taskId, long prevRetryMs, long nextRetryMs) {
    storageUtil.expectTaskFetch(killingQuery(taskId), makeTask(taskId, KILLING));
    driver.killTask(taskId);
    expectGetRetryDelay(prevRetryMs, nextRetryMs);
  }

  @Test
  public void testRetries() {
    String taskId = "a";
    expectGetRetryDelay(0, 100);
    expectRetry(taskId, 100, 1000);
    expectRetry(taskId, 1000, 10000);

    // Signal that task has transitioned.
    storageUtil.expectTaskFetch(killingQuery(taskId));

    control.replay();

    moveToKilling(taskId);
    clock.advance(Amount.of(100L, Time.MILLISECONDS));
    clock.advance(Amount.of(1000L, Time.MILLISECONDS));
    clock.advance(Amount.of(10000L, Time.MILLISECONDS));
    assertEquals(ImmutableMap.of(KillRetry.RETRIES_COUNTER, 2L), statsProvider.getAllValues());
  }

  @Test
  public void testDoesNotRetry() {
    String taskId = "a";
    expectGetRetryDelay(0, 100);

    storageUtil.expectTaskFetch(killingQuery(taskId));

    control.replay();

    moveToKilling(taskId);
    clock.advance(Amount.of(100L, Time.MILLISECONDS));
    assertEquals(ImmutableMap.of(KillRetry.RETRIES_COUNTER, 0L), statsProvider.getAllValues());
  }
}

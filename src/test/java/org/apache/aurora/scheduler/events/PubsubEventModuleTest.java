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
package org.apache.aurora.scheduler.events;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.Executor;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;

import org.apache.aurora.GuavaUtils;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.scheduler.AppStartup;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.app.LifecycleModule;
import org.apache.aurora.scheduler.async.AsyncModule.AsyncExecutor;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import static org.easymock.EasyMock.anyString;
import static org.junit.Assert.assertEquals;

public class PubsubEventModuleTest extends EasyMockTest {

  private FakeStatsProvider statsProvider;
  private Logger logger;
  private UncaughtExceptionHandler exceptionHandler;
  private SchedulingFilter schedulingFilter;

  @Before
  public void setUp() {
    statsProvider = new FakeStatsProvider();
    logger = createMock(Logger.class);
    exceptionHandler = createMock(UncaughtExceptionHandler.class);
    schedulingFilter = createMock(SchedulingFilter.class);
  }

  @Test
  public void testHandlesDeadEvent() {
    logger.warn(String.format(PubsubEventModule.DEAD_EVENT_MESSAGE, "hello"));

    control.replay();

    getInjector().getInstance(EventBus.class).post("hello");
    assertEquals(1L, statsProvider.getLongValue(PubsubEventModule.EVENT_BUS_DEAD_EVENTS));
  }

  @Test
  public void testPubsubExceptionTracking() throws Exception {
    logger.error(anyString(), EasyMock.<Throwable>anyObject());

    control.replay();

    Injector injector = getInjector(
        new AbstractModule() {
          @Override
          protected void configure() {
            PubsubEventModule.bindSubscriber(binder(), ThrowingSubscriber.class);
          }
        });
    injector.getInstance(Key.get(GuavaUtils.ServiceManagerIface.class, AppStartup.class))
        .startAsync().awaitHealthy();
    assertEquals(0L, statsProvider.getLongValue(PubsubEventModule.EXCEPTIONS_STAT));
    injector.getInstance(EventBus.class).post("hello");
    assertEquals(1L, statsProvider.getLongValue(PubsubEventModule.EXCEPTIONS_STAT));
    assertEquals(0L, statsProvider.getLongValue(PubsubEventModule.EVENT_BUS_DEAD_EVENTS));
  }

  static class ThrowingSubscriber implements PubsubEvent.EventSubscriber {
    @Subscribe
    public void receiveString(String value) {
      throw new UnsupportedOperationException();
    }
  }

  public Injector getInjector(Module... additionalModules) {
    return Guice.createInjector(
        new LifecycleModule(),
        new PubsubEventModule(logger),
        new SchedulerServicesModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(Executor.class).annotatedWith(AsyncExecutor.class)
                .toInstance(MoreExecutors.directExecutor());

            bind(UncaughtExceptionHandler.class).toInstance(exceptionHandler);

            bind(StatsProvider.class).toInstance(statsProvider);
            PubsubEventModule.bindSchedulingFilterDelegate(binder()).toInstance(schedulingFilter);
            for (Module module : additionalModules) {
              install(module);
            }
          }
        });
  }
}

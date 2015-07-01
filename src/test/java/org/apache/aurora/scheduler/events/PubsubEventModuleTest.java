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

import java.util.logging.Logger;

import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.EventBus;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.twitter.common.application.StartupStage;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.base.ExceptionalCommand;
import com.twitter.common.stats.StatsProvider;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PubsubEventModuleTest extends EasyMockTest {

  private FakeStatsProvider statsProvider;
  private Logger logger;

  @Before
  public void setUp() {
    statsProvider = new FakeStatsProvider();
    logger = createMock(Logger.class);
  }

  @Test
  public void testHandlesDeadEvent() {
    logger.warning(String.format(PubsubEventModule.DEAD_EVENT_MESSAGE, "hello"));
    Injector injector = getInjector(false);

    control.replay();

    injector.getInstance(EventBus.class).post("hello");
  }

  @Test
  public void testPubsubQueueGauge() throws Exception {
    Injector injector = getInjector(true);

    control.replay();

    injector.getInstance(Key.get(ExceptionalCommand.class, StartupStage.class)).execute();
    assertEquals(
        ImmutableMap.of(PubsubEventModule.PUBSUB_EXECUTOR_QUEUE_GAUGE, 0),
        statsProvider.getAllValues()
    );
  }

  public Injector getInjector(boolean isAsync) {
    return Guice.createInjector(
        new LifecycleModule(),
        new PubsubEventModule(isAsync, logger),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(Thread.UncaughtExceptionHandler.class)
                .toInstance(createMock(Thread.UncaughtExceptionHandler.class));

            bind(StatsProvider.class).toInstance(statsProvider);
            PubsubEventModule.bindSchedulingFilterDelegate(binder())
                .toInstance(createMock(SchedulingFilter.class));
          }
        });
  }
}

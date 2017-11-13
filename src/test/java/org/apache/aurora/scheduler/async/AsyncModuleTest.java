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

import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;

import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.scheduler.AppStartup;
import org.apache.aurora.scheduler.app.LifecycleModule;
import org.apache.aurora.scheduler.async.AsyncModule.RegisterGauges;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * TODO(wfarner): Make this test more useful by validating the bindings set up by the module.
 * Multibindings makes this tricky since it uses an internal binding annotation which makes a direct
 * check on injector.getBindings() impossible.
 */
public class AsyncModuleTest extends EasyMockTest {

  private FakeStatsProvider statsProvider;

  @Before
  public void setUp() {
    statsProvider = new FakeStatsProvider();
    new StorageTestUtil(this).expectOperations();
  }

  private Injector createInjector(Module module) {
    return Guice.createInjector(
        module,
        new LifecycleModule(),
        new AbstractModule() {
          private <T> void bindMock(Class<T> clazz) {
            bind(clazz).toInstance(createMock(clazz));
          }

          @Override
          protected void configure() {
            bind(StatsProvider.class).toInstance(statsProvider);
            bindMock(Clock.class);
            bindMock(Thread.UncaughtExceptionHandler.class);
          }
        });
  }

  @Test
  public void testBindings() throws Exception {
    Injector injector = createInjector(new AsyncModule(new AsyncModule.Options()));

    control.replay();

    Set<Service> services = injector.getInstance(
        Key.get(new TypeLiteral<Set<Service>>() { }, AppStartup.class));
    for (Service service : services) {
      service.startAsync().awaitRunning();
    }

    injector.getBindings();

    assertEquals(
        ImmutableMap.of(
            RegisterGauges.TIMEOUT_QUEUE_GAUGE, 0,
            RegisterGauges.ASYNC_TASKS_GAUGE, 0L),
        statsProvider.getAllValues()
    );
  }
}

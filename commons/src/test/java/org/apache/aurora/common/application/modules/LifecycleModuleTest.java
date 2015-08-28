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
package org.apache.aurora.common.application.modules;

import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

import org.apache.aurora.common.application.ShutdownRegistry;
import org.apache.aurora.common.application.modules.LifecycleModule.LaunchException;
import org.apache.aurora.common.application.modules.LifecycleModule.ServiceRunner;
import org.apache.aurora.common.application.modules.LocalServiceRegistry.LocalService;
import org.apache.aurora.common.base.Command;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.junit.Test;

import static org.apache.aurora.common.net.InetSocketAddressHelper.getLocalAddress;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author William Farner
 */
public class LifecycleModuleTest extends EasyMockTest {

  private static class SystemModule extends AbstractModule {
    @Override protected void configure() {
      install(new LifecycleModule());
      bind(UncaughtExceptionHandler.class).toInstance(new UncaughtExceptionHandler() {
        @Override public void uncaughtException(Thread thread, Throwable throwable) {
          fail("Uncaught exception.");
        }
      });
    }
  }

  @Test
  public void testNoServices() {
    control.replay();

    Injector injector = Guice.createInjector(new SystemModule());

    LocalServiceRegistry registry = injector.getInstance(LocalServiceRegistry.class);
    assertEquals(Optional.<InetSocketAddress>absent(), registry.getPrimarySocket());
    assertEquals(ImmutableMap.<String, InetSocketAddress>of(), registry.getAuxiliarySockets());
  }

  @Test
  public void testOrdering() throws Exception {
    final ServiceRunner runner = createMock(ServiceRunner.class);
    Command shutdown = createMock(Command.class);

    expect(runner.launch()).andReturn(LocalService.primaryService(100, shutdown));
    shutdown.execute();

    Module testModule = new AbstractModule() {
      @Override protected void configure() {
        LifecycleModule.runnerBinder(binder()).addBinding().toInstance(runner);
      }
    };

    Injector injector = Guice.createInjector(new SystemModule(), testModule);
    LocalServiceRegistry registry = injector.getInstance(LocalServiceRegistry.class);

    control.replay();

    assertEquals(Optional.of(getLocalAddress(100)), registry.getPrimarySocket());
    injector.getInstance(ShutdownRegistry.ShutdownRegistryImpl.class).execute();
  }

  @Test(expected = IllegalStateException.class)
  public void testFailedLauncher() throws Exception {
    final ServiceRunner runner = createMock(ServiceRunner.class);

    expect(runner.launch()).andThrow(new LaunchException("Injected failure."));

    Module testModule = new AbstractModule() {
      @Override protected void configure() {
        LifecycleModule.runnerBinder(binder()).addBinding().toInstance(runner);
      }
    };

    Injector injector = Guice.createInjector(new SystemModule(), testModule);
    LocalServiceRegistry registry = injector.getInstance(LocalServiceRegistry.class);

    control.replay();

    assertEquals(Optional.of(getLocalAddress(100)), registry.getPrimarySocket());
  }
}

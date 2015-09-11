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
package org.apache.aurora.scheduler.app;

import javax.inject.Inject;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Singleton;

import org.apache.aurora.common.application.Lifecycle;
import org.apache.aurora.common.application.ShutdownRegistry;
import org.apache.aurora.common.application.ShutdownRegistry.ShutdownRegistryImpl;
import org.apache.aurora.common.application.ShutdownStage;
import org.apache.aurora.common.base.Command;
import org.apache.aurora.scheduler.SchedulerServicesModule;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Binding module for a shutdown registry.
 */
public class LifecycleModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(Lifecycle.class).in(Singleton.class);

    bind(ShutdownRegistry.class).to(ShutdownRegistryImpl.class);
    bind(Key.get(Command.class, ShutdownStage.class)).to(ShutdownRegistryImpl.class);
    bind(ShutdownRegistryImpl.class).in(Singleton.class);
    SchedulerServicesModule.addAppStartupServiceBinding(binder())
        .to(TearDownShutdownRegistry.class);
  }

  /**
   * Startup command to register the shutdown registry as a process shutdown hook.
   * TODO(wfarner): Replace the shutdown registry with services and remove this behavior.
   */
  private static class TearDownShutdownRegistry extends AbstractIdleService {
    private final Command shutdownCommand;

    @Inject
    TearDownShutdownRegistry(@ShutdownStage Command shutdownCommand) {
      this.shutdownCommand = checkNotNull(shutdownCommand);
    }

    @Override
    protected void startUp() {
      // no-op
    }

    @Override
    protected void shutDown() {
      shutdownCommand.execute();
    }
  }
}

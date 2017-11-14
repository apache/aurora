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
package org.apache.aurora.scheduler.state;

import java.util.List;
import javax.inject.Singleton;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Module;

import org.apache.aurora.scheduler.app.MoreModules;
import org.apache.aurora.scheduler.config.CliOptions;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory.MesosTaskFactoryImpl;
import org.apache.aurora.scheduler.state.MaintenanceController.MaintenanceControllerImpl;
import org.apache.aurora.scheduler.state.UUIDGenerator.UUIDGeneratorImpl;

/**
 * Binding module for scheduling logic and higher-level state management.
 */
public class StateModule extends AbstractModule {

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-task_assigner_modules",
        description = "Guice modules for customizing task assignment.")
    @SuppressWarnings("rawtypes")
    public List<Class> taskAssignerModules = ImmutableList.of(FirstFitTaskAssignerModule.class);
  }

  private final CliOptions options;

  public StateModule(CliOptions options) {
    this.options = options;
  }

  @Override
  protected void configure() {
    for (Module module : MoreModules.instantiateAll(options.state.taskAssignerModules, options)) {
      install(module);
    }
    bind(MesosTaskFactory.class).to(MesosTaskFactoryImpl.class);

    bind(StateManager.class).to(StateManagerImpl.class);
    bind(StateManagerImpl.class).in(Singleton.class);

    bind(UUIDGenerator.class).to(UUIDGeneratorImpl.class);
    bind(UUIDGeneratorImpl.class).in(Singleton.class);

    bindMaintenanceController(binder());
  }

  @VisibleForTesting
  static void bindMaintenanceController(Binder binder) {
    binder.bind(MaintenanceController.class).to(MaintenanceControllerImpl.class);
    binder.bind(MaintenanceControllerImpl.class).in(Singleton.class);
    PubsubEventModule.bindSubscriber(binder, MaintenanceControllerImpl.class);
  }
}

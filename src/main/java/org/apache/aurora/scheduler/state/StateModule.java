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

import java.util.Set;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Module;

import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.scheduler.app.MoreModules;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory.MesosTaskFactoryImpl;
import org.apache.aurora.scheduler.state.MaintenanceController.MaintenanceControllerImpl;
import org.apache.aurora.scheduler.state.UUIDGenerator.UUIDGeneratorImpl;

import static java.util.Objects.requireNonNull;

/**
 * Binding module for scheduling logic and higher-level state management.
 */
public class StateModule extends AbstractModule {

  @CmdLine(name = "task_assigner_modules",
      help = "Guice modules for customizing task assignment.")
  private static final Arg<Set<Module>> TASK_ASSIGNER_MODULES = Arg.create(
      ImmutableSet.of(MoreModules.lazilyInstantiated(FirstFitTaskAssignerModule.class)));

  private final Set<Module> assignerModules;

  public StateModule() {
    this(TASK_ASSIGNER_MODULES.get());
  }

  private StateModule(Set<Module> assignerModules) {
    this.assignerModules = requireNonNull(assignerModules);
  }

  @Override
  protected void configure() {
    for (Module module: assignerModules) {
      install(module);
    }
    bind(MesosTaskFactory.class).to(MesosTaskFactoryImpl.class);

    bind(StateManager.class).to(StateManagerImpl.class);
    bind(StateManagerImpl.class).in(Singleton.class);

    bind(UUIDGenerator.class).to(UUIDGeneratorImpl.class);
    bind(UUIDGeneratorImpl.class).in(Singleton.class);
    bind(LockManager.class).to(LockManagerImpl.class);
    bind(LockManagerImpl.class).in(Singleton.class);

    bindMaintenanceController(binder());
  }

  @VisibleForTesting
  static void bindMaintenanceController(Binder binder) {
    binder.bind(MaintenanceController.class).to(MaintenanceControllerImpl.class);
    binder.bind(MaintenanceControllerImpl.class).in(Singleton.class);
    PubsubEventModule.bindSubscriber(binder, MaintenanceControllerImpl.class);
  }
}

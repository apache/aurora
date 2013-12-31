/**
 * Copyright 2013 Apache Software Foundation
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
package org.apache.aurora.scheduler.state;

import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;

import org.apache.aurora.scheduler.MesosTaskFactory;
import org.apache.aurora.scheduler.MesosTaskFactory.MesosTaskFactoryImpl;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.apache.aurora.scheduler.state.MaintenanceController.MaintenanceControllerImpl;
import org.apache.aurora.scheduler.state.TaskAssigner.TaskAssignerImpl;
import org.apache.aurora.scheduler.state.UUIDGenerator.UUIDGeneratorImpl;

/**
 * Binding module for scheduling logic and higher-level state management.
 */
public class StateModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(TaskAssigner.class).to(TaskAssignerImpl.class);
    bind(TaskAssignerImpl.class).in(Singleton.class);
    bind(MesosTaskFactory.class).to(MesosTaskFactoryImpl.class);

    bind(SchedulerCore.class).to(SchedulerCoreImpl.class).in(Singleton.class);

    bind(StateManager.class).to(StateManagerImpl.class);
    bind(StateManagerImpl.class).in(Singleton.class);

    bind(UUIDGenerator.class).to(UUIDGeneratorImpl.class);
    bind(UUIDGeneratorImpl.class).in(Singleton.class);
    bind(LockManager.class).to(LockManagerImpl.class);
    bind(LockManagerImpl.class).in(Singleton.class);

    bindCronJobManager(binder());
    bind(ImmediateJobManager.class).in(Singleton.class);

    bindMaintenanceController(binder());
  }

  @VisibleForTesting
  static void bindCronJobManager(Binder binder) {
    binder.bind(CronJobManager.class).in(Singleton.class);
    PubsubEventModule.bindSubscriber(binder, CronJobManager.class);
  }

  @VisibleForTesting
  static void bindMaintenanceController(Binder binder) {
    binder.bind(MaintenanceController.class).to(MaintenanceControllerImpl.class);
    binder.bind(MaintenanceControllerImpl.class).in(Singleton.class);
    PubsubEventModule.bindSubscriber(binder, MaintenanceControllerImpl.class);
  }
}

package com.twitter.aurora.scheduler.state;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

import com.twitter.aurora.scheduler.MesosTaskFactory;
import com.twitter.aurora.scheduler.MesosTaskFactory.MesosTaskFactoryImpl;
import com.twitter.aurora.scheduler.events.PubsubEventModule;
import com.twitter.aurora.scheduler.state.MaintenanceController.MaintenanceControllerImpl;
import com.twitter.aurora.scheduler.state.TaskAssigner.TaskAssignerImpl;

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

    bind(CronJobManager.class).in(Singleton.class);
    // TODO(William Farner): Add a test that fails if CronJobManager is not wired for events.
    PubsubEventModule.bindSubscriber(binder(), CronJobManager.class);
    bind(ImmediateJobManager.class).in(Singleton.class);

    bind(MaintenanceController.class).to(MaintenanceControllerImpl.class);
    bind(MaintenanceControllerImpl.class).in(Singleton.class);
  }
}

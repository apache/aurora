package com.twitter.aurora.scheduler.state;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
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

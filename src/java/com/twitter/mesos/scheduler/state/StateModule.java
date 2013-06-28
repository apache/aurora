package com.twitter.mesos.scheduler.state;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

import com.twitter.mesos.scheduler.MesosTaskFactory;
import com.twitter.mesos.scheduler.MesosTaskFactory.MesosTaskFactoryImpl;
import com.twitter.mesos.scheduler.events.TaskEventModule;
import com.twitter.mesos.scheduler.state.CronJobManager.CronScheduler;
import com.twitter.mesos.scheduler.state.CronJobManager.CronScheduler.Cron4jScheduler;
import com.twitter.mesos.scheduler.state.MaintenanceController.MaintenanceControllerImpl;
import com.twitter.mesos.scheduler.state.TaskAssigner.TaskAssignerImpl;

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

    bind(CronScheduler.class).to(Cron4jScheduler.class);

    bind(StateManager.class).to(StateManagerImpl.class);
    bind(StateManagerImpl.class).in(Singleton.class);

    bind(CronJobManager.class).in(Singleton.class);
    // TODO(William Farner): Add a test that fails if CronJobManager is not wired for events.
    TaskEventModule.bindSubscriber(binder(), CronJobManager.class);
    bind(ImmediateJobManager.class).in(Singleton.class);

    bind(MaintenanceController.class).to(MaintenanceControllerImpl.class);
    bind(MaintenanceControllerImpl.class).in(Singleton.class);
  }
}

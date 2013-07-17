package com.twitter.aurora.scheduler.local;

import java.util.Collection;

import javax.annotation.Nullable;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Provider;

import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.Filters;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Request;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import com.twitter.aurora.scheduler.DriverFactory;
import com.twitter.common.application.Lifecycle;

/**
 * A factory for fake scheduler driver instances.
 */
class FakeDriverFactory implements DriverFactory {

  private final Provider<Scheduler> scheduler;
  private final Lifecycle lifecycle;

  @Inject
  FakeDriverFactory(Provider<Scheduler> scheduler, Lifecycle lifecycle) {
    this.scheduler = Preconditions.checkNotNull(scheduler);
    this.lifecycle = Preconditions.checkNotNull(lifecycle);
  }

  @Override
  public SchedulerDriver apply(@Nullable final String frameworkId) {
    return new FakeSchedulerDriver() {
      @Override public Status run() {
        scheduler.get().registered(
            this,
            FrameworkID.newBuilder().setValue(
                Optional.fromNullable(frameworkId).or("new-framework-id")).build(),
            MasterInfo.newBuilder().setId("master-id").setIp(100).setPort(200).build());
        lifecycle.awaitShutdown();
        return null;
      }
    };
  }

  static class FakeSchedulerDriver implements SchedulerDriver {
    @Override public Status start() {
      return null;
    }

    @Override public Status stop(boolean b) {
      return null;
    }

    @Override public Status stop() {
      return null;
    }

    @Override public Status abort() {
      return null;
    }

    @Override public Status join() {
      return run();
    }

    @Override public Status run() {
      return null;
    }

    @Override public Status requestResources(Collection<Request> requests) {
      return null;
    }

    @Override public Status launchTasks(OfferID offerID, Collection<TaskInfo> taskInfos,
        Filters filters) {
      return null;
    }

    @Override public Status launchTasks(OfferID offerID, Collection<TaskInfo> taskInfos) {
      return null;
    }

    @Override public Status killTask(TaskID taskID) {
      return null;
    }

    @Override public Status declineOffer(OfferID offerID, Filters filters) {
      return null;
    }

    @Override public Status declineOffer(OfferID offerID) {
      return null;
    }

    @Override public Status reviveOffers() {
      return null;
    }

    @Override public Status sendFrameworkMessage(ExecutorID executorID, SlaveID slaveID,
        byte[] bytes) {
      return null;
    }
  }
}

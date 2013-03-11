package com.twitter.mesos.scheduler.testing;

import java.util.Collection;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.Filters;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Request;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.SchedulerDriver;

import com.twitter.common.application.Lifecycle;
import com.twitter.mesos.scheduler.DriverFactory;

/**
 * A factory for fake scheduler driver instances.
 */
class FakeDriverFactory implements DriverFactory {

  private final Lifecycle lifecycle;

  @Inject
  FakeDriverFactory(Lifecycle lifecycle) {
    this.lifecycle = Preconditions.checkNotNull(lifecycle);
  }

  @Override
  public SchedulerDriver apply(@Nullable String frameworkId) {
    return new FakeSchedulerDriver() {
      @Override public Status run() {
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

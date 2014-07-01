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
package org.apache.aurora.scheduler.local;

import java.util.Collection;
import java.util.Objects;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;

import com.google.common.base.Optional;
import com.twitter.common.application.Lifecycle;

import org.apache.aurora.scheduler.DriverFactory;
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
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

/**
 * A factory for fake scheduler driver instances.
 */
@SuppressWarnings("deprecation")
class FakeDriverFactory implements DriverFactory {

  private final Provider<Scheduler> scheduler;
  private final Lifecycle lifecycle;

  @Inject
  FakeDriverFactory(Provider<Scheduler> scheduler, Lifecycle lifecycle) {
    this.scheduler = Objects.requireNonNull(scheduler);
    this.lifecycle = Objects.requireNonNull(lifecycle);
  }

  @Override
  public SchedulerDriver apply(@Nullable final String frameworkId) {
    return new FakeSchedulerDriver() {
      @Override
      public Status start() {
        scheduler.get().registered(
            this,
            FrameworkID.newBuilder().setValue(
                Optional.fromNullable(frameworkId).or("new-framework-id")).build(),
            MasterInfo.newBuilder().setId("master-id").setIp(100).setPort(200).build());
        return null;
      }

      @Override
      public Status join() {
        lifecycle.awaitShutdown();
        return null;
      }
    };
  }

  static class FakeSchedulerDriver implements SchedulerDriver {
    @Override
    public Status start() {
      return null;
    }

    @Override
    public Status stop(boolean b) {
      return null;
    }

    @Override
    public Status stop() {
      return null;
    }

    @Override
    public Status abort() {
      return null;
    }

    @Override
    public Status join() {
      return run();
    }

    @Override
    public Status run() {
      return null;
    }

    @Override
    public Status requestResources(Collection<Request> requests) {
      return null;
    }

    @Override
    public Status launchTasks(
        Collection<OfferID> offerIds,
        Collection<TaskInfo> tasks,
        Filters filters) {

      return null;
    }

    @Override
    public Status launchTasks(Collection<OfferID> offerIds, Collection<TaskInfo> tasks) {
      return null;
    }

    @Override
    public Status launchTasks(OfferID offerID, Collection<TaskInfo> taskInfos, Filters filters) {
      return null;
    }

    @Override
    public Status launchTasks(OfferID offerID, Collection<TaskInfo> taskInfos) {
      return null;
    }

    @Override
    public Status killTask(TaskID taskID) {
      return null;
    }

    @Override
    public Status declineOffer(OfferID offerID, Filters filters) {
      return null;
    }

    @Override
    public Status declineOffer(OfferID offerID) {
      return null;
    }

    @Override
    public Status reviveOffers() {
      return null;
    }

    @Override
    public Status sendFrameworkMessage(ExecutorID executorID, SlaveID slaveID, byte[] bytes) {
      return null;
    }

    @Override
    public Status reconcileTasks(Collection<TaskStatus> statuses) {
      return null;
    }
  }
}

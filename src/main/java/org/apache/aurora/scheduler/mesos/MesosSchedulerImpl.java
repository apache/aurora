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
package org.apache.aurora.scheduler.mesos;

import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import org.apache.aurora.GuiceUtils.AllowUnchecked;
import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkState;

import static org.apache.aurora.scheduler.mesos.ProtosConversion.convert;

/**
 * Implementation of Scheduler callback interfaces for Mesos SchedulerDriver.
 */
@VisibleForTesting
public class MesosSchedulerImpl implements Scheduler {
  private final MesosCallbackHandler handler;
  private volatile boolean isRegistered = false;

  @Inject
  MesosSchedulerImpl(MesosCallbackHandler handler) {
    this.handler = requireNonNull(handler);
  }

  @Override
  public void slaveLost(SchedulerDriver schedulerDriver, SlaveID slaveId) {
    handler.handleLostAgent(convert(slaveId));
  }

  @Override
  public void registered(
      SchedulerDriver driver,
      final FrameworkID frameworkId,
      MasterInfo masterInfo) {
    handler.handleRegistration(convert(frameworkId), convert(masterInfo));
    isRegistered = true;
  }

  @Override
  public void disconnected(SchedulerDriver schedulerDriver) {
    handler.handleDisconnection();
  }

  @Override
  public void reregistered(SchedulerDriver schedulerDriver, MasterInfo masterInfo) {
    handler.handleReregistration(convert(masterInfo));
  }

  @Timed("scheduler_resource_offers")
  @Override
  public void resourceOffers(SchedulerDriver driver, final List<Offer> offers) {
    checkState(isRegistered, "Must be registered before receiving offers.");
    handler.handleOffers(Lists.transform(offers, ProtosConversion::convert));
  }

  @Override
  public void offerRescinded(SchedulerDriver schedulerDriver, OfferID offerId) {
    handler.handleRescind(convert(offerId));
  }

  @AllowUnchecked
  @Timed("scheduler_status_update")
  @Override
  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    handler.handleUpdate(convert(status));
  }

  @Override
  public void error(SchedulerDriver driver, String message) {
    handler.handleError(message);
  }

  @Override
  public void executorLost(SchedulerDriver schedulerDriver, ExecutorID executorID, SlaveID slaveID,
      int status) {
    handler.handleLostExecutor(convert(executorID), convert(slaveID), status);
  }

  @Timed("scheduler_framework_message")
  @Override
  public void frameworkMessage(
      SchedulerDriver driver,
      ExecutorID executorID,
      SlaveID slave,
      byte[] data) {
    handler.handleMessage(convert(executorID), convert(slave));
  }
}

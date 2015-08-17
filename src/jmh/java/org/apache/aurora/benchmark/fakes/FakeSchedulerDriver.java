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
package org.apache.aurora.benchmark.fakes;

import java.util.Collection;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

public class FakeSchedulerDriver implements SchedulerDriver {
  @Override
  public Protos.Status start() {
    return null;
  }

  @Override
  public Protos.Status stop(boolean failover) {
    return null;
  }

  @Override
  public Protos.Status stop() {
    return null;
  }

  @Override
  public Protos.Status abort() {
    return null;
  }

  @Override
  public Protos.Status join() {
    return null;
  }

  @Override
  public Protos.Status run() {
    return null;
  }

  @Override
  public Protos.Status requestResources(
      Collection<Protos.Request> requests) {
    return null;
  }

  @Override
  public Protos.Status launchTasks(
      Collection<Protos.OfferID> offerIds,
      Collection<Protos.TaskInfo> tasks,
      Protos.Filters filters) {
    return null;
  }

  @Override
  public Protos.Status launchTasks(
      Collection<Protos.OfferID> offerIds,
      Collection<Protos.TaskInfo> tasks) {
    return null;
  }

  @Override
  public Protos.Status launchTasks(
      Protos.OfferID offerId,
      Collection<Protos.TaskInfo> tasks,
      Protos.Filters filters) {
    return null;
  }

  @Override
  public Protos.Status launchTasks(
      Protos.OfferID offerId,
      Collection<Protos.TaskInfo> tasks) {
    return null;
  }

  @Override
  public Protos.Status killTask(
      Protos.TaskID taskId) {
    return null;
  }

  @Override
  public Protos.Status acceptOffers(
      Collection<Protos.OfferID> offerIds,
      Collection<Protos.Offer.Operation> operations,
      Protos.Filters filters) {
    return null;
  }

  @Override
  public Protos.Status declineOffer(
      Protos.OfferID offerId,
      Protos.Filters filters) {
    return null;
  }

  @Override
  public Protos.Status declineOffer(
      Protos.OfferID offerId) {
    return null;
  }

  @Override
  public Protos.Status reviveOffers() {
    return null;
  }

  @Override
  public Protos.Status acknowledgeStatusUpdate(
      Protos.TaskStatus status) {
    return null;
  }

  @Override
  public Protos.Status sendFrameworkMessage(
      Protos.ExecutorID executorId,
      Protos.SlaveID slaveId, byte[] data) {
    return null;
  }

  @Override
  public Protos.Status reconcileTasks(
      Collection<Protos.TaskStatus> statuses) {
    return null;
  }
}

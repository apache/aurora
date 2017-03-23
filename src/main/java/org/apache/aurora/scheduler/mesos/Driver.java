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

import java.util.Collection;

import com.google.common.util.concurrent.Service;

import org.apache.mesos.v1.Protos.Filters;
import org.apache.mesos.v1.Protos.Offer.Operation;
import org.apache.mesos.v1.Protos.OfferID;
import org.apache.mesos.v1.Protos.TaskStatus;

/**
 * Wraps the mesos Scheduler driver to ensure its used in a valid lifecycle; namely:
 * <pre>
 *   (run -> kill*)? -> stop*
 * </pre>
 *
 * Also ensures the driver is only asked for when needed.
 */
public interface Driver extends Service {

  /**
   * Performs operations eg launching a task or reserving an offer.
   *
   * @param offerId ID of the resource offer to accept with the task.
   * @param operations Operations to perform on the offer eg reserve offer and launch a task.
   * @param filter offer filter applied to unused resources in this offer.
   */
  void acceptOffers(OfferID offerId, Collection<Operation> operations, Filters filter);

  /**
   * Accepts an inverse offer.
   *
   * @param offerID ID of the inverse offer.
   * @param filter offer filter to apply.
   */
  void acceptInverseOffer(OfferID offerID, Filters filter);

  /**
   * Declines a resource offer.
   *
   * @param offerId ID of the offer to decline.
   * @param filter offer filter applied to this declined offer.
   */
  void declineOffer(OfferID offerId, Filters filter);

  /**
   * Sends a kill task request for the given {@code taskId} to the mesos master.
   *
   * @param taskId The id of the task to kill.
   */
  void killTask(String taskId);

  /**
   * Acknowledges the given {@code status} update.
   *
   * @param status The status to acknowledge.
   */
  void acknowledgeStatusUpdate(TaskStatus status);

  /**
   * Blocks until the driver is no longer active.
   */
  void blockUntilStopped();

  /**
   * Aborts the driver.
   */
  void abort();

  /**
   * Requests task reconciliation.
   *
   * @param statuses Task statuses to reconcile.
   */
  void reconcileTasks(Collection<TaskStatus> statuses);
}

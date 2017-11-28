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
package org.apache.aurora.scheduler.offers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.Protos.AgentID;
import org.apache.mesos.v1.Protos.OfferID;

import static org.apache.aurora.scheduler.events.PubsubEvent.HostAttributesChanged;

/**
 * Tracks the Offers currently known by the scheduler.
 */
public interface OfferManager extends EventSubscriber {

  /**
   * Notifies the scheduler of a new resource offer.
   *
   * @param offer Newly-available resource offer.
   */
  void add(HostOffer offer);

  /**
   * Invalidates an offer.  This indicates that the scheduler should not attempt to match any
   * tasks against the offer.
   *
   * @param offerId Cancelled offer.
   * @return A boolean on whether or not the offer was successfully cancelled.
   */
  boolean cancel(OfferID offerId);

  /**
   * Exclude an offer from being matched against all tasks.
   *
   * @param offerId Offer ID to ban.
   */
  void ban(OfferID offerId);

  /**
   * Notifies the offer queue that a host's attributes have changed.
   *
   * @param change State change notification.
   */
  void hostAttributesChanged(HostAttributesChanged change);

  /**
   * Gets the offer for the given slave ID.
   *
   * @param slaveId Slave ID to get the offer for.
   * @return The offer for the slave ID.
   */
  Optional<HostOffer> get(AgentID slaveId);

  /**
   * Gets all offers that the scheduler is holding, excluding banned offers.
   *
   * @return A snapshot of the offers that the scheduler is currently holding.
   */
  Iterable<HostOffer> getAll();

  /**
   * Gets the offer for the given slave ID if satisfies the supplied {@link ResourceRequest}.
   *
   * @param slaveId Slave ID to get the offer for.
   * @param resourceRequest The request that the offer should satisfy.
   * @param revocable Whether or not the request can use revocable resources.
   * @return An option containing the offer for the slave ID if it fits.
   */
  Optional<HostOffer> getMatching(AgentID slaveId,
                                  ResourceRequest resourceRequest,
                                  boolean revocable);

  /**
   * Gets all offers that the scheduler is holding that satisfy the supplied
   * {@link ResourceRequest}.
   *
   * @param groupKey The {@link TaskGroupKey} of the task in the {@link ResourceRequest}.
   * @param resourceRequest The request that the offer should satisfy.
   * @param revocable Whether or not the request can use revocable resources.
   * @return An option containing the offer for the slave ID if it fits.
   */
  Iterable<HostOffer> getAllMatching(TaskGroupKey groupKey,
                                     ResourceRequest resourceRequest,
                                     boolean revocable);

  /**
   * Launches the task matched against the offer.
   *
   * @param offerId Matched offer ID.
   * @param task Matched task info.
   * @throws LaunchException If there was an error launching the task.
   */
  void launchTask(OfferID offerId, Protos.TaskInfo task) throws LaunchException;

  /**
   * Thrown when there was an unexpected failure trying to launch a task.
   */
  class LaunchException extends Exception {
    @VisibleForTesting
    public LaunchException(String msg) {
      super(msg);
    }

    LaunchException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }
}

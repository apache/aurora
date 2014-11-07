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

import com.google.common.util.concurrent.Service;

import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.TaskInfo;

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
   * Launches a task.
   *
   * @param offerId ID of the resource offer to accept with the task.
   * @param task Task to launch.
   */
  void launchTask(OfferID offerId, TaskInfo task);

  /**
   * Declines a resource offer.
   *
   * @param offerId ID of the offer to decline.
   */
  void declineOffer(OfferID offerId);

  /**
   * Sends a kill task request for the given {@code taskId} to the mesos master.
   *
   * @param taskId The id of the task to kill.
   */
  void killTask(String taskId);

  /**
   * Blocks until the driver is no longer active.
   */
  void blockUntilStopped();
}

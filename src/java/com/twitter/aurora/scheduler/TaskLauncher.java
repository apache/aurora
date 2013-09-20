/*
 * Copyright 2013 Twitter, Inc.
 *
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
package com.twitter.aurora.scheduler;

import com.google.common.base.Optional;

import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskStatus;

/**
 * A receiver of resource offers and task status updates.
 */
public interface TaskLauncher {

  /**
   * Grants a resource offer to the task launcher, which will be passed to any subsequent task
   * launchers if this one does not accept.
   * <p>
   * A task launcher may choose to retain an offer for later use.  Any retained offers must be
   * cleaned up with {@link #cancelOffer(OfferID)}.
   *
   * @param offer The resource offer.
   * @return A task, absent if the launcher chooses not to accept the offer.
   */
  Optional<TaskInfo> createTask(Offer offer);

  /**
   * Informs the launcher that a status update has been received for a task.  If the task is not
   * associated with the launcher, it should return {@code false} so that another launcher may
   * receive it.
   *
   * @param status The status update.
   * @return {@code true} if the status is relevant to the launcher and should not be delivered to
   * other launchers, {@code false} otherwise.
   */
  boolean statusUpdate(TaskStatus status);

  /**
   * Informs the launcher that a previously-advertised offer is canceled and may not be used.
   *
   * @param offer The canceled offer.
   */
  void cancelOffer(OfferID offer);
}

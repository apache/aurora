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
package org.apache.aurora.scheduler;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.async.OfferQueue;
import org.apache.aurora.scheduler.base.Conversions;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.SchedulerException;
import org.apache.aurora.scheduler.state.StateManager;

import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskStatus;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A task launcher that matches resource offers against user tasks.
 */
class UserTaskLauncher implements TaskLauncher {

  private static final Logger LOG = Logger.getLogger(UserTaskLauncher.class.getName());

  @VisibleForTesting
  static final String MEMORY_LIMIT_EXCEEDED = "MEMORY STATISTICS";

  @VisibleForTesting
  static final String MEMORY_LIMIT_DISPLAY = "Task used more memory than requested.";

  private final OfferQueue offerQueue;
  private final StateManager stateManager;

  @Inject
  UserTaskLauncher(OfferQueue offerQueue, StateManager stateManager) {
    this.offerQueue = checkNotNull(offerQueue);
    this.stateManager = checkNotNull(stateManager);
  }

  @Override
  public Optional<TaskInfo> createTask(Offer offer) {
    checkNotNull(offer);

    offerQueue.addOffer(offer);
    return Optional.absent();
  }

  @Override
  public synchronized boolean statusUpdate(TaskStatus status) {
    @Nullable String message = null;
    if (status.hasMessage()) {
      message = status.getMessage();
    }

    try {
      ScheduleStatus translatedState = Conversions.convertProtoState(status.getState());
      // TODO(William Farner): Remove this hack once Mesos API change is done.
      //                       Tracked by: https://issues.apache.org/jira/browse/MESOS-343
      if ((translatedState == ScheduleStatus.FAILED)
          && (message != null)
          && (message.contains(MEMORY_LIMIT_EXCEEDED))) {
        message = MEMORY_LIMIT_DISPLAY;
      }

      stateManager.changeState(
          Query.taskScoped(status.getTaskId().getValue()),
          translatedState,
          Optional.fromNullable(message));
    } catch (SchedulerException e) {
      LOG.log(Level.WARNING, "Failed to update status for: " + status, e);
      throw e;
    }
    return true;
  }

  @Override
  public void cancelOffer(OfferID offer) {
    offerQueue.cancelOffer(offer);
  }
}
